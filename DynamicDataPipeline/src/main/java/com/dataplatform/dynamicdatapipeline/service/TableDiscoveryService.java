package com.dataplatform.dynamicdatapipeline.service;

import com.dataplatform.dynamicdatapipeline.config.BigQueryProperties;
import com.dataplatform.dynamicdatapipeline.config.DynamicCosmosContainerManager;
import com.dataplatform.dynamicdatapipeline.dto.DiscoveryResult;
import com.dataplatform.dynamicdatapipeline.model.TableConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Auto-discovery engine for BigQuery tables.
 *
 * Flow per scan:
 *   1. For each configured scan-dataset in application.yml:
 *      a. Query INFORMATION_SCHEMA.TABLES for all BASE TABLE entries
 *      b. Compare against TableRegistry
 *      c. Skip tables in excluded-tables list
 *      d. For each NEW table found:
 *         i.  Fetch schema from INFORMATION_SCHEMA.COLUMNS
 *         ii. Detect idField from schema
 *         iii.Register in TableRegistry (memory + Cosmos)
 *         iv. Create Cosmos container via DynamicCosmosContainerManager
 *         v.  Trigger full sync in background (CompletableFuture)
 *
 * Called by:
 *   - TableDiscoveryScheduler on startup + every 12 hours
 *   - TableAdminController POST /api/v1/admin/tables/discover (manual trigger)
 *
 * Sync failures for one table do NOT stop discovery of other tables.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TableDiscoveryService {

    private final GenericBigQueryService bigQueryService;
    private final BigQueryProperties bigQueryProperties;
    private final TableRegistry tableRegistry;
    private final DynamicCosmosContainerManager containerManager;
    private final GenericDataSyncService syncService;

    /**
     * Scan all configured datasets for new tables.
     * @return DiscoveryResult summary of what was found and registered
     */
    public DiscoveryResult discoverNewTables() {
        log.info("=== Starting table discovery scan (datasets: {}) ===",
                bigQueryProperties.getScanDatasets());

        long start = System.currentTimeMillis();
        List<String> newlyDiscovered  = new ArrayList<>();
        List<String> alreadyRegistered = new ArrayList<>();
        List<String> excluded          = new ArrayList<>();
        List<String> errors            = new ArrayList<>();

        for (String dataset : bigQueryProperties.getScanDatasets()) {
            try {
                scanDataset(dataset, newlyDiscovered, alreadyRegistered, excluded, errors);
            } catch (Exception e) {
                log.error("Failed to scan dataset '{}': {}", dataset, e.getMessage(), e);
                errors.add("dataset:" + dataset + " → " + e.getMessage());
            }
        }

        long durationMs = System.currentTimeMillis() - start;
        log.info("=== Discovery complete in {}ms: new={} existing={} excluded={} errors={} ===",
                durationMs, newlyDiscovered.size(), alreadyRegistered.size(),
                excluded.size(), errors.size());

        return DiscoveryResult.builder()
                .newlyDiscovered(newlyDiscovered)
                .alreadyRegistered(alreadyRegistered)
                .excluded(excluded)
                .errors(errors)
                .scannedAt(Instant.now())
                .durationMs(durationMs)
                .build();
    }

    // ── Private helpers ─────────────────────────────────────────────────────

    private void scanDataset(String dataset,
                             List<String> newlyDiscovered,
                             List<String> alreadyRegistered,
                             List<String> excluded,
                             List<String> errors) {

        List<String> tables = bigQueryService.listTables(dataset);
        log.info("Dataset '{}': found {} tables", dataset, tables.size());

        for (String tableName : tables) {

            // Skip tables in the excluded list
            if (isExcluded(tableName)) {
                log.debug("Skipping excluded table: {}", tableName);
                excluded.add(tableName);
                continue;
            }

            // Skip tables already registered
            if (tableRegistry.exists(tableName)) {
                log.debug("Table already registered: {}", tableName);
                alreadyRegistered.add(tableName);
                continue;
            }

            // New table found — auto-register it
            log.info("New table discovered: {}.{}", dataset, tableName);
            try {
                autoRegister(dataset, tableName);
                newlyDiscovered.add(tableName);
            } catch (Exception e) {
                log.error("Failed to auto-register table '{}': {}",
                        tableName, e.getMessage(), e);
                errors.add(tableName + " → " + e.getMessage());
            }
        }
    }

    private void autoRegister(String dataset, String tableName) {
        // 1. Fetch schema from BigQuery INFORMATION_SCHEMA
        Map<String, String> schema = bigQueryService.fetchSchema(dataset, tableName);

        // 2. Detect the idField from schema
        String idField = detectIdField(tableName, schema);
        log.info("Auto-detected idField for '{}': '{}'", tableName, idField);

        // 3. Detect partition key — prefer a high-cardinality field over the id field.
        //    Priority order:
        //      1. "vendor"   — 30 distinct values, ideal distribution
        //      2. "region"   — 12 distinct values, good distribution
        //      3. "category" — 18 distinct values, good distribution
        //      4. idField    — fallback (sequential ids cause hotspots)
        String partitionKeyField = detectPartitionKeyField(schema, idField);
        log.info("Auto-detected partitionKeyField for '{}': '{}'", tableName, partitionKeyField);

        // 4. Build TableConfig
        TableConfig config = TableConfig.builder()
                .name(tableName)
                .dataset(dataset)
                .bqTable(tableName)
                .cosmosContainer(tableName)          // container name = table name
                .partitionKey("/" + partitionKeyField)
                .idField(idField)
                .schema(schema)
                .cacheTtlMinutes(10)
                .autoDiscovered(true)
                .registeredAt(Instant.now())
                .lastSyncTime(null)                  // null = will trigger FULL LOAD
                .lastSyncStatus("PENDING")
                .build();

        // 5. Register in memory + Cosmos DB
        tableRegistry.register(config);

        // 6. Create Cosmos DB container for this table
        containerManager.createContainerIfNotExists(config);

        // 7. Trigger full sync in background — don't block the discovery scan
        CompletableFuture.runAsync(() -> {
            try {
                log.info("Starting background full sync for newly discovered table: {}", tableName);
                syncService.syncTable(config);
            } catch (Exception e) {
                log.error("Background sync failed for table '{}': {}",
                        tableName, e.getMessage(), e);
                tableRegistry.updateSyncStatus(tableName, "FAILED", null);
            }
        });

        log.info("Auto-registered table: '{}' (idField='{}', schema={} columns)",
                tableName, idField, schema.size());
    }

    /**
     * Detect the idField from BigQuery schema.
     *
     * Priority order:
     *   1. Column named exactly "id"
     *   2. Column named "<tableName>_id"
     *   3. Any STRING column ending with "id" (case-insensitive)
     *   4. Fallback: first column in schema
     */
    private String detectIdField(String tableName, Map<String, String> schema) {
        if (schema.isEmpty()) return "id";

        // Priority 1: exact "id"
        if (schema.containsKey("id")) return "id";

        // Priority 2: "<tableName>_id"
        String tableIdField = tableName + "_id";
        if (schema.containsKey(tableIdField)) return tableIdField;

        // Priority 3: any STRING column ending in "id"
        for (Map.Entry<String, String> entry : schema.entrySet()) {
            String colName = entry.getKey();
            String colType = entry.getValue();
            if (colName.toLowerCase().endsWith("id")
                    && "STRING".equalsIgnoreCase(colType)) {
                return colName;
            }
        }

        // Fallback: first column
        String firstCol = schema.keySet().iterator().next();
        log.warn("Could not detect idField for table '{}', using first column: '{}'",
                tableName, firstCol);
        return firstCol;
    }

    /**
     * Detect the best field to use as the Cosmos DB partition key.
     *
     * A good partition key has HIGH CARDINALITY — many distinct values that
     * distribute writes evenly across physical partition ranges.
     *
     * Priority:
     *   1. "vendor"   — 30 distinct values (software dataset)
     *   2. "region"   — 12 distinct values
     *   3. "category" — 18 distinct values
     *   4. idField    — fallback (sequential IDs cause write hotspots)
     *
     * This list can be extended with domain-specific high-cardinality fields.
     * The key criterion: field must have 10+ distinct values AND not change
     * over the lifetime of a document (immutable after creation).
     */
    private String detectPartitionKeyField(Map<String, String> schema, String idField) {
        // Preferred high-cardinality fields in priority order
        List<String> preferred = List.of("vendor", "region", "category",
                "tenant_id", "org_id", "country",
                "department", "team");

        for (String field : preferred) {
            if (schema.containsKey(field)) {
                return field;
            }
        }

        // Fallback — use the id field (works, but may cause hotspots with sequential IDs)
        log.warn("No preferred partition key field found in schema {}. " +
                "Falling back to idField='{}'. " +
                "Consider adding a high-cardinality field (vendor, region, category) " +
                "to avoid write hotspots.", schema.keySet(), idField);
        return idField;
    }

    private boolean isExcluded(String tableName) {
        List<String> excluded = bigQueryProperties.getExcludedTables();
        return excluded != null && excluded.contains(tableName);
    }
}