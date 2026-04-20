package com.dataplatform.dynamicdatapipeline.service;

import com.dataplatform.dynamicdatapipeline.dto.SyncResult;
import com.dataplatform.dynamicdatapipeline.model.TableConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

/**
 * Orchestrates the full sync pipeline for one or all tables.
 *
 * Per-table pipeline (3 steps):
 *
 *   Step 1: Fetch from BigQuery
 *     - FULL LOAD if lastSyncTime = null (first run or admin reset)
 *     - INCREMENTAL if lastSyncTime != null (WHERE updated_at > lastSyncTime - 25h)
 *
 *   Step 2: Validate + Write to Cosmos DB
 *     - SchemaValidationService filters invalid records to dead-letter container
 *     - Parallel chunk processing: 500 records/chunk × 8 threads
 *     - Failed chunks retried up to max-retries times
 *     - Chunk failures do NOT stop other chunks
 *
 *   Step 3: Update EhCache
 *     - FULL LOAD: clearTable() then bulkLoad()
 *     - INCREMENTAL: bulkUpdate() only changed records
 *
 *   After success: update lastSyncTime in TableRegistry + Cosmos DB
 *
 * Multi-table behavior (syncAllTables):
 *   - Loops all registered tables sequentially
 *   - Failure on one table does NOT stop others
 *   - Returns per-table SyncResult map
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GenericDataSyncService {

    private final TableRegistry tableRegistry;
    private final GenericBigQueryService bigQueryService;
    private final GenericCosmosService cosmosService;
    private final GenericCacheService cacheService;
    private final SchemaValidationService validationService;
    private final MeterRegistry meterRegistry;

    // ── Public API ──────────────────────────────────────────────────────────

    /**
     * Sync ALL registered tables — called by GenericSyncScheduler every 24 hours.
     * Failures on individual tables are caught and logged — other tables continue.
     */
    public Map<String, SyncResult> syncAllTables() {
        List<TableConfig> tables = tableRegistry.getAll();
        log.info("=== Syncing all {} registered tables ===", tables.size());

        Map<String, SyncResult> results = new LinkedHashMap<>();
        for (TableConfig table : tables) {
            try {
                results.put(table.getName(), syncTable(table));
            } catch (Exception e) {
                log.error("Unexpected error syncing table '{}': {}",
                        table.getName(), e.getMessage(), e);
                results.put(table.getName(),
                        SyncResult.failed(table.getName(), e.getMessage()));
            }
        }

        long successCount = results.values().stream()
                .filter(r -> r.getStatus() == SyncResult.SyncStatus.SUCCESS)
                .count();
        log.info("=== All tables sync complete: {}/{} succeeded ===",
                successCount, tables.size());

        return results;
    }

    /**
     * Sync a single table — determines FULL vs INCREMENTAL automatically.
     * Called by: syncAllTables(), TableDiscoveryService (new table), TableAdminController.
     */
    public SyncResult syncTable(TableConfig config) {
        Instant startedAt = Instant.now();

        // Determine expected sync mode for logging.
        // Note: GenericBigQueryService.fetchRecords() may override to FULL
        // if the table has no updated_at column — even when lastSyncTime is set.
        boolean hasUpdatedAt = config.getSchema() != null
                && config.getSchema().containsKey("updated_at");
        String syncMode = config.getLastSyncTime() == null ? "FULL"
                : hasUpdatedAt ? "INCREMENTAL" : "FULL (no updated_at column)";

        log.info("=== Sync START: table='{}' mode={} lastSyncTime={} ===",
                config.getName(), syncMode, config.getLastSyncTime());

        tableRegistry.updateSyncStatus(config.getName(), "IN_PROGRESS", null);

        Timer.Sample timerSample = Timer.start(meterRegistry);

        // ── Step 1: Fetch from BigQuery ─────────────────────────────────────
        List<Map<String, Object>> records;
        try {
            records = bigQueryService.fetchRecords(config);
        } catch (Exception e) {
            log.error("Step 1 FAILED — BigQuery fetch error for '{}': {}",
                    config.getName(), e.getMessage());
            tableRegistry.updateSyncStatus(config.getName(), "FAILED", null);
            return SyncResult.failed(config.getName(), "BigQuery fetch failed: " + e.getMessage());
        }

        if (records.isEmpty()) {
            // Empty list = circuit breaker OPEN or all retries exhausted
            // Do NOT write zero records to Cosmos — abort cleanly
            String msg = syncMode.equals("INCREMENTAL")
                    ? "No new/changed records in incremental window — nothing to sync"
                    : "BigQuery returned 0 records — circuit breaker may be OPEN";
            log.warn("Step 1 result: 0 records for '{}' — {}", config.getName(), msg);

            if (syncMode.equals("INCREMENTAL")) {
                // Zero incremental records is normal — update timestamp and return success
                tableRegistry.updateSyncStatus(config.getName(), "SUCCESS", Instant.now());
                return SyncResult.success(config.getName(), syncMode, 0, 0, 0,
                        System.currentTimeMillis() - startedAt.toEpochMilli());
            } else {
                tableRegistry.updateSyncStatus(config.getName(), "FAILED", null);
                return SyncResult.failed(config.getName(), msg);
            }
        }

        log.info("Step 1 complete: table='{}' fetched={} records", config.getName(), records.size());

        // ── Step 2: Schema validation + Cosmos DB write ─────────────────────
        List<Map<String, Object>> invalidRecords = new ArrayList<>();
        List<Map<String, Object>> validRecords = records;

        if (config.getSchema() != null && !config.getSchema().isEmpty()) {
            validRecords = validationService.filterValid(
                    records, config.getSchema(), config.getIdField(), invalidRecords);

            // Log invalid records (dead-letter container removed — memory-only mode)
            if (!invalidRecords.isEmpty()) {
                log.warn("Table '{}': {} records failed validation and were skipped",
                        config.getName(), invalidRecords.size());
            }
        }

        // partitionKeyField: strip leading slash from partitionKey path
        // e.g. "/vendor" → "vendor" — used as the field name on each document
        String partitionKeyField = config.getPartitionKey() != null
                ? config.getPartitionKey().replace("/", "")
                : config.getIdField();

        // ── Step 2A: Inject last_sync_time into every document ──────────────
        //
        // last_sync_time is a field added BY THIS APPLICATION — it does not
        // exist in BigQuery. It records which sync run wrote each document.
        //
        // Purpose:
        //   After upsert completes, we delete documents whose last_sync_time
        //   is older than the current run. This removes orphaned records from
        //   previous sync runs (e.g. records that existed in BQ yesterday but
        //   were replaced with entirely new IDs today).
        //
        // Why inject BEFORE upsert (not after):
        //   The timestamp must be embedded in the document at write time so
        //   Cosmos stores it as a queryable field. We cannot patch it after.
        //
        // Why this is better than delete-then-upsert:
        //   Old records remain in Cosmos and serve cache-miss fallbacks during
        //   the upsert phase. There is no empty-window where Cosmos has 0 records.
        //   Only after all new records are safely written do we remove the old ones.
        String currentSyncTime = startedAt.toString(); // ISO-8601, e.g. "2026-04-16T02:00:00Z"

        log.info("Step 2A: injecting last_sync_time='{}' into {} records",
                currentSyncTime, validRecords.size());

        for (Map<String, Object> doc : validRecords) {
            doc.put("last_sync_time", currentSyncTime);
        }

        // ── Step 2B: Bulk upsert all records with current last_sync_time ────
        log.info("Step 2B START: bulk upsert to Cosmos — container='{}' records={} mode={}",
                config.getName(), validRecords.size(), syncMode);

        int  writtenToCosmos = cosmosService.upsertBulk(
                config.getCosmosContainer(), validRecords, partitionKeyField);
        long failedRecords   = validRecords.size() - writtenToCosmos;

        log.info("Step 2B complete: table='{}' written={} failed={}",
                config.getName(), writtenToCosmos, failedRecords);

        // ── Step 2C: Delete orphaned records from previous sync runs ─────────
        //
        // Only runs on FULL sync — incremental only patches changed records,
        // old unchanged records must stay.
        //
        // Only runs if upsert completed with ZERO failures — if some records
        // failed to write, their last_sync_time was not updated. Deleting
        // by "!= currentSyncTime" would incorrectly remove those failed records.
        // Keeping old data is always safer than accidentally deleting valid data.
        if (syncMode.startsWith("FULL")) {
            if (failedRecords == 0) {
                log.info("Step 2C START: deleting documents with last_sync_time != '{}' " +
                        "from container='{}' ...", currentSyncTime, config.getCosmosContainer());

                int deleted = cosmosService.deleteByLastSyncTime(
                        config.getCosmosContainer(), partitionKeyField, currentSyncTime);

                log.info("Step 2C complete: container='{}' deleted={} orphaned documents",
                        config.getCosmosContainer(), deleted);
            } else {
                log.warn("Step 2C SKIPPED: upsert had {} failures — keeping previous sync data " +
                                "as safety net. Orphaned records will be cleaned on next successful sync.",
                        failedRecords);
            }
        }

        log.info("Step 2 complete: table='{}' written={} failed={}",
                config.getName(), writtenToCosmos, failedRecords);

        // ── Step 3: Cache warm-up (runs AFTER all Cosmos writes complete) ──────
        // Strategy: clear entire table cache, then bulk-load from the same
        // in-memory list used for Cosmos writes — zero extra I/O, just a fast
        // heap iteration. This guarantees 100% cache warmth post-sync with no
        // stale or partially-expired entries regardless of how long sync took.
        long cachedCount = 0;
        try {
            if (syncMode.startsWith("FULL")) {
                log.info("Step 3 START: clearing cache for '{}' then bulk-loading {} records ...",
                        config.getName(), validRecords.size());
                cacheService.clearTable(config.getName());
                cachedCount = cacheService.bulkLoad(
                        config.getName(), validRecords, config.getIdField());
            } else {
                // Incremental: only update the changed records in-place
                log.info("Step 3 START: incremental cache update for '{}' ({} changed records) ...",
                        config.getName(), validRecords.size());
                cachedCount = cacheService.bulkUpdate(
                        config.getName(), validRecords, config.getIdField());
            }
            log.info("Step 3 complete: table='{}' cached={} (cache is now 100% warm)",
                    config.getName(), cachedCount);
        } catch (Exception e) {
            log.error("Step 3 FAILED — cache warm-up for '{}': {} (non-fatal, reads fall back to Cosmos)",
                    config.getName(), e.getMessage());
        }

        // ── Update lastSyncTime ─────────────────────────────────────────────
        Instant syncedAt = Instant.now();
        tableRegistry.updateSyncStatus(config.getName(), "SUCCESS", syncedAt);

        // ── Metrics ─────────────────────────────────────────────────────────
        timerSample.stop(Timer.builder("sync.table.duration")
                .tag("table", config.getName())
                .tag("mode", syncMode)
                .register(meterRegistry));

        meterRegistry.counter("sync.table.records.fetched", "table", config.getName())
                .increment(records.size());
        meterRegistry.counter("sync.table.records.written", "table", config.getName())
                .increment(writtenToCosmos);

        long durationMs = syncedAt.toEpochMilli() - startedAt.toEpochMilli();
        SyncResult result = SyncResult.builder()
                .tableName(config.getName())
                .status(failedRecords == 0
                        ? SyncResult.SyncStatus.SUCCESS
                        : SyncResult.SyncStatus.PARTIAL_SUCCESS)
                .syncMode(syncMode)
                .fetchedCount(records.size())
                .savedCount(writtenToCosmos)
                .cachedCount(cachedCount)
                .failedCount(failedRecords)
                .validationFailedCount(invalidRecords.size())
                .startedAt(startedAt)
                .completedAt(syncedAt)
                .durationMs(durationMs)
                .build();

        log.info("=== Sync END: table='{}' status={} mode={} fetched={} written={} " +
                        "cached={} failed={} durationMs={} ===",
                config.getName(), result.getStatus(), syncMode,
                records.size(), writtenToCosmos,
                cachedCount, failedRecords, durationMs);

        return result;
    }


}