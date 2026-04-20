package com.dataplatform.dynamicdatapipeline.controller;

import com.dataplatform.dynamicdatapipeline.config.DynamicCosmosContainerManager;
import com.dataplatform.dynamicdatapipeline.dto.DiscoveryResult;
import com.dataplatform.dynamicdatapipeline.dto.SyncResult;
import com.dataplatform.dynamicdatapipeline.model.TableConfig;
import com.dataplatform.dynamicdatapipeline.service.GenericDataSyncService;
import com.dataplatform.dynamicdatapipeline.service.TableDiscoveryService;
import com.dataplatform.dynamicdatapipeline.service.TableRegistry;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Admin API for table registry management.
 *
 * Allows runtime registration/deregistration of tables
 * and manual control of discovery and sync — no restart needed.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/admin/tables")
@RequiredArgsConstructor
@Tag(name = "Admin", description = "Table registry management and sync control")
public class TableAdminController {

    private final TableRegistry tableRegistry;
    private final DynamicCosmosContainerManager containerManager;
    private final GenericDataSyncService syncService;
    private final TableDiscoveryService discoveryService;
    // ── GET /api/v1/admin/tables ────────────────────────────────────────────

    @GetMapping
    @Operation(summary = "List all registered tables with full config",
            description = "Returns complete TableConfig for every registered table")
    public ResponseEntity<List<TableConfig>> listTables() {
        return ResponseEntity.ok(tableRegistry.getAll());
    }

    // ── POST /api/v1/admin/tables/register ──────────────────────────────────

    @PostMapping("/register")
    @Operation(summary = "Manually register a new table",
            description = "Registers a table, creates its Cosmos container, " +
                    "and triggers a full sync in the background. No restart needed.")
    public ResponseEntity<Map<String, String>> registerTable(
            @RequestBody TableConfig config) {

        if (tableRegistry.exists(config.getName())) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error",
                            "Table '" + config.getName() + "' is already registered. " +
                                    "Use POST /{table}/sync to trigger a sync."));
        }

        // Set defaults for manually registered tables
        config.setRegisteredAt(Instant.now());
        config.setAutoDiscovered(false);
        config.setLastSyncTime(null);        // null = full load on first sync
        config.setLastSyncStatus("PENDING");

        if (config.getCacheTtlMinutes() <= 0) config.setCacheTtlMinutes(10);
        if (config.getPartitionKey() == null) {
            config.setPartitionKey("/" + config.getIdField());
        }

        // 1. Register in memory + Cosmos DB
        tableRegistry.register(config);

        // 2. Create Cosmos container
        containerManager.createContainerIfNotExists(config);

        // 3. Trigger full sync in background
        CompletableFuture.runAsync(() -> {
            try {
                syncService.syncTable(config);
            } catch (Exception e) {
                log.error("Background sync failed for manually registered table '{}': {}",
                        config.getName(), e.getMessage(), e);
                tableRegistry.updateSyncStatus(config.getName(), "FAILED", null);
            }
        });

        log.info("Manually registered table: '{}' — sync started in background",
                config.getName());

        return ResponseEntity.ok(Map.of(
                "message", "Table '" + config.getName() + "' registered successfully.",
                "status",  "Full sync started in background.",
                "table",   config.getName()
        ));
    }

    // ── DELETE /api/v1/admin/tables/{table} ─────────────────────────────────

    @DeleteMapping("/{table}")
    @Operation(summary = "Remove a table from the registry",
            description = "Removes the table from the registry and stops syncing. " +
                    "Does NOT delete the Cosmos DB container or its data.")
    public ResponseEntity<Map<String, String>> deregisterTable(
            @Parameter(description = "Table name to deregister")
            @PathVariable String table) {

        tableRegistry.get(table); // throws 404 if not registered
        tableRegistry.deregister(table);

        log.info("Deregistered table: '{}'", table);
        return ResponseEntity.ok(Map.of(
                "message", "Table '" + table + "' removed from registry.",
                "note",    "Cosmos DB container and data preserved."
        ));
    }

    // ── POST /api/v1/admin/tables/{table}/sync ──────────────────────────────

    @PostMapping("/{table}/sync")
    @Operation(summary = "Trigger sync for a specific table",
            description = "Runs incremental sync (or full if lastSyncTime is null). " +
                    "Sync runs synchronously — response returns when complete.")
    public ResponseEntity<SyncResult> syncTable(
            @Parameter(description = "Table name to sync")
            @PathVariable String table,
            @Parameter(description = "Force full reload (ignores lastSyncTime)")
            @RequestParam(defaultValue = "false") boolean fullReload) {

        TableConfig config = tableRegistry.get(table);

        if (fullReload) {
            log.info("Admin forced full reload for table: '{}'", table);
            config.setLastSyncTime(null);  // reset → triggers full load
            tableRegistry.register(config);
        }

        SyncResult result = syncService.syncTable(config);
        return ResponseEntity.ok(result);
    }

    // ── POST /api/v1/admin/tables/discover ──────────────────────────────────

    @PostMapping("/discover")
    @Operation(summary = "Manually trigger table discovery scan",
            description = "Scans all configured BigQuery datasets for new tables " +
                    "and auto-registers any that are not yet in the registry.")
    public ResponseEntity<DiscoveryResult> triggerDiscovery() {
        log.info("Admin triggered manual table discovery scan");
        DiscoveryResult result = discoveryService.discoverNewTables();
        return ResponseEntity.ok(result);
    }

}