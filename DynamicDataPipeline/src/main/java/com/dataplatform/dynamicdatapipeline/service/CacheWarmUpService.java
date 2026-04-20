package com.dataplatform.dynamicdatapipeline.service;

import com.dataplatform.dynamicdatapipeline.model.TableConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Warms up EhCache from Cosmos DB on every application startup.
 *
 * WHY THIS IS NEEDED:
 *   EhCache is heap-only (in-memory). Every JVM restart wipes it completely.
 *   On restart, Cosmos DB already has the latest synced data — this service
 *   streams it back into cache so the app is warm before serving traffic.
 *
 * PRIMARY CACHE POPULATION PATH (normal operation, no restart):
 *   The scheduler fires daily at 2 AM:
 *     Step 1: BigQuery fetch → 300K records in memory
 *     Step 2: Bulk upsert   → Cosmos DB
 *     Step 3: clearTable()  → bulkLoad() from same in-memory list ← cache warm ✅
 *   This service is NOT involved in the normal daily sync path.
 *
 * RESTART PATH (this service):
 *   App restarts → JVM wipes EhCache → ApplicationReadyEvent fires →
 *   fetchAllForCache() streams Cosmos → EhCache warm ✅
 *
 * Continuation token pagination:
 *   fetchAllForCache() uses CosmosAsyncClient which handles continuation
 *   tokens internally. Unlike OFFSET/LIMIT (O(n²) cost), each page costs
 *   the same RU regardless of position — O(n) total.
 *   300K records streams in ~5-10 minutes at 1000 RU/s.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CacheWarmUpService {

    private static final int PAGE_SIZE = 1000;

    private final TableRegistry        tableRegistry;
    private final GenericCosmosService cosmosService;
    private final GenericCacheService  cacheService;

    /**
     * Fires AFTER all beans are initialised and Tomcat is ready.
     * TableRegistry.initialize() is guaranteed complete by this point.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void warmUpOnStartup() {
        List<TableConfig> tables = tableRegistry.getAll();

        if (tables.isEmpty()) {
            log.info("Cache warm-up: no registered tables — skipping");
            return;
        }

        log.info("=== Cache warm-up START: {} table(s) to load from Cosmos ===",
                tables.size());
        long start = System.currentTimeMillis();

        for (TableConfig table : tables) {
            warmUpTable(table);
        }

        long elapsed = System.currentTimeMillis() - start;
        log.info("=== Cache warm-up COMPLETE: {} table(s) loaded in {}s ===",
                tables.size(), elapsed / 1000);
    }

    // ── Private ─────────────────────────────────────────────────────────────

    private void warmUpTable(TableConfig table) {
        String containerName = table.getCosmosContainer();
        String idField       = table.getIdField();
        long   startMs       = System.currentTimeMillis();

        log.info("Cache warm-up: table='{}' streaming from Cosmos container='{}'...",
                table.getName(), containerName);

        try {
            cacheService.clearTable(table.getName());

            final int[] total = {0};
            cosmosService.fetchAllForCache(containerName, PAGE_SIZE, page -> {
                total[0] += cacheService.loadPage(table.getName(), page, idField);
            });

            long elapsed = System.currentTimeMillis() - startMs;
            log.info("Cache warm-up complete: table='{}' loaded={} elapsed={}s",
                    table.getName(), total[0], elapsed / 1000);

        } catch (Exception e) {
            log.error("Cache warm-up FAILED for table='{}': {} " +
                            "(non-fatal — reads fall back to Cosmos until next sync)",
                    table.getName(), e.getMessage(), e);
        }
    }
}