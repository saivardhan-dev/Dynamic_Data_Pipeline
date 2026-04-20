package com.dataplatform.dynamicdatapipeline.scheduler;

import com.dataplatform.dynamicdatapipeline.dto.SyncResult;
import com.dataplatform.dynamicdatapipeline.service.GenericDataSyncService;
import com.dataplatform.dynamicdatapipeline.service.TableRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Schedules data sync for all registered tables.
 *
 * Triggers:
 *   1. On startup — full load for tables where lastSyncTime = null
 *      (first run or after admin reset). Tables with existing lastSyncTime
 *      will do incremental sync.
 *
 *   2. Daily at 2:00 AM — incremental sync for all registered tables
 *      (WHERE updated_at > NOW() - INTERVAL 25 HOUR)
 *      Fires 2 hours after the expected midnight batch load in BigQuery.
 *
 * IMPORTANT: TableDiscoveryScheduler runs BEFORE this scheduler on startup
 * because ApplicationReadyEvent listeners are called in registration order.
 * Discovery must complete first so all tables are registered before sync starts.
 *
 * AtomicBoolean guard prevents overlap if sync takes longer than 24 hours
 * (unlikely but handled safely).
 *
 * Disable via: scheduler.sync.enabled=false
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduler.sync.enabled",
        havingValue = "true", matchIfMissing = true)
public class GenericSyncScheduler {

    private final GenericDataSyncService syncService;
    private final TableRegistry tableRegistry;
    private final AtomicBoolean syncInProgress = new AtomicBoolean(false);

    @Value("${scheduler.sync.run-on-startup:true}")
    private boolean runOnStartup;

    /**
     * Startup sync — fires after TableDiscoveryScheduler has registered all tables.
     * Uses a small delay to allow discovery to complete first.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() throws InterruptedException {
        if (!runOnStartup) {
            log.info("Startup sync disabled (scheduler.sync.run-on-startup=false)");
            return;
        }

        // Brief delay to let TableDiscoveryScheduler complete its initial scan
        // Discovery triggers sync per-table via CompletableFuture for new tables,
        // so here we only sync tables that were already registered (loaded from Cosmos DB)
        Thread.sleep(3000);

        int registeredCount = tableRegistry.size();
        if (registeredCount == 0) {
            log.info("Startup sync: no tables registered yet — discovery will trigger syncs");
            return;
        }

        log.info("=== Startup sync triggered for {} pre-registered tables ===",
                registeredCount);
        runSync("startup");
    }

    /**
     * Daily incremental sync at 2:00 AM.
     * All tables with lastSyncTime != null will do incremental sync.
     * Tables with lastSyncTime = null will do full load.
     */
    @Scheduled(cron = "${scheduler.sync.cron:0 0 2 * * *}")
    public void scheduledSync() {
        log.info("=== Scheduled daily sync triggered (2:00 AM cron) ===");
        runSync("scheduled");
    }

    private void runSync(String trigger) {
        if (!syncInProgress.compareAndSet(false, true)) {
            log.warn("Sync already in progress — skipping trigger='{}'", trigger);
            return;
        }

        try {
            Map<String, SyncResult> results = syncService.syncAllTables();

            long success = results.values().stream()
                    .filter(r -> r.getStatus() == SyncResult.SyncStatus.SUCCESS
                            || r.getStatus() == SyncResult.SyncStatus.PARTIAL_SUCCESS)
                    .count();
            long failed = results.values().stream()
                    .filter(r -> r.getStatus() == SyncResult.SyncStatus.FAILED)
                    .count();

            log.info("Sync [{}] complete: {}/{} succeeded, {} failed",
                    trigger, success, results.size(), failed);

            // Log per-table summary
            results.forEach((table, result) ->
                    log.info("  [{}] table='{}' mode={} fetched={} written={} durationMs={}",
                            result.getStatus(), table, result.getSyncMode(),
                            result.getFetchedCount(), result.getSavedCount(),
                            result.getDurationMs()));

        } catch (Exception e) {
            log.error("Sync [{}] failed unexpectedly: {}", trigger, e.getMessage(), e);
        } finally {
            syncInProgress.set(false);
        }
    }
}