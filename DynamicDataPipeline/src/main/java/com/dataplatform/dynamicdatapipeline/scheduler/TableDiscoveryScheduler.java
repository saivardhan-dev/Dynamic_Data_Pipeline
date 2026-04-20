package com.dataplatform.dynamicdatapipeline.scheduler;

import com.dataplatform.dynamicdatapipeline.dto.DiscoveryResult;
import com.dataplatform.dynamicdatapipeline.service.TableDiscoveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Schedules automatic table discovery scans.
 *
 * Triggers:
 *   1. On startup (ApplicationReadyEvent) — discovers tables immediately
 *   2. Every 12 hours (midnight cron) — scans for new tables added to BigQuery
 *
 * AtomicBoolean guard prevents overlap if a scan takes longer than expected.
 *
 * Disable via: scheduler.discovery.enabled=false
 */
@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduler.discovery.enabled",
        havingValue = "true", matchIfMissing = true)
public class TableDiscoveryScheduler {

    private final TableDiscoveryService discoveryService;
    private final AtomicBoolean discoveryInProgress = new AtomicBoolean(false);

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        log.info("Application ready — running initial table discovery scan...");
        runDiscovery("startup");
    }

    @Scheduled(cron = "${scheduler.discovery.cron:0 0 0 * * *}")
    public void scheduledDiscovery() {
        log.info("Scheduled table discovery triggered (midnight cron)");
        runDiscovery("scheduled");
    }

    private void runDiscovery(String trigger) {
        if (!discoveryInProgress.compareAndSet(false, true)) {
            log.warn("Discovery already in progress — skipping trigger='{}'", trigger);
            return;
        }

        try {
            DiscoveryResult result = discoveryService.discoverNewTables();
            log.info("Discovery [{}] complete: new={} existing={} excluded={} errors={}",
                    trigger,
                    result.getNewlyDiscovered().size(),
                    result.getAlreadyRegistered().size(),
                    result.getExcluded().size(),
                    result.getErrors().size());

            if (!result.getNewlyDiscovered().isEmpty()) {
                log.info("Newly discovered tables: {}", result.getNewlyDiscovered());
            }
            if (!result.getErrors().isEmpty()) {
                log.warn("Discovery errors: {}", result.getErrors());
            }
        } catch (Exception e) {
            log.error("Discovery failed [{}]: {}", trigger, e.getMessage(), e);
        } finally {
            discoveryInProgress.set(false);
        }
    }
}