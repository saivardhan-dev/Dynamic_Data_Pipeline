package com.dataplatform.dynamicdatapipeline.service;

import com.dataplatform.dynamicdatapipeline.config.BigQueryProperties;
import com.dataplatform.dynamicdatapipeline.model.TableConfig;
import com.dataplatform.dynamicdatapipeline.exception.TableNotFoundException;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central in-memory registry for all table configurations.
 *
 * Memory-only — no Cosmos DB persistence.
 * On restart: TableDiscoveryService re-discovers all tables from BigQuery
 * and re-syncs them automatically (full sync each restart).
 *
 * Thread safety: ConcurrentHashMap is safe for concurrent reads/writes
 * from scheduler threads and HTTP request threads simultaneously.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TableRegistry {

    private final ConcurrentHashMap<String, TableConfig> registry
            = new ConcurrentHashMap<>();

    private final BigQueryProperties bigQueryProperties;

    @PostConstruct
    public void initialize() {
        log.info("Initializing TableRegistry...");

        // Load any bootstrap tables defined in application.yml
        if (bigQueryProperties.getTables() != null) {
            bigQueryProperties.getTables().forEach(config -> {
                config.setRegisteredAt(Instant.now());
                config.setAutoDiscovered(false);
                if (config.getLastSyncStatus() == null) {
                    config.setLastSyncStatus("PENDING");
                }
                registry.put(config.getName(), config);
                log.info("  Loaded from yml: table='{}'", config.getName());
            });
        }

        log.info("TableRegistry initialized with {} table(s): {}",
                registry.size(), registry.keySet());
    }

    // ── Write operations ────────────────────────────────────────────────────

    public void register(TableConfig config) {
        registry.put(config.getName(), config);
        log.info("Registered table: '{}' (autoDiscovered={})",
                config.getName(), config.isAutoDiscovered());
    }

    public void updateSyncStatus(String tableName, String status, Instant lastSyncTime) {
        TableConfig config = registry.get(tableName);
        if (config == null) {
            log.warn("updateSyncStatus called for unknown table: {}", tableName);
            return;
        }
        config.setLastSyncStatus(status);
        if (lastSyncTime != null) {
            config.setLastSyncTime(lastSyncTime);
        }
        registry.put(tableName, config);
    }

    public void deregister(String tableName) {
        registry.remove(tableName);
        log.info("Deregistered table: '{}'", tableName);
    }

    // ── Read operations ─────────────────────────────────────────────────────

    public TableConfig get(String tableName) {
        TableConfig config = registry.get(tableName);
        if (config == null) {
            throw new TableNotFoundException(tableName);
        }
        return config;
    }

    public boolean exists(String tableName) {
        return registry.containsKey(tableName);
    }

    public List<TableConfig> getAll() {
        return new ArrayList<>(registry.values());
    }

    public int size() {
        return registry.size();
    }
}