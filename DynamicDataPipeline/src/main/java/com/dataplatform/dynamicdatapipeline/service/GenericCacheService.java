package com.dataplatform.dynamicdatapipeline.service;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.ehcache.Cache;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Generic EhCache service using native EhCache 3 API.
 *
 * HARD CONSTRAINTS:
 *   NO @Cacheable
 *   NO @EnableCaching
 *   NO JCache (javax.cache)
 *   NO ehcache.xml
 *
 * Key format: "tableName:id"  e.g. "orders:ord-0000001"
 *
 * This composite key allows a single cache instance to serve
 * all tables without key collisions.
 *
 * Cache-aside pattern (mandatory for all reads):
 *   1. cache.get(tableName:id)
 *   2. HIT  → return immediately (< 1ms)
 *   3. MISS → query Cosmos DB → cache.put → return (~600ms)
 *
 * Metrics tracked per table:
 *   cache.hit{table=orders}   — counter
 *   cache.miss{table=orders}  — counter
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GenericCacheService {

    private final Cache<String, Map> genericRecordsCache;
    private final MeterRegistry meterRegistry;

    // ── Key helper ──────────────────────────────────────────────────────────

    private String key(String tableName, String id) {
        return tableName + ":" + id;
    }

    // ── Read ────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    public Optional<Map<String, Object>> get(String tableName, String id) {
        Map value = genericRecordsCache.get(key(tableName, id));

        if (value != null) {
            log.debug("Cache HIT  {} : {}", tableName, id);
            meterRegistry.counter("cache.hit", "table", tableName).increment();
            return Optional.of((Map<String, Object>) value);
        }

        log.debug("Cache MISS {} : {}", tableName, id);
        meterRegistry.counter("cache.miss", "table", tableName).increment();
        return Optional.empty();
    }

    // ── Write ───────────────────────────────────────────────────────────────

    public void put(String tableName, String id, Map<String, Object> document) {
        genericRecordsCache.put(key(tableName, id), document);
        log.debug("Cache PUT  {} : {}", tableName, id);
    }

    public void evict(String tableName, String id) {
        genericRecordsCache.remove(key(tableName, id));
        log.debug("Cache EVICT {} : {}", tableName, id);
    }

    /**
     * Bulk load records into cache after a full sync.
     * Replaces all existing entries for this table.
     */
    public int bulkLoad(String tableName,
                        List<Map<String, Object>> documents,
                        String idField) {
        log.info("Cache bulk load: table={} records={}", tableName, documents.size());

        Map<String, Map> batch = new HashMap<>(documents.size());
        int skipped = 0;

        for (Map<String, Object> doc : documents) {
            Object idValue = doc.get(idField);
            if (idValue == null) {
                skipped++;
                continue;
            }
            batch.put(key(tableName, idValue.toString()), doc);
        }

        genericRecordsCache.putAll(batch);

        if (skipped > 0) {
            log.warn("Cache bulk load: skipped {} records with null idField '{}'",
                    skipped, idField);
        }

        log.info("Cache bulk load complete: table={} loaded={} skipped={}",
                tableName, batch.size(), skipped);
        return batch.size();
    }

    /**
     * Load a single page of records into cache silently (no per-page log spam).
     * Used by CacheWarmUpService — streams Cosmos in pages, loading each page
     * directly into EhCache without holding all 300K records in memory at once.
     *
     * @return number of entries actually loaded (excludes records with null id)
     */
    @SuppressWarnings("unchecked")
    public int loadPage(String tableName,
                        List<Map<String, Object>> page,
                        String idField) {
        Map<String, Map> batch = new HashMap<>(page.size());
        for (Map<String, Object> doc : page) {
            Object idValue = doc.get(idField);
            if (idValue == null) continue;
            batch.put(key(tableName, idValue.toString()), doc);
        }
        if (!batch.isEmpty()) {
            genericRecordsCache.putAll(batch);
        }
        return batch.size();
    }

    /**
     * Update cache for only the changed records during incremental sync.
     * Does NOT clear existing entries — only updates/adds changed records.
     */
    public int bulkUpdate(String tableName,
                          List<Map<String, Object>> documents,
                          String idField) {
        log.info("Cache bulk update: table={} changedRecords={}", tableName, documents.size());

        int updated = 0;
        for (Map<String, Object> doc : documents) {
            Object idValue = doc.get(idField);
            if (idValue == null) continue;
            genericRecordsCache.put(key(tableName, idValue.toString()), doc);
            updated++;
        }

        log.info("Cache bulk update complete: table={} updated={}", tableName, updated);
        return updated;
    }

    /**
     * Remove all cache entries for a specific table.
     * Used before a full reload sync.
     */
    public void clearTable(String tableName) {
        String prefix = tableName + ":";

        Set<String> keysToRemove = StreamSupport
                .stream(genericRecordsCache.spliterator(), false)
                .map(Cache.Entry::getKey)
                .filter(k -> k.startsWith(prefix))
                .collect(Collectors.toSet());

        if (!keysToRemove.isEmpty()) {
            genericRecordsCache.removeAll(keysToRemove);
            log.info("Cache cleared: table={} removedEntries={}", tableName, keysToRemove.size());
        } else {
            log.debug("Cache clear: no entries found for table={}", tableName);
        }
    }

    /**
     * Count cached entries for a specific table.
     * Used for monitoring/observability.
     */
    public long countEntries(String tableName) {
        String prefix = tableName + ":";
        return StreamSupport
                .stream(genericRecordsCache.spliterator(), false)
                .map(Cache.Entry::getKey)
                .filter(k -> k.startsWith(prefix))
                .count();
    }
}