package com.dataplatform.dynamicdatapipeline.config;

import lombok.extern.slf4j.Slf4j;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.*;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Native EhCache 3 configuration — programmatic API only.
 *
 * HARD CONSTRAINTS:
 *   NO @Cacheable / NO @EnableCaching / NO JCache / NO ehcache.xml
 *
 * Key:   String        ("tableName:id")
 * Value: LinkedHashMap (concrete + Serializable — required for off-heap)
 *
 * Off-heap serialization fix:
 *   EhCache cannot serialize the Map interface.
 *   Solution: heap-only cache (no off-heap tier).
 *   Off-heap requires a registered Serializer<LinkedHashMap> but
 *   PlainJavaSerializer<LinkedHashMap> cannot be passed due to Java
 *   generic type erasure at the withSerializer() call site.
 *   Heap-only with 200K entries handles production loads fine.
 *   Add off-heap back when migrating to a custom serializer.
 *
 * VM flag: -XX:MaxDirectMemorySize=1g still needed for Netty/Cosmos SDK.
 */
@Slf4j
@Configuration
public class EhCacheConfig {

    @Value("${ehcache.generic.heap-entries:200000}")
    private long heapEntries;

    @Bean(destroyMethod = "close")
    public CacheManager ehCacheManager() {
        log.info("Building native EhCache CacheManager: heap={} entries, TTL=NONE (scheduler-managed)",
                heapEntries);

        // No TTL — cache freshness is managed entirely by the sync scheduler.
        // After every BQ->Cosmos sync completes, GenericDataSyncService calls
        // clearTable() then bulkLoad() to atomically replace the entire cache.
        // This guarantees 100% cache warmth after each sync with no stale entries.
        //
        // Heap-only — off-heap requires a concrete Serializer<LinkedHashMap>
        // which cannot be provided via withSerializer() due to type erasure.
        CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
                .withCache("generic-records",
                        CacheConfigurationBuilder
                                .newCacheConfigurationBuilder(
                                        String.class,
                                        LinkedHashMap.class,
                                        ResourcePoolsBuilder.newResourcePoolsBuilder()
                                                .heap(heapEntries, EntryUnit.ENTRIES)
                                )
                                .withExpiry(ExpiryPolicyBuilder.noExpiration())
                                .build()
                )
                .build(true);

        log.info("Native EhCache CacheManager initialised: [generic-records] heap-only, no-expiry");
        return manager;
    }

    @Bean
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Cache<String, Map> genericRecordsCache(CacheManager ehCacheManager) {
        Cache<String, LinkedHashMap> cache = ehCacheManager
                .getCache("generic-records", String.class, LinkedHashMap.class);
        return (Cache<String, Map>) (Cache) cache;
    }
}