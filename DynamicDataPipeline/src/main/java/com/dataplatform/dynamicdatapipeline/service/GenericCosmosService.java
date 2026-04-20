package com.dataplatform.dynamicdatapipeline.service;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Generic Cosmos DB service for dynamic container operations.
 *
 * Uses the raw Cosmos SDK — NOT Spring Data CosmosRepository.
 *
 * Why raw SDK:
 *   Spring Data CosmosRepository requires @Container(containerName) at compile time.
 *   It cannot resolve container names dynamically at runtime. Since every BigQuery
 *   table maps to its own Cosmos container, we must use the raw SDK.
 *
 * Two clients:
 *   cosmosClient      (sync)  — all read operations (findById, findAll, findByField, count)
 *   cosmosAsyncClient (async) — bulk writes via executeBulkOperations()
 *
 * Document model: Map<String, Object>
 *   Cosmos stores documents as JSON so any Map maps directly.
 *   Schema-agnostic — handles any table without knowing column names.
 *
 * Bulk write strategy:
 *   The Cosmos Bulk Executor API pipelines thousands of upsert operations across
 *   RNTBD connections internally, amortising per-request RTT overhead across a
 *   large batch. This reduces 300K sequential upserts (~75 min at 1000 RU/s) to
 *   a pipelined bulk stream (~3-6 min at the same 1000 RU/s).
 *
 *   Key design decisions:
 *   1. Partition key = document "id" field (matches container setup)
 *   2. contentResponseOnWrite=false on async client — we only need statusCode +
 *      requestCharge per bulk response item, not the full response body
 *   3. Progress logging every 10K records — bulk ops are opaque otherwise
 *   4. Non-fatal item errors are counted and logged; do NOT abort the batch
 *   5. Flux.fromIterable() feeds ops lazily — no OOM risk for 300K records
 *
 * Fix notes:
 *   - CosmosPagedIterable requires explicit import from com.azure.cosmos.util
 *   - queryItems() type token must use LinkedHashMap.class not Map.class
 *     because Map is abstract — SDK needs a concrete instantiable class
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GenericCosmosService {

    private final CosmosClient      cosmosClient;       // sync  — reads
    private final CosmosAsyncClient cosmosAsyncClient;  // async — bulk writes

    @Value("${azure.cosmos.database}")
    private String databaseName;

    // ── Write operations ────────────────────────────────────────────────────

    /**
     * Bulk-upsert a list of documents using the Cosmos Bulk Executor API.
     *
     * Flow:
     *   1. Build a CosmosBulkUpsertItemOperation per document (partitionKey = id)
     *   2. Wrap in a Flux and pass to CosmosAsyncContainer.executeBulkOperations()
     *   3. SDK batches ops internally across RNTBD connections, honouring RU/s
     *      back-pressure via x-ms-retry-after-ms automatically
     *   4. Each CosmosBulkOperationResponse is inspected — successes counted,
     *      failures logged individually without aborting the stream
     *   5. blockLast() makes this method synchronous so GenericDataSyncService
     *      needs no Reactor knowledge
     *
     * @param containerName    Cosmos container (matches BQ table name)
     * @param documents        All records — same in-memory list from BQ fetch (zero extra I/O)
     * @param partitionKeyField Field name whose value is used as the Cosmos partition key.
     *                         Must match the container's partition key path (e.g. "vendor" for /vendor).
     *                         High-cardinality fields (30+ distinct values) prevent write hotspots.
     * @return number of successfully written documents
     */
    public int upsertBulk(String containerName,
                          List<Map<String, Object>> documents,
                          String partitionKeyField) {
        if (documents == null || documents.isEmpty()) return 0;

        CosmosAsyncContainer container = cosmosAsyncClient
                .getDatabase(databaseName)
                .getContainer(containerName);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount    = new AtomicInteger(0);
        AtomicLong    totalRU      = new AtomicLong(0);
        int           total        = documents.size();

        log.info("Bulk upsert START: container='{}' totalRecords={} partitionKeyField='{}'",
                containerName, total, partitionKeyField);
        long startMs = System.currentTimeMillis();

        // Lazy Flux of bulk upsert operations — one per document.
        // PartitionKey value is read from partitionKeyField on each document.
        // e.g. partitionKeyField="vendor" → PartitionKey("Microsoft"), PartitionKey("Adobe")...
        // This distributes writes evenly across all physical partition ranges.
        Flux<CosmosItemOperation> operations = Flux.fromIterable(documents)
                .map(doc -> {
                    String pkValue = String.valueOf(doc.get(partitionKeyField));
                    return CosmosBulkOperations.getUpsertItemOperation(
                            doc,
                            new PartitionKey(pkValue),
                            new CosmosBulkItemRequestOptions()
                    );
                });

        // CosmosBulkExecutionOptions — controls how the SDK groups and fires ops.
        //
        // maxMicroBatchSize=100:
        //   Each micro-batch sent to Cosmos contains up to 100 operations.
        //   Smaller batches reduce per-burst RU spikes that trigger 429s.
        //
        // maxMicroBatchConcurrency=1:
        //   Only 1 micro-batch fires per partition range at a time.
        //   Default is 5 — with 5 concurrent batches all hitting the same
        //   partition, a 500-op burst lands simultaneously, blowing past the
        //   RU/s budget for that partition and causing cascading 429s.
        //   Serialising to 1 keeps the request rate smooth and within budget.
        CosmosBulkExecutionOptions bulkOptions = new CosmosBulkExecutionOptions();
        bulkOptions.setMaxMicroBatchSize(100);
        bulkOptions.setMaxMicroBatchConcurrency(1);

        // executeBulkOperations() pipelines the Flux internally:
        //   - Groups ops into micro-batches per partition range
        //   - Fires requests across available RNTBD channels concurrently
        //   - Applies RU/s back-pressure via retry-after automatically
        //   - Returns one CosmosBulkOperationResponse per op
        container.executeBulkOperations(operations, bulkOptions)
                .doOnNext(response -> {
                    CosmosBulkItemResponse itemResponse = response.getResponse();

                    if (response.getException() != null) {
                        // Item-level error — log and count, do NOT abort the stream
                        Throwable ex      = response.getException();
                        Object failedDoc  = response.getOperation().getItem();
                        String failedId   = failedDoc instanceof Map
                                ? String.valueOf(((Map<?, ?>) failedDoc).get("id"))
                                : "unknown";
                        log.warn("Bulk item FAILED: container='{}' id='{}' error={}",
                                containerName, failedId, ex.getMessage());
                        failCount.incrementAndGet();

                    } else if (itemResponse != null
                            && (itemResponse.getStatusCode() == 200
                            || itemResponse.getStatusCode() == 201)) {
                        // 200 = replaced, 201 = created
                        int  current = successCount.incrementAndGet();
                        totalRU.addAndGet((long) itemResponse.getRequestCharge());

                        // Progress log every 10K records
                        if (current % 10_000 == 0) {
                            long   elapsed = System.currentTimeMillis() - startMs;
                            double rate    = current / (elapsed / 1000.0);
                            log.info("Bulk upsert progress: container='{}' written={}/{} " +
                                            "failed={} totalRU={} elapsed={}s rate={}/s",
                                    containerName, current, total,
                                    failCount.get(), totalRU.get(),
                                    elapsed / 1000, (int) rate);
                        }

                    } else {
                        // Unexpected status — treat as failure
                        int status = itemResponse != null ? itemResponse.getStatusCode() : -1;
                        log.warn("Bulk item unexpected status: container='{}' statusCode={}",
                                containerName, status);
                        failCount.incrementAndGet();
                    }
                })
                .doOnError(e -> log.error(
                        "Bulk upsert stream ERROR: container='{}' error={}",
                        containerName, e.getMessage()))
                .publishOn(Schedulers.boundedElastic())
                .blockLast();   // safe — running on boundedElastic, not a NIO thread

        long   elapsed = System.currentTimeMillis() - startMs;
        double rate    = elapsed > 0 ? successCount.get() / (elapsed / 1000.0) : 0;
        log.info("Bulk upsert COMPLETE: container='{}' written={} failed={} " +
                        "totalRU={} elapsed={}s avgRate={}/s",
                containerName, successCount.get(), failCount.get(),
                totalRU.get(), elapsed / 1000, (int) rate);

        return successCount.get();
    }

    // ── Delete operations ───────────────────────────────────────────────────

    /**
     * Delete all documents whose last_sync_time does NOT match the current sync run.
     *
     * This is the cleanup step of the last_sync_time strategy:
     *
     *   1. Every sync run injects last_sync_time = syncStartTime into each document
     *      before upserting (done in GenericDataSyncService Step 2A).
     *
     *   2. After upsert completes successfully (failedRecords == 0), this method
     *      removes all documents that still carry an OLD last_sync_time — i.e.
     *      records that existed in previous syncs but are no longer in BigQuery.
     *
     * WHY THIS IS SAFE (zero-downtime):
     *   New records are fully written to Cosmos BEFORE old ones are deleted.
     *   During the upsert phase, old records still serve cache-miss fallbacks.
     *   There is never a window where Cosmos has 0 records.
     *
     * WHY NOT DELETE ALL THEN UPSERT:
     *   Delete-then-upsert creates an empty-window where Cosmos has 0 records.
     *   Any cache miss during that window returns 404 to the client.
     *   last_sync_time approach eliminates this window entirely.
     *
     * DELETE QUERY:
     *   SELECT c.id, c.{partitionKeyField} FROM c
     *   WHERE c.last_sync_time != @currentSyncTime
     *
     *   Fetches only id + partitionKey — not full documents.
     *   Minimises RU cost of the pre-delete scan.
     *
     * @param containerName    Cosmos container to clean up
     * @param partitionKeyField field name used as the partition key (e.g. "vendor")
     * @param currentSyncTime  ISO-8601 timestamp of the current sync run
     * @return number of successfully deleted orphaned documents
     */
    @SuppressWarnings("unchecked")
    public int deleteByLastSyncTime(String containerName,
                                    String partitionKeyField,
                                    String currentSyncTime) {
        log.info("deleteByLastSyncTime START: container='{}' keeping last_sync_time='{}'",
                containerName, currentSyncTime);
        long          startMs     = System.currentTimeMillis();
        AtomicInteger deleteCount = new AtomicInteger(0);
        AtomicInteger failCount   = new AtomicInteger(0);

        CosmosAsyncContainer container = cosmosAsyncClient
                .getDatabase(databaseName)
                .getContainer(containerName);

        // Fetch only id + partitionKeyField of documents from PREVIOUS sync runs.
        // Using a parameterised query to prevent injection.
        String sql = String.format(
                "SELECT c.id, c.%s FROM c WHERE c.last_sync_time != @currentSyncTime",
                partitionKeyField);

        SqlQuerySpec querySpec = new SqlQuerySpec(sql,
                new SqlParameter("@currentSyncTime", currentSyncTime));

        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setMaxDegreeOfParallelism(-1);

        // Build delete operations lazily from the id + partitionKey scan.
        // Only documents with an old last_sync_time are included.
        Flux<CosmosItemOperation> deleteOps = container
                .queryItems(querySpec, queryOptions, LinkedHashMap.class)
                .publishOn(Schedulers.boundedElastic())
                .map(item -> {
                    String id    = String.valueOf(item.get("id"));
                    String pkVal = String.valueOf(item.get(partitionKeyField));
                    return CosmosBulkOperations.getDeleteItemOperation(
                            id,
                            new PartitionKey(pkVal)
                    );
                });

        CosmosBulkExecutionOptions bulkOptions = new CosmosBulkExecutionOptions();
        bulkOptions.setMaxMicroBatchSize(100);
        bulkOptions.setMaxMicroBatchConcurrency(1);

        container.executeBulkOperations(deleteOps, bulkOptions)
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(response -> {
                    if (response.getException() != null) {
                        log.warn("Bulk delete FAILED: container='{}' error={}",
                                containerName, response.getException().getMessage());
                        failCount.incrementAndGet();
                    } else if (response.getResponse() != null
                            && (response.getResponse().getStatusCode() == 204
                            || response.getResponse().getStatusCode() == 200)) {
                        int current = deleteCount.incrementAndGet();
                        if (current % 50_000 == 0) {
                            long elapsed = System.currentTimeMillis() - startMs;
                            log.info("deleteByLastSyncTime progress: container='{}' " +
                                            "deleted={} elapsed={}s",
                                    containerName, current, elapsed / 1000);
                        }
                    } else {
                        failCount.incrementAndGet();
                    }
                })
                .doOnError(e -> log.error(
                        "deleteByLastSyncTime ERROR: container='{}' error={}",
                        containerName, e.getMessage()))
                .blockLast();

        long elapsed = System.currentTimeMillis() - startMs;
        log.info("deleteByLastSyncTime COMPLETE: container='{}' deleted={} failed={} elapsed={}s",
                containerName, deleteCount.get(), failCount.get(), elapsed / 1000);

        return deleteCount.get();
    }

    // ── Read operations ─────────────────────────────────────────────────────

    /**
     * Fetch ALL documents from a container efficiently using the async client.
     *
     * WHY ASYNC CLIENT INSTEAD OF OFFSET/LIMIT:
     *   The previous implementation used OFFSET/LIMIT pagination on the sync client.
     *   OFFSET in Cosmos is NOT a skip — it forces a full scan of all preceding
     *   documents on every page. By page 90 (90K records), Cosmos scans 91,000
     *   documents to return 1,000. Cost grows O(n²) — 300K records takes days.
     *
     *   The async client's queryItems() uses continuation tokens internally.
     *   Each page costs exactly the same RU regardless of position — O(n) total.
     *   300K records at 1000 RU/s completes in ~5-10 minutes instead of hours.
     *
     * HOW IT WORKS:
     *   CosmosAsyncContainer.queryItems() returns a CosmosPagedFlux<T>.
     *   byPage() groups individual items into pages of up to preferredPageSize.
     *   Each page is handed to the pageConsumer callback immediately — only one
     *   page lives in memory at a time, so heap usage stays flat (no OOM risk
     *   with 300K records).
     *   blockLast() makes this method synchronous from the caller's perspective
     *   so CacheWarmUpService needs no Reactor knowledge.
     *
     * @param containerName  Cosmos container to stream
     * @param pageSize       preferred records per page (1000 is optimal)
     * @param pageConsumer   callback invoked once per page — process and discard
     */
    @SuppressWarnings("unchecked")
    public void fetchAllForCache(String containerName,
                                 int pageSize,
                                 java.util.function.Consumer<List<Map<String, Object>>> pageConsumer) {
        log.info("Cosmos fetchAllForCache START: container='{}' preferredPageSize={}",
                containerName, pageSize);
        long          startMs = System.currentTimeMillis();
        AtomicInteger total   = new AtomicInteger(0);

        CosmosAsyncContainer container = cosmosAsyncClient
                .getDatabase(databaseName)
                .getContainer(containerName);

        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        options.setMaxDegreeOfParallelism(-1);   // SDK auto-selects parallelism
        options.setMaxBufferedItemCount(pageSize);

        // SELECT * FROM c — no OFFSET, no LIMIT.
        // SDK issues continuation-token requests internally — each page costs
        // the same RU regardless of position in the result set.
        //
        // publishOn(boundedElastic): CRITICAL for API call path.
        // blockLast() is forbidden on Netty NIO threads (http-nio-8080-exec-*).
        // boundedElastic is Reactor's designated scheduler for blocking work —
        // it parks the reactive chain on a thread-pool thread where blocking
        // is safe, leaving the NIO thread free. Without this, calling this
        // method from an HTTP request thread throws IllegalStateException
        // which escapes the try/catch and surfaces as a 500 error.
        container.queryItems("SELECT * FROM c", options, LinkedHashMap.class)
                .byPage(pageSize)
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(page -> {
                    List<Map<String, Object>> batch = page.getResults()
                            .stream()
                            .map(m -> (Map<String, Object>) m)
                            .collect(Collectors.toList());

                    if (!batch.isEmpty()) {
                        pageConsumer.accept(batch);
                        int streamed = total.addAndGet(batch.size());

                        // Progress log every 50K records
                        if (streamed % 50_000 == 0) {
                            long elapsed = System.currentTimeMillis() - startMs;
                            double rate  = streamed / (elapsed / 1000.0);
                            log.info("Cosmos fetchAllForCache progress: container='{}' " +
                                            "streamed={} elapsed={}s rate={}/s",
                                    containerName, streamed, elapsed / 1000, (int) rate);
                        }
                    }
                })
                .doOnError(e -> log.error(
                        "Cosmos fetchAllForCache ERROR: container='{}' error={}",
                        containerName, e.getMessage()))
                .blockLast();  // safe — running on boundedElastic, not a NIO thread

        long   elapsed = System.currentTimeMillis() - startMs;
        double rate    = elapsed > 0 ? total.get() / (elapsed / 1000.0) : 0;
        log.info("Cosmos fetchAllForCache COMPLETE: container='{}' total={} elapsed={}s avgRate={}/s",
                containerName, total.get(), elapsed / 1000, (int) rate);
    }

    /**
     * Find a single document by id.
     *
     * WHY QUERY INSTEAD OF POINT READ:
     *   Cosmos point reads (readItem) require BOTH the document id AND the partition
     *   key value. When partition key = /vendor, we would need to know the vendor
     *   value to do a point read — but at cache-miss time we only have the id.
     *
     *   Options:
     *     A) Point read  → needs partition key value → not available → can't use
     *     B) SQL query   → SELECT * FROM c WHERE c.id = 'x' → works, costs ~3-5 RU
     *     C) Cross-partition point read → not supported by Cosmos SDK
     *
     *   We use option B. The cost is higher (3-5 RU vs 1 RU) but this path only
     *   executes on cache MISS — which only happens during the brief startup
     *   warm-up window. During normal operation the cache is 100% warm so this
     *   method is almost never called.
     *
     *   If you want 1 RU point reads on cache miss, cache the partition key value
     *   alongside the document in EhCache (e.g. store vendor in a separate small map).
     *   For now the query approach is simpler and correct.
     */
    @SuppressWarnings("unchecked")
    public Optional<Map<String, Object>> findById(String containerName, String id) {
        try {
            // Parameterised query — safe against injection since id comes from URL path
            String sql = "SELECT * FROM c WHERE c.id = @id";
            SqlQuerySpec querySpec = new SqlQuerySpec(sql,
                    new SqlParameter("@id", id));

            CosmosPagedIterable<LinkedHashMap> results = getContainer(containerName)
                    .queryItems(querySpec, new CosmosQueryRequestOptions(), LinkedHashMap.class);

            Optional<Map<String, Object>> doc = results.stream()
                    .map(m -> (Map<String, Object>) m)
                    .findFirst();

            if (doc.isPresent()) {
                log.debug("Cosmos query read: container={} id={} found=true", containerName, id);
            } else {
                log.debug("Cosmos query read: container={} id={} found=false", containerName, id);
            }

            return doc;

        } catch (CosmosException e) {
            if (e.getStatusCode() == 404) {
                return Optional.empty();
            }
            log.error("Cosmos read failed: container={} id={} status={} message={}",
                    containerName, id, e.getStatusCode(), e.getMessage());
            throw e;
        }
    }

    /**
     * Paginated query — returns one page of documents using OFFSET/LIMIT.
     */
    public List<Map<String, Object>> findAll(String containerName, int page, int size) {
        int offset = page * size;
        String sql = String.format(
                "SELECT * FROM c OFFSET %d LIMIT %d", offset, size);
        return queryDocuments(containerName, sql);
    }

    /**
     * Field-value search — returns documents where the specified field equals the value.
     */
    public List<Map<String, Object>> findByField(String containerName,
                                                 String fieldName,
                                                 String value) {
        if (!fieldName.matches("[a-zA-Z0-9_]+")) {
            throw new IllegalArgumentException(
                    "Invalid field name: '" + fieldName +
                            "'. Only alphanumeric and underscore characters are allowed.");
        }
        String sql = String.format(
                "SELECT * FROM c WHERE c.%s = '%s'",
                fieldName, value.replace("'", "\\'"));
        return queryDocuments(containerName, sql);
    }

    /**
     * Count all documents in a container.
     */
    public long count(String containerName) {
        try {
            CosmosPagedIterable<Object> results = getContainer(containerName)
                    .queryItems(
                            "SELECT VALUE COUNT(1) FROM c",
                            new CosmosQueryRequestOptions(),
                            Object.class);
            return results.stream()
                    .mapToLong(r -> ((Number) r).longValue())
                    .findFirst()
                    .orElse(0L);
        } catch (Exception e) {
            log.error("Count failed for container '{}': {}", containerName, e.getMessage());
            return 0L;
        }
    }

    // ── Private helpers ─────────────────────────────────────────────────────

    private CosmosContainer getContainer(String containerName) {
        return cosmosClient
                .getDatabase(databaseName)
                .getContainer(containerName);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> queryDocuments(String containerName, String sql) {
        log.debug("Cosmos query: container={} sql={}", containerName, sql);
        try {
            // LinkedHashMap.class (not Map.class) — Map is abstract, SDK needs concrete type
            CosmosPagedIterable<LinkedHashMap> results = getContainer(containerName)
                    .queryItems(sql, new CosmosQueryRequestOptions(), LinkedHashMap.class);

            return results.stream()
                    .map(m -> (Map<String, Object>) m)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Query failed on container '{}': {}", containerName, e.getMessage());
            return Collections.emptyList();
        }
    }
}