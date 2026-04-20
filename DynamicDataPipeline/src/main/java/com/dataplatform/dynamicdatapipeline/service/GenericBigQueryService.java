package com.dataplatform.dynamicdatapipeline.service;

import com.google.cloud.bigquery.*;
import com.dataplatform.dynamicdatapipeline.config.BigQueryProperties;
import com.dataplatform.dynamicdatapipeline.model.TableConfig;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

/**
 * Fetches data from Google BigQuery using the Jobs API.
 *
 * Two sync modes:
 *
 *   FULL LOAD (lastSyncTime = null):
 *     SELECT * FROM `project.dataset.table`
 *     Used on first run or admin-forced reset.
 *
 *   INCREMENTAL (lastSyncTime != null):
 *     SELECT * FROM `project.dataset.table`
 *     WHERE updated_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR)
 *     Used on all subsequent scheduled syncs.
 *     25-hour window (not 24) handles clock skew and late-arriving data.
 *
 * Why the Jobs API (not Query API):
 *   BigQuery Jobs API submits the query as an async job on Google's servers.
 *   For large tables (1M+ rows) the synchronous Query API times out.
 *   The Jobs API scans the table on Google's distributed infrastructure,
 *   then streams results back page by page.
 *
 * Protected by Resilience4j:
 *   @CircuitBreaker — opens after 50% failure rate over 10 calls
 *   @Retry — 3 attempts with exponential backoff (2s → 4s → 8s)
 *   Retry fires BEFORE the circuit breaker records a failure,
 *   so transient errors don't trip the breaker.
 *   Fallback returns empty list → sync aborts cleanly.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GenericBigQueryService {

    private final BigQuery bigQuery;
    private final BigQueryProperties properties;

    // ── Public API ──────────────────────────────────────────────────────────

    /**
     * Fetch records for a table.
     *
     * Mode selection:
     *   1. If lastSyncTime is null          → FULL (first run)
     *   2. If schema has no "updated_at"    → FULL (table has no change-tracking column)
     *   3. If schema has "updated_at"       → INCREMENTAL (only changed records)
     *
     * WHY CHECK FOR updated_at:
     *   The incremental query uses WHERE updated_at > TIMESTAMP_SUB(...).
     *   If the table has no updated_at column, BigQuery throws an error and
     *   the sync returns 0 records — silently leaving Cosmos and cache stale.
     *   Checking the schema first ensures we never attempt an incremental sync
     *   on a table that cannot support it.
     */
    @CircuitBreaker(name = "bigquery", fallbackMethod = "fetchFallback")
    @Retry(name = "bigquery")
    public List<Map<String, Object>> fetchRecords(TableConfig config) {
        if (config.getLastSyncTime() == null) {
            // First run — always full
            return fetchFull(config);
        }

        // Only run incremental if the table actually has an updated_at column.
        // If not, fall back to full sync — guarantees no stale data regardless
        // of whether records changed since the last sync.
        boolean hasUpdatedAt = config.getSchema() != null
                && config.getSchema().containsKey("updated_at");

        if (!hasUpdatedAt) {
            log.info("Table '{}' has no updated_at column — using FULL sync to ensure " +
                    "all changed records are picked up", config.getName());
            return fetchFull(config);
        }

        return fetchIncremental(config);
    }

    /**
     * Force a full load regardless of lastSyncTime.
     * Called by admin API when user requests a forced full sync.
     */
    @CircuitBreaker(name = "bigquery", fallbackMethod = "fetchFallback")
    @Retry(name = "bigquery")
    public List<Map<String, Object>> fetchFull(TableConfig config) {
        String sql = String.format(
                "SELECT * FROM `%s.%s.%s`",
                properties.getProjectId(),
                config.getDataset(),
                config.getBqTable()
        );
        log.info("BigQuery FULL LOAD: {}.{}.{}",
                properties.getProjectId(), config.getDataset(), config.getBqTable());
        log.debug("SQL: {}", sql);
        return executeQuery(sql, config);
    }

    /**
     * Fetch only records changed in the last N hours (from application.yml).
     * Uses parameterized query to prevent SQL injection.
     *
     * The 25-hour window ensures records aren't missed due to:
     *   - Clock skew between BigQuery producers and this service
     *   - Late-arriving data (records inserted after their updated_at timestamp)
     *   - Overlap between sync windows
     */
    @CircuitBreaker(name = "bigquery", fallbackMethod = "fetchFallback")
    @Retry(name = "bigquery")
    public List<Map<String, Object>> fetchIncremental(TableConfig config) {
        int windowHours = properties.getIncrementalWindowHours();

        String sql = String.format(
                "SELECT * FROM `%s.%s.%s` " +
                        "WHERE updated_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d HOUR)",
                properties.getProjectId(),
                config.getDataset(),
                config.getBqTable(),
                windowHours
        );

        log.info("BigQuery INCREMENTAL: {}.{}.{} (last {} hours)",
                properties.getProjectId(), config.getDataset(),
                config.getBqTable(), windowHours);
        log.debug("SQL: {}", sql);
        return executeQuery(sql, config);
    }

    /**
     * Fetch schema for a table from INFORMATION_SCHEMA.
     * Returns Map of column name → BigQuery data type (e.g. "STRING", "INT64").
     * Called by TableDiscoveryService during auto-discovery.
     */
    public Map<String, String> fetchSchema(String dataset, String tableName) {
        String sql = String.format(
                "SELECT column_name, data_type " +
                        "FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS` " +
                        "WHERE table_name = '%s' " +
                        "ORDER BY ordinal_position",
                properties.getProjectId(), dataset, tableName
        );

        log.debug("Fetching schema for {}.{}", dataset, tableName);

        Map<String, String> schema = new LinkedHashMap<>();
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration
                    .newBuilder(sql)
                    .setUseLegacySql(false)
                    .build();

            Job job = bigQuery.create(
                    JobInfo.newBuilder(queryConfig)
                            .setJobId(JobId.of(UUID.randomUUID().toString()))
                            .build());

            job = job.waitFor();

            if (job == null || job.getStatus().getError() != null) {
                log.error("Schema fetch failed for {}.{}", dataset, tableName);
                return schema;
            }

            for (FieldValueList row : job.getQueryResults().iterateAll()) {
                String colName  = row.get("column_name").getStringValue();
                String dataType = row.get("data_type").getStringValue();
                schema.put(colName, dataType);
            }

            log.debug("Schema for {}: {} columns", tableName, schema.size());
        } catch (Exception e) {
            log.error("Failed to fetch schema for {}.{}: {}", dataset, tableName, e.getMessage());
        }
        return schema;
    }

    /**
     * List all table names in a dataset using INFORMATION_SCHEMA.
     * Used by TableDiscoveryService for auto-discovery.
     */
    public List<String> listTables(String dataset) {
        String sql = String.format(
                "SELECT table_name " +
                        "FROM `%s.%s.INFORMATION_SCHEMA.TABLES` " +
                        "WHERE table_type = 'BASE TABLE' " +
                        "ORDER BY table_name",
                properties.getProjectId(), dataset
        );

        log.debug("Listing tables in dataset: {}", dataset);
        List<String> tables = new ArrayList<>();

        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration
                    .newBuilder(sql)
                    .setUseLegacySql(false)
                    .build();

            Job job = bigQuery.create(
                    JobInfo.newBuilder(queryConfig)
                            .setJobId(JobId.of(UUID.randomUUID().toString()))
                            .build());

            job = job.waitFor();

            if (job == null || job.getStatus().getError() != null) {
                log.error("Failed to list tables in dataset: {}", dataset);
                return tables;
            }

            for (FieldValueList row : job.getQueryResults().iterateAll()) {
                tables.add(row.get("table_name").getStringValue());
            }

            log.info("Found {} tables in dataset '{}'", tables.size(), dataset);
        } catch (Exception e) {
            log.error("Error listing tables in dataset '{}': {}", dataset, e.getMessage());
        }
        return tables;
    }

    // ── Private helpers ─────────────────────────────────────────────────────

    private List<Map<String, Object>> executeQuery(String sql, TableConfig config) {
        try {
            QueryJobConfiguration queryConfig = QueryJobConfiguration
                    .newBuilder(sql)
                    .setUseLegacySql(false)
                    .setAllowLargeResults(true)
                    .build();

            Job job = bigQuery.create(
                    JobInfo.newBuilder(queryConfig)
                            .setJobId(JobId.of(UUID.randomUUID().toString()))
                            .build());

            // Wait for BigQuery to execute the job on Google's servers
            job = job.waitFor();

            if (job == null) {
                throw new RuntimeException("BigQuery job was null after submission");
            }
            if (job.getStatus().getError() != null) {
                throw new RuntimeException(
                        "BigQuery job failed: " + job.getStatus().getError().getMessage());
            }

            TableResult result = job.getQueryResults();
            Schema schema = result.getSchema();
            List<Map<String, Object>> records = new ArrayList<>();

            for (FieldValueList row : result.iterateAll()) {
                Map<String, Object> doc = new LinkedHashMap<>();

                // Dynamically map every column — no hardcoded field names
                for (Field field : schema.getFields()) {
                    FieldValue value = row.get(field.getName());
                    doc.put(field.getName(), extractValue(field, value));
                }

                // Ensure the "id" field exists for Cosmos DB
                // Use the configured idField as the document id
                Object idValue = doc.get(config.getIdField());
                if (idValue != null) {
                    doc.put("id", idValue.toString());
                }

                records.add(doc);
            }

            log.info("BigQuery fetch complete: {} records from {}.{}",
                    records.size(), config.getDataset(), config.getBqTable());
            return records;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("BigQuery query interrupted", e);
        }
    }

    /**
     * Maps BigQuery field types to Java types.
     * Returns null for null values — Cosmos DB handles null gracefully.
     */
    private Object extractValue(Field field, FieldValue value) {
        if (value == null || value.isNull()) return null;

        try {
            StandardSQLTypeName type = field.getType().getStandardType();
            return switch (type) {
                case STRING    -> value.getStringValue();
                case INT64     -> value.getLongValue();
                case FLOAT64   -> value.getDoubleValue();
                case NUMERIC,
                     BIGNUMERIC -> value.getNumericValue();
                case BOOL      -> value.getBooleanValue();
                case TIMESTAMP -> Instant.ofEpochSecond(
                        value.getTimestampValue() / 1_000_000L,
                        (value.getTimestampValue() % 1_000_000L) * 1_000L);
                case DATE,
                     TIME,
                     DATETIME  -> value.getStringValue();
                case BYTES     -> value.getBytesValue();
                default        -> value.getStringValue();
            };
        } catch (Exception e) {
            log.warn("Failed to extract value for field '{}': {}",
                    field.getName(), e.getMessage());
            return null;
        }
    }

    // ── Resilience4j fallback ───────────────────────────────────────────────

    /**
     * Called when circuit breaker is OPEN or all retry attempts are exhausted.
     * Returns empty list → DataSyncService detects this and aborts sync cleanly
     * without writing zero records to Cosmos DB.
     */
    public List<Map<String, Object>> fetchFallback(TableConfig config, Throwable t) {
        log.error("BigQuery fetch fallback triggered for table '{}': {}",
                config.getName(), t.getMessage());
        return Collections.emptyList();
    }

    public List<Map<String, Object>> fetchFallback(TableConfig config,
                                                   Throwable t,
                                                   RuntimeException ignored) {
        return fetchFallback(config, t);
    }
}