package com.dataplatform.dynamicdatapipeline.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * In-memory representation of a registered table configuration.
 * Lives in TableRegistry's ConcurrentHashMap.
 * Persisted to Cosmos DB via TableConfigDocument.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableConfig {

    /** Logical name used as the registry key and cache prefix. e.g. "orders" */
    private String name;

    /** BigQuery dataset name. e.g. "cache_dataset" */
    private String dataset;

    /** Actual BigQuery table name. e.g. "orders_table" */
    private String bqTable;

    /** Cosmos DB container name for this table's data. e.g. "orders" */
    private String cosmosContainer;

    /** Cosmos DB partition key path. e.g. "/id" */
    private String partitionKey;

    /**
     * Column name in BigQuery that maps to the Cosmos document "id" field.
     * Auto-detected during discovery:
     *   1. Column named exactly "id"
     *   2. Column named "<tableName>_id"
     *   3. Any STRING column ending in "id"
     *   4. Fallback: first column
     */
    private String idField;

    /**
     * Schema detected from BigQuery INFORMATION_SCHEMA.COLUMNS.
     * Map of column name → BigQuery data type (e.g. "STRING", "INT64", "TIMESTAMP")
     * Used by SchemaValidationService to validate records before writing.
     */
    private Map<String, String> schema;

    /** EhCache TTL in minutes for this table's records. Default: 10 */
    @Builder.Default
    private int cacheTtlMinutes = 10;

    /** true if auto-discovered from BigQuery, false if manually registered via admin API */
    private boolean autoDiscovered;

    /** When this table was registered */
    private Instant registeredAt;

    /**
     * Timestamp of the last successful sync.
     * null = no sync has completed → triggers FULL LOAD.
     * non-null → triggers INCREMENTAL (WHERE updated_at > lastSyncTime - 25h).
     * Stored in Cosmos DB so it survives application restarts.
     */
    private Instant lastSyncTime;

    /** Last sync status: PENDING / IN_PROGRESS / SUCCESS / FAILED */
    private String lastSyncStatus;
}