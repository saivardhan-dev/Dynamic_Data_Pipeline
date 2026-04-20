package com.dataplatform.dynamicdatapipeline.config;

import com.dataplatform.dynamicdatapipeline.model.TableConfig;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Reads BigQuery configuration from application.yml under the "bigquery" prefix.
 *
 * scan-datasets: datasets to scan for auto-discovery
 * excluded-tables: tables to never auto-sync
 * incremental-window-hours: safety buffer for clock skew (default 25)
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "bigquery")
public class BigQueryProperties {

    private String projectId;
    private String defaultDataset;

    /** Datasets scanned every 12 hours for new tables */
    private List<String> scanDatasets = new ArrayList<>();

    /**
     * Tables to never auto-sync even if found in a scan-dataset.
     * Add temp tables, staging tables, or INFORMATION_SCHEMA views here.
     */
    private List<String> excludedTables = new ArrayList<>();

    /**
     * Incremental sync fetches records WHERE updated_at > NOW() - INTERVAL N HOUR.
     * Use 25 (not 24) to handle clock skew and late-arriving data.
     */
    private int incrementalWindowHours = 25;

    /** BigQuery Jobs API timeout in seconds */
    private int queryTimeoutSeconds = 300;

    /**
     * Optional: bootstrap tables defined in application.yml.
     * These are loaded on startup and merged with Cosmos-persisted configs.
     * yml-defined tables take priority over Cosmos-persisted configs on conflict.
     */
    private List<TableConfig> tables = new ArrayList<>();
}