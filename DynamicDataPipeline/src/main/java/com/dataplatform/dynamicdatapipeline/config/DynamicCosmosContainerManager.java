package com.dataplatform.dynamicdatapipeline.config;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.models.*;
import com.dataplatform.dynamicdatapipeline.model.TableConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Creates Cosmos DB containers dynamically for newly discovered or registered tables.
 *
 * Called by:
 *   - CosmosContainerInitializer (startup) → creates table-configs + sync-errors containers
 *   - TableDiscoveryService (auto-discovery) → creates container for each new table
 *   - TableAdminController (manual register) → creates container on POST /admin/tables/register
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DynamicCosmosContainerManager {

    private final CosmosClient cosmosClient;

    @Value("${azure.cosmos.database}")
    private String databaseName;

    @Value("${cosmos.default-throughput:400}")
    private int defaultThroughput;

    /**
     * Creates a Cosmos container for the given TableConfig if it doesn't already exist.
     * Uses the table's configured partitionKey and applies a standard indexing policy.
     */
    public void createContainerIfNotExists(TableConfig config) {
        createContainerIfNotExists(
                config.getCosmosContainer(),
                config.getPartitionKey(),
                defaultThroughput
        );
    }

    /**
     * Creates a container with explicit name, partition key, and throughput.
     * Used for system containers (table-configs, sync-errors).
     */
    public void createContainerIfNotExists(String containerName,
                                           String partitionKeyPath,
                                           int throughput) {
        log.info("Ensuring Cosmos container exists: {} (partitionKey={}, RU/s={})",
                containerName, partitionKeyPath, throughput);

        CosmosContainerProperties containerProps =
                new CosmosContainerProperties(containerName, partitionKeyPath);

        // Standard indexing policy — index everything, exclude metadata fields
        // IMPORTANT: Must include "/*" in includedPaths.
        // Cosmos DB rejects policies without the root wildcard.
        IndexingPolicy policy = new IndexingPolicy();
        policy.setIndexingMode(IndexingMode.CONSISTENT);
        policy.setIncludedPaths(List.of(new IncludedPath("/*")));
        policy.setExcludedPaths(List.of(
                new ExcludedPath("/_etag/?"),
                new ExcludedPath("/syncedAt/?")
        ));
        containerProps.setIndexingPolicy(policy);

        cosmosClient
                .getDatabase(databaseName)
                .createContainerIfNotExists(
                        containerProps,
                        ThroughputProperties.createManualThroughput(throughput)
                );

        // If the container already existed at a lower throughput (e.g. created
        // at 400 RU/s before this config change), update it to the current value.
        // replaceThroughput is idempotent — safe to call on every startup.
        try {
            cosmosClient
                    .getDatabase(databaseName)
                    .getContainer(containerName)
                    .replaceThroughput(ThroughputProperties.createManualThroughput(throughput));
            log.info("Container throughput set: {} → {} RU/s", containerName, throughput);
        } catch (Exception e) {
            // Shared-throughput databases don't support container-level RU/s —
            // this is non-fatal, the container still works at the database level.
            log.warn("Could not set throughput on container '{}': {} (non-fatal — " +
                    "may be a shared-throughput database)", containerName, e.getMessage());
        }

        log.info("Container ready: {}", containerName);
    }
}