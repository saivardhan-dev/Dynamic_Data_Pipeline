package com.dataplatform.dynamicdatapipeline.config;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.spring.data.cosmos.config.AbstractCosmosConfiguration;
import com.azure.spring.data.cosmos.config.CosmosConfig;
import com.azure.spring.data.cosmos.repository.config.EnableCosmosRepositories;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Azure Cosmos DB configuration.
 *
 * Two clients:
 *  1. Spring Data Cosmos (AbstractCosmosConfiguration) — for TableConfigRepository
 *  2. Raw CosmosClient — for GenericCosmosService + DynamicCosmosContainerManager
 *
 * IMPORTANT: contentResponseOnWriteEnabled must be set in application.yml under
 *   spring.cloud.azure.cosmos — NOT here. Spring Cloud Azure overrides this value.
 */
@Slf4j
@Configuration
@EnableCosmosRepositories(
        basePackages = "com.dataplatform.dynamicdatapipeline.repository"
)
public class CosmosDbConfig extends AbstractCosmosConfiguration {

    @Value("${azure.cosmos.endpoint}")
    private String endpoint;

    @Value("${azure.cosmos.key}")
    private String key;

    @Value("${azure.cosmos.database}")
    private String database;

    @Bean
    public CosmosClientBuilder cosmosClientBuilder() {
        log.info("Configuring Cosmos DB client: endpoint={}, database={}, mode=DIRECT",
                endpoint, database);
        return new CosmosClientBuilder()
                .endpoint(endpoint)
                .key(key)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .directMode(DirectConnectionConfig.getDefaultConfig());
    }

    @Bean
    public CosmosClient cosmosClient() {
        return new CosmosClientBuilder()
                .endpoint(endpoint)
                .key(key)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .directMode(DirectConnectionConfig.getDefaultConfig())
                .buildClient();
    }

    /**
     * Async client — required for executeBulkOperations() and fetchAllForCache()
     * in GenericCosmosService.
     *
     * Why a separate async client:
     *   CosmosClient (sync) does not expose executeBulkOperations() or
     *   CosmosPagedFlux (continuation-token pagination).
     *   Both clients share the same underlying RNTBD connection pool —
     *   no double connection overhead.
     *
     * ThrottlingRetryOptions:
     *   The SDK default is 9 retries with short waits. Under heavy bulk write
     *   load this is insufficient — operations exhaust retries and get re-queued,
     *   burning RUs on failed attempts. Increasing to 20 retries with a 60s
     *   ceiling lets the SDK ride out 429 bursts gracefully instead of failing.
     *
     * contentResponseOnWrite=false:
     *   Bulk operations return a CosmosBulkOperationResponse per item.
     *   We only inspect statusCode and requestCharge — not the response body.
     *   Disabling saves deserialisation overhead on every bulk response item.
     */
    @Bean
    public CosmosAsyncClient cosmosAsyncClient() {
        log.info("Configuring Cosmos DB async client for bulk writes: endpoint={}", endpoint);

        ThrottlingRetryOptions retryOptions = new ThrottlingRetryOptions();
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(20); // default: 9
        retryOptions.setMaxRetryWaitTime(Duration.ofSeconds(60)); // default: 30s

        return new CosmosClientBuilder()
                .endpoint(endpoint)
                .key(key)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .directMode(DirectConnectionConfig.getDefaultConfig())
                .throttlingRetryOptions(retryOptions)
                .contentResponseOnWriteEnabled(false)
                .buildAsyncClient();
    }

    @Override
    public CosmosConfig cosmosConfig() {
        return CosmosConfig.builder()
                .enableQueryMetrics(false)
                .build();
    }

    @Override
    protected String getDatabaseName() {
        return database;
    }
}