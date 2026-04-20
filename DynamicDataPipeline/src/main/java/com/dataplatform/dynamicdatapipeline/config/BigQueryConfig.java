package com.dataplatform.dynamicdatapipeline.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * BigQuery client configuration.
 *
 * Auth strategy:
 *   - If GOOGLE_APPLICATION_CREDENTIALS env var points to a service account JSON → use it
 *   - Otherwise → Application Default Credentials (ADC)
 *     Local dev: run "gcloud auth application-default login" once
 *     GKE: Workload Identity automatically provides credentials
 */
@Slf4j
@Configuration
public class BigQueryConfig {

    @Value("${bigquery.project-id}")
    private String projectId;

    @Value("${GOOGLE_APPLICATION_CREDENTIALS:}")
    private String credentialsPath;

    @Bean
    public BigQuery bigQuery() throws IOException {
        BigQueryOptions.Builder optionsBuilder = BigQueryOptions.newBuilder()
                .setProjectId(projectId);

        if (StringUtils.hasText(credentialsPath)) {
            log.info("BigQuery: using service account credentials from: {}", credentialsPath);
            GoogleCredentials credentials = GoogleCredentials
                    .fromStream(new FileInputStream(credentialsPath))
                    .createScoped("https://www.googleapis.com/auth/bigquery");
            optionsBuilder.setCredentials(credentials);
        } else {
            log.info("BigQuery: using Application Default Credentials (ADC)");
        }

        BigQuery client = optionsBuilder.build().getService();
        log.info("BigQuery client initialized for project: {}", projectId);
        return client;
    }
}