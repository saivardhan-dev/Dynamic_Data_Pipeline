package com.dataplatform.dynamicdatapipeline.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * System container initialization — disabled.
 *
 * table-configs and sync-errors containers removed to stay within
 * the 1000 RU/s free tier limit. TableRegistry is now memory-only.
 * All 1000 RU/s is available for data containers (e.g. software).
 */
@Slf4j
@Component
public class CosmosContainerInitializer implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) {
        log.info("System container initialization skipped — memory-only mode");
    }
}