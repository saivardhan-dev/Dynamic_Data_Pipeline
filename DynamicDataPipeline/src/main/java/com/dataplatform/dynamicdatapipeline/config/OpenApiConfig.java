package com.dataplatform.dynamicdatapipeline.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.tags.Tag;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Dynamic Data Pipeline API")
                        .description("Metadata-driven pipeline: BigQuery → Cosmos DB → EhCache → REST APIs. " +
                                "All endpoints are generic — table name is a path variable.")
                        .version("2.0.0"))
                .tags(List.of(
                        new Tag().name("Records").description("Generic record read APIs (cache-aside)"),
                        new Tag().name("Admin").description("Table registry management and sync control")
                ));
    }
}