package com.dataplatform.dynamicdatapipeline.controller;

import com.dataplatform.dynamicdatapipeline.dto.PagedResponse;
import com.dataplatform.dynamicdatapipeline.model.TableConfig;
import com.dataplatform.dynamicdatapipeline.service.GenericCacheService;
import com.dataplatform.dynamicdatapipeline.service.GenericCosmosService;
import com.dataplatform.dynamicdatapipeline.service.TableRegistry;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Generic record read API — one controller handles ALL registered tables.
 *
 * The {table} path variable is looked up in TableRegistry at runtime.
 * Returns 404 with a clear message if the table is not registered.
 *
 * All read operations use the cache-aside pattern:
 *   1. Check EhCache (key: tableName:id)
 *   2. HIT  → return immediately (< 1ms)
 *   3. MISS → query Cosmos DB → populate cache → return (~600ms)
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/records")
@RequiredArgsConstructor
@Tag(name = "Records", description = "Generic record read APIs — all tables")
public class GenericRecordController {

    private final TableRegistry tableRegistry;
    private final GenericCacheService cacheService;
    private final GenericCosmosService cosmosService;

    // ── GET /api/v1/records ─────────────────────────────────────────────────

    @GetMapping
    @Operation(summary = "List all registered tables",
            description = "Returns all tables currently registered in the pipeline with their sync status")
    public ResponseEntity<List<Map<String, Object>>> listTables() {
        List<Map<String, Object>> tables = tableRegistry.getAll().stream()
                .map(config -> {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("name",            config.getName());
                    row.put("bqTable",         config.getBqTable());
                    row.put("cosmosContainer", config.getCosmosContainer());
                    row.put("idField",         config.getIdField());
                    row.put("autoDiscovered",  config.isAutoDiscovered());
                    row.put("lastSyncTime",    String.valueOf(config.getLastSyncTime()));
                    row.put("lastSyncStatus",  String.valueOf(config.getLastSyncStatus()));
                    row.put("cacheTtlMinutes", config.getCacheTtlMinutes());
                    return row;
                })
                .collect(Collectors.toList());

        return ResponseEntity.ok(tables);
    }

    // ── GET /api/v1/records/{table}/{id} ────────────────────────────────────

    @GetMapping("/{table}/{id}")
    @Operation(summary = "Get a record by ID",
            description = "Cache-aside: checks EhCache first, falls back to Cosmos DB on miss")
    public ResponseEntity<Map<String, Object>> getById(
            @Parameter(description = "Registered table name", example = "orders")
            @PathVariable String table,
            @Parameter(description = "Record ID", example = "ord-0000001")
            @PathVariable String id) {

        TableConfig config = tableRegistry.get(table); // throws 404 if not registered

        // Cache-aside pattern
        Optional<Map<String, Object>> cached = cacheService.get(table, id);
        if (cached.isPresent()) {
            return ResponseEntity.ok(cached.get());
        }

        // Cache MISS → query Cosmos DB
        Optional<Map<String, Object>> fromCosmos =
                cosmosService.findById(config.getCosmosContainer(), id);

        if (fromCosmos.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        // Populate cache for next request
        cacheService.put(table, id, fromCosmos.get());
        return ResponseEntity.ok(fromCosmos.get());
    }

    // ── GET /api/v1/records/{table}?page=0&size=20 ──────────────────────────

    @GetMapping("/{table}")
    @Operation(summary = "List records with pagination",
            description = "Paginated list from Cosmos DB. Use page and size query params.")
    public ResponseEntity<PagedResponse<Map<String, Object>>> listRecords(
            @Parameter(description = "Registered table name", example = "orders")
            @PathVariable String table,
            @RequestParam(defaultValue = "0")  int page,
            @RequestParam(defaultValue = "20") int size) {

        TableConfig config = tableRegistry.get(table);

        // Clamp page size to prevent abuse
        size = Math.min(size, 1000);

        List<Map<String, Object>> items =
                cosmosService.findAll(config.getCosmosContainer(), page, size);
        long total = cosmosService.count(config.getCosmosContainer());

        return ResponseEntity.ok(
                PagedResponse.of(items, page, size, total, "cosmos"));
    }

    // ── GET /api/v1/records/{table}/search?field=value ──────────────────────

    @GetMapping("/{table}/search")
    @Operation(summary = "Search records by field value",
            description = "Returns records where the specified field equals the given value. " +
                    "Field must be indexed (all fields are indexed by default).")
    public ResponseEntity<List<Map<String, Object>>> search(
            @Parameter(description = "Registered table name", example = "orders")
            @PathVariable String table,
            @Parameter(description = "Field name to search on", example = "category")
            @RequestParam String field,
            @Parameter(description = "Value to match", example = "software")
            @RequestParam String value) {

        TableConfig config = tableRegistry.get(table);

        List<Map<String, Object>> results =
                cosmosService.findByField(config.getCosmosContainer(), field, value);

        return ResponseEntity.ok(results);
    }
}