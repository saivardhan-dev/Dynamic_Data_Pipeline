package com.dataplatform.dynamicdatapipeline.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Validates each BigQuery record against the detected schema
 * before writing to Cosmos DB.
 *
 * Validation philosophy:
 *   - HARD reject: record missing the idField (cannot write without an id)
 *   - SOFT log:    type mismatches, null optional fields — log only, don't reject
 *
 * This lenient approach prevents over-rejection of valid records
 * that may have null optional fields or minor type variations.
 *
 * Invalid records (missing idField) are written to the sync-errors
 * dead-letter container via GenericCosmosService.writeToDeadLetter().
 */
@Slf4j
@Service
public class SchemaValidationService {

    public record ValidationResult(boolean valid, List<String> errors) {
        public static ValidationResult ok() {
            return new ValidationResult(true, List.of());
        }
        public static ValidationResult invalid(List<String> errors) {
            return new ValidationResult(false, errors);
        }
    }

    /**
     * Validate a single record.
     *
     * @param record   the BigQuery row as Map<String, Object>
     * @param schema   column name → BigQuery data type (from INFORMATION_SCHEMA)
     * @param idField  the field that must be non-null for Cosmos DB id
     * @return ValidationResult — valid=true means safe to write to Cosmos
     */
    public ValidationResult validate(Map<String, Object> record,
                                     Map<String, String> schema,
                                     String idField) {
        List<String> errors = new ArrayList<>();

        // HARD CHECK: idField must be present and non-null
        Object idValue = record.get(idField);
        if (idValue == null || idValue.toString().isBlank()) {
            errors.add("Missing required idField: '" + idField + "'");
            return ValidationResult.invalid(errors);
        }

        // SOFT CHECKS: log but don't reject
        if (schema != null && !schema.isEmpty()) {
            for (Map.Entry<String, String> schemaField : schema.entrySet()) {
                String colName = schemaField.getKey();
                String expectedType = schemaField.getValue();
                Object value = record.get(colName);

                if (value == null) {
                    // Null values are acceptable for non-id fields
                    log.debug("Null value for column '{}' (type: {})", colName, expectedType);
                    continue;
                }

                // Soft type check — log warning only
                if (!isCompatible(value, expectedType)) {
                    log.warn("Type mismatch: column='{}' expectedType='{}' actualType='{}' value='{}'",
                            colName, expectedType, value.getClass().getSimpleName(), value);
                    // Not added to errors — soft check only
                }
            }
        }

        return ValidationResult.ok();
    }

    /**
     * Validate a batch of records.
     * Returns only the records that passed validation.
     * Invalid records are written to the dead-letter store by the caller.
     */
    public List<Map<String, Object>> filterValid(List<Map<String, Object>> records,
                                                 Map<String, String> schema,
                                                 String idField,
                                                 List<Map<String, Object>> invalidOut) {
        List<Map<String, Object>> valid = new ArrayList<>();

        for (Map<String, Object> record : records) {
            ValidationResult result = validate(record, schema, idField);
            if (result.valid()) {
                valid.add(record);
            } else {
                log.warn("Record failed validation: errors={} record_id={}",
                        result.errors(), record.get(idField));
                if (invalidOut != null) {
                    invalidOut.add(record);
                }
            }
        }

        if (!invalidOut.isEmpty()) {
            log.warn("Validation: {}/{} records invalid",
                    invalidOut.size(), records.size());
        }

        return valid;
    }

    // ── Type compatibility check ─────────────────────────────────────────────

    private boolean isCompatible(Object value, String bigQueryType) {
        return switch (bigQueryType.toUpperCase()) {
            case "STRING"    -> value instanceof String;
            case "INT64",
                 "INTEGER"   -> value instanceof Long || value instanceof Integer;
            case "FLOAT64",
                 "FLOAT"     -> value instanceof Double || value instanceof Float;
            case "BOOL",
                 "BOOLEAN"   -> value instanceof Boolean;
            case "TIMESTAMP",
                 "DATE",
                 "TIME",
                 "DATETIME"  -> value instanceof String || value instanceof java.time.Instant;
            case "NUMERIC",
                 "BIGNUMERIC" -> value instanceof java.math.BigDecimal
                    || value instanceof Double
                    || value instanceof Long;
            default          -> true; // Unknown type — allow through
        };
    }
}