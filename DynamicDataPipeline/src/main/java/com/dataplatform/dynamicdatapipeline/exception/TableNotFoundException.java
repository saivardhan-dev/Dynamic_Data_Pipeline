package com.dataplatform.dynamicdatapipeline.exception;

public class TableNotFoundException extends RuntimeException {

    private final String tableName;

    public TableNotFoundException(String tableName) {
        super("Table not registered in pipeline: '" + tableName +
                "'. Use GET /api/v1/records to see all registered tables.");
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }
}