package com.dataplatform.dynamicdatapipeline.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SyncResult {

    public enum SyncStatus { SUCCESS, PARTIAL_SUCCESS, FAILED, SKIPPED }

    private String tableName;
    private SyncStatus status;
    private String syncMode;        // FULL or INCREMENTAL

    private long fetchedCount;
    private long savedCount;
    private long cachedCount;
    private long failedCount;
    private long validationFailedCount;

    private Instant startedAt;
    private Instant completedAt;
    private long durationMs;

    private String errorMessage;
    private List<String> warnings;

    // ── Factory methods ─────────────────────────────────────────────────────

    public static SyncResult success(String tableName, String mode,
                                     long fetched, long saved,
                                     long cached, long durationMs) {
        return SyncResult.builder()
                .tableName(tableName)
                .status(SyncStatus.SUCCESS)
                .syncMode(mode)
                .fetchedCount(fetched)
                .savedCount(saved)
                .cachedCount(cached)
                .failedCount(0)
                .durationMs(durationMs)
                .completedAt(Instant.now())
                .build();
    }

    public static SyncResult failed(String tableName, String errorMessage) {
        return SyncResult.builder()
                .tableName(tableName)
                .status(SyncStatus.FAILED)
                .fetchedCount(0)
                .savedCount(0)
                .cachedCount(0)
                .failedCount(0)
                .errorMessage(errorMessage)
                .completedAt(Instant.now())
                .build();
    }

    public static SyncResult skipped(String tableName, String reason) {
        return SyncResult.builder()
                .tableName(tableName)
                .status(SyncStatus.SKIPPED)
                .errorMessage(reason)
                .completedAt(Instant.now())
                .build();
    }
}