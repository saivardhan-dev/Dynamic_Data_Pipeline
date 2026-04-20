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
public class DiscoveryResult {

    /** Tables newly found and registered during this scan */
    private List<String> newlyDiscovered;

    /** Tables already registered — no action taken */
    private List<String> alreadyRegistered;

    /** Tables skipped due to excluded-tables list */
    private List<String> excluded;

    /** Tables that failed to register or sync */
    private List<String> errors;

    private Instant scannedAt;
    private long durationMs;
}