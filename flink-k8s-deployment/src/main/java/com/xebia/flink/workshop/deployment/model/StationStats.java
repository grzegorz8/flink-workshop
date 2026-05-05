package com.xebia.flink.workshop.deployment.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StationStats {
    private long unitCount;
    private long minDurationMs;
    private long maxDurationMs;
}