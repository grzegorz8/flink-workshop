package com.xebia.flink.workshop.factorylines.tasks.windowing;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Statistics {
    private int line;
    private long windowStart;
    private long windowEnd;
    private long count;
}
