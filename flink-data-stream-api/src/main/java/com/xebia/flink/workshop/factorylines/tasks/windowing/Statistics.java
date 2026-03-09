package com.xebia.flink.workshop.factorylines.tasks.windowing;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Statistics {
    private long windowStart;
    private long windowEnd;
    private long count;
}
