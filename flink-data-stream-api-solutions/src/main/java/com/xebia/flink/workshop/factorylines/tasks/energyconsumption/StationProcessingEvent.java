package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import lombok.Data;

import java.time.Instant;

@Data
class StationProcessingEvent {
    private Instant startTimestamp;
    private Instant endTimestamp;
    private int line;
    private int station;
    private long unitId;
    private Double energyConsumption;
}
