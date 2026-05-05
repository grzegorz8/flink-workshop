package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model;

import lombok.Data;

import java.time.Instant;

@Data
public class StationProcessingEvent {
    private Instant startTimestamp;
    private Instant endTimestamp;
    private int line;
    private int station;
    private long unitId;
}
