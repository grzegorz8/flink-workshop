package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StationProcessingEvent {
    private Instant startTimestamp;
    private Instant endTimestamp;
    private int line;
    private int station;
    private long unitId;
}
