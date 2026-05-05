package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.Instant;

@Data
@AllArgsConstructor
public class UnitSummary {
    private long unitId;
    private int line;
    private Instant startTimestamp;
    private Instant endTimestamp;
    private Double energyConsumption;
    private Double[] avgTemperatures;
}
