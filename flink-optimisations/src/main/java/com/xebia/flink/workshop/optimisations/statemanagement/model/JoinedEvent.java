package com.xebia.flink.workshop.optimisations.statemanagement.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinedEvent {
    private Event event;
    private SensorReading sensorReading;
    private long timeDeltaMs;
    private long iterations;
}