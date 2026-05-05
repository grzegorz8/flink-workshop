package com.xebia.flink.workshop.optimisations.statemanagement.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    private int lineId;
    private int stationId;
    private long timestamp;
    private String description;
    private String unitType;
    private String operatorId;
    private double processingDurationMs;
    private int sequenceNumber;
}