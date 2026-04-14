package com.xebia.flink.workshop.deployment.model;

import lombok.Data;

import java.time.Instant;

/**
 * Represents a unit entering ({@link Action#IN}) or leaving ({@link Action#OUT}) a station.
 */
@Data
public class ProcessingEvent {

    public enum Action {
        IN, OUT
    }

    private Instant timestamp;
    private int line;
    private int station;
    private long unitId;
    private Action action;
}