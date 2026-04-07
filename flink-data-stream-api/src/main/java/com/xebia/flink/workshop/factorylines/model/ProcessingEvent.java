package com.xebia.flink.workshop.factorylines.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * A {@link ProcessingEvent} represents item an entering or leaving a station S on line L.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
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
