package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
public class EnrichedStationProcessingEvent {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Statistics {
        private Double energyConsumption;
        private Double avgTemperature;
    }

    private StationProcessingEvent stationProcessingEvent;
    private Statistics statistics;
}
