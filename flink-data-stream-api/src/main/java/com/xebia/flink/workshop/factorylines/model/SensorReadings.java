package com.xebia.flink.workshop.factorylines.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * {@link SensorReadings} represents a set of sensor readings collected at given time on station S on line L.
 */
@Data
public class SensorReadings {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SensorReading {

        public enum Type {
            ENERGY_CONSUMPTION, TEMPERATURE
        }

        private Type type;
        private double value;
        private String unit;
    }

    private Instant timestamp;
    private int line;
    private int station;
    private Map<SensorReading.Type, SensorReading> sensorReadingsMap;
}
