package com.xebia.flink.workshop.factorylines.tasks;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.SensorReadings;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading.Type.ENERGY_CONSUMPTION;
import static com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading.Type.TEMPERATURE;

public class TestUtils {

    public static List<ProcessingEvent> createEventsForStation(int line, int station, long unitId, Instant startTime, Duration duration) {
        ProcessingEvent in = new ProcessingEvent();
        in.setUnitId(unitId);
        in.setTimestamp(startTime);
        in.setAction(ProcessingEvent.Action.IN);
        in.setLine(line);
        in.setStation(station);

        ProcessingEvent out = new ProcessingEvent();
        out.setUnitId(unitId);
        out.setTimestamp(startTime.plus(duration));
        out.setAction(ProcessingEvent.Action.OUT);
        out.setLine(line);
        out.setStation(station);

        return List.of(in, out);
    }

    public static SensorReadings createSensorReadings(int line, int station, Instant timestamp, double energy, double temperature) {
        SensorReadings readings = new SensorReadings();
        readings.setLine(line);
        readings.setStation(station);
        readings.setTimestamp(timestamp);
        Map<SensorReadings.SensorReading.Type, SensorReadings.SensorReading> readingsMap = new HashMap<>(2);
        readingsMap.put(ENERGY_CONSUMPTION, new SensorReadings.SensorReading(ENERGY_CONSUMPTION, energy, "kWh"));
        readingsMap.put(TEMPERATURE, new SensorReadings.SensorReading(TEMPERATURE, temperature, "C"));
        readings.setSensorReadingsMap(readingsMap);
        return readings;
    }

}
