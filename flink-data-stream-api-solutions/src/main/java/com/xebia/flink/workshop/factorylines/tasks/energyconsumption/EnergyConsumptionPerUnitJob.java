package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.ProcessingEvent.Action;
import com.xebia.flink.workshop.factorylines.model.SensorReadings;
import com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading.Type.ENERGY_CONSUMPTION;
import static java.util.Collections.singletonMap;

public class EnergyConsumptionPerUnitJob {

    private static final int LINES_COUNT = 2;
    private static final int STATIONS_COUNT = 2;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ProcessingEvent> processingEvents = env.fromData(
                        generateProcessingEvents(Instant.parse("2026-01-30T12:00:01.000Z"))
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ProcessingEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                                .withTimestampAssigner((SerializableTimestampAssigner<ProcessingEvent>) (element, recordTimestamp) -> element.getTimestamp().toEpochMilli())
                );

        DataStream<SensorReadings> sensorReadings = env.fromData(
                        generateSensorReadings(Instant.parse("2026-01-30T12:00:00.000Z"), Instant.parse("2026-01-30T12:02:00.000Z"))
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SensorReadings>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                                .withTimestampAssigner((SerializableTimestampAssigner<SensorReadings>) (element, recordTimestamp) -> element.getTimestamp().toEpochMilli())
                );

        processingEvents
                // join processing-events with sensor-readings
                .keyBy(e -> Tuple2.of(e.getLine(), e.getStation()), Types.TUPLE(Types.INT, Types.INT))
                .connect(sensorReadings.keyBy(e -> Tuple2.of(e.getLine(), e.getStation()), Types.TUPLE(Types.INT, Types.INT)))
                .process(new EnrichWithEnergyConsumption())
                // join IN and OUT events
                .keyBy(
                        e -> Tuple3.of(e.getProcessingEvent().getLine(), e.getProcessingEvent().getStation(), e.getProcessingEvent().getUnitId()),
                        Types.TUPLE(Types.INT, Types.INT, Types.LONG)
                )
                .process(new JoinStartAndFinishEvents())
                // aggregate
                .keyBy(StationProcessingEvent::getUnitId)
                .process(new SumEnergyConsumption(STATIONS_COUNT))
                .print();

        env.execute();

    }

    static List<ProcessingEvent> generateProcessingEvents(Instant startTime) {
        List<ProcessingEvent> output = new ArrayList<>();

        // Unit 01
        ProcessingEvent unit01InL0S0 = new ProcessingEvent();
        unit01InL0S0.setUnitId(1);
        unit01InL0S0.setTimestamp(startTime);
        unit01InL0S0.setAction(Action.IN);
        unit01InL0S0.setLine(0);
        unit01InL0S0.setStation(0);
        output.add(unit01InL0S0);

        ProcessingEvent unit01OutL0S0 = new ProcessingEvent();
        unit01OutL0S0.setUnitId(1);
        unit01OutL0S0.setTimestamp(startTime.plus(Duration.ofSeconds(30L)));
        unit01OutL0S0.setAction(Action.OUT);
        unit01OutL0S0.setLine(0);
        unit01OutL0S0.setStation(0);
        output.add(unit01OutL0S0);


        ProcessingEvent unit01InL0S1 = new ProcessingEvent();
        unit01InL0S1.setUnitId(1);
        unit01InL0S1.setTimestamp(startTime.plus(Duration.ofSeconds(35L)));
        unit01InL0S1.setAction(Action.IN);
        unit01InL0S1.setLine(0);
        unit01InL0S1.setStation(1);
        output.add(unit01InL0S1);

        ProcessingEvent unit01OutL0S1 = new ProcessingEvent();
        unit01OutL0S1.setUnitId(1);
        unit01OutL0S1.setTimestamp(startTime.plus(Duration.ofSeconds(65L)));
        unit01OutL0S1.setAction(Action.OUT);
        unit01OutL0S1.setLine(0);
        unit01OutL0S1.setStation(1);
        output.add(unit01OutL0S1);

        // Unit 02
        ProcessingEvent unit02InL1S0 = new ProcessingEvent();
        unit02InL1S0.setUnitId(2);
        unit02InL1S0.setTimestamp(startTime.plus(Duration.ofSeconds(10L)));
        unit02InL1S0.setAction(Action.IN);
        unit02InL1S0.setLine(0);
        unit02InL1S0.setStation(0);
        output.add(unit02InL1S0);

        ProcessingEvent unit02OutL1S0 = new ProcessingEvent();
        unit02OutL1S0.setUnitId(2);
        unit02OutL1S0.setTimestamp(startTime.plus(Duration.ofSeconds(20L)));
        unit02OutL1S0.setAction(Action.OUT);
        unit02OutL1S0.setLine(0);
        unit02OutL1S0.setStation(0);
        output.add(unit02OutL1S0);


        ProcessingEvent unit02InL1S1 = new ProcessingEvent();
        unit02InL1S1.setUnitId(2);
        unit02InL1S1.setTimestamp(startTime.plus(Duration.ofSeconds(22L)));
        unit02InL1S1.setAction(Action.IN);
        unit02InL1S1.setLine(0);
        unit02InL1S1.setStation(1);
        output.add(unit02InL1S1);

        ProcessingEvent unit02OutL1S1 = new ProcessingEvent();
        unit02OutL1S1.setUnitId(2);
        unit02OutL1S1.setTimestamp(startTime.plus(Duration.ofSeconds(28L)));
        unit02OutL1S1.setAction(Action.OUT);
        unit02OutL1S1.setLine(0);
        unit02OutL1S1.setStation(1);
        output.add(unit02OutL1S1);

        return output;
    }

    static List<SensorReadings> generateSensorReadings(Instant startTime, Instant endTime) {
        Random random = new Random();
        List<SensorReadings> output = new ArrayList<>();

        int iteration = 0;
        Instant currentTime = startTime;
        while (currentTime.isBefore(endTime)) {
            for (int l = 0; l < LINES_COUNT; l++) {
                for (int s = 0; s < STATIONS_COUNT; s++) {
                    SensorReadings readings = new SensorReadings();
                    readings.setLine(l);
                    readings.setStation(s);
                    readings.setTimestamp(currentTime.plus(Duration.ofMillis(random.nextInt(-10, 10))));
                    readings.setSensorReadingsMap(singletonMap(ENERGY_CONSUMPTION, new SensorReading(ENERGY_CONSUMPTION, iteration * 10.0d + random.nextDouble(-1.0d, 1.0d), "kWh")));
                    output.add(readings);
                }
            }
            iteration++;
            currentTime = currentTime.plus(Duration.ofSeconds(1));
        }
        return output;
    }
}
