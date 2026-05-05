package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.SensorReadings;
import com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.UnitSummary;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading.Type.ENERGY_CONSUMPTION;
import static com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading.Type.TEMPERATURE;

class SensorReadingsAggregationJob {

    private static final int LINES_COUNT = 2;
    private static final int STATIONS_COUNT = 2;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ProcessingEvent> processingEvents = env.fromData(
                generateProcessingEvents(Instant.parse("2026-01-30T12:00:01.000Z"))
        );

        DataStream<SensorReadings> sensorReadings = env.fromData(
                generateSensorReadings(Instant.parse("2026-01-30T12:00:00.000Z"), Instant.parse("2026-01-30T12:02:00.000Z"))
        );

        DataStream<UnitSummary> unitSummaries = getPipeline(processingEvents, sensorReadings);
        unitSummaries.print();

        env.execute();
    }

    static DataStream<UnitSummary> getPipeline(DataStream<ProcessingEvent> processingEvents,
                                               DataStream<SensorReadings> sensorReadings) {
        DataStream<ProcessingEvent> processingEventsWithWatermark = processingEvents.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<ProcessingEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                        .withTimestampAssigner((SerializableTimestampAssigner<ProcessingEvent>) (element, recordTimestamp) -> element.getTimestamp().toEpochMilli())
        );

        DataStream<SensorReadings> sensorReadingsWithWatermark = sensorReadings.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<SensorReadings>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                        .withTimestampAssigner((SerializableTimestampAssigner<SensorReadings>) (element, recordTimestamp) -> element.getTimestamp().toEpochMilli())
        );

        return processingEventsWithWatermark
                // join IN and OUT processing events
                .keyBy(e -> Tuple3.of(e.getLine(), e.getStation(), e.getUnitId()), Types.TUPLE(Types.INT, Types.INT, Types.LONG))
                .process(new JoinStartAndFinishEvents())
                // enrich with sensor readings
                .keyBy(e -> Tuple2.of(e.getLine(), e.getStation()), Types.TUPLE(Types.INT, Types.INT))
                .connect(sensorReadingsWithWatermark.keyBy(e -> Tuple2.of(e.getLine(), e.getStation()), Types.TUPLE(Types.INT, Types.INT)))
                .process(new EnrichWithSensorReadings(Duration.ofSeconds(60L).toMillis()))
                // calculate final output
                .keyBy(e -> e.getStationProcessingEvent().getUnitId())
                .process(new FinalUnitProcessingAggregation(STATIONS_COUNT));
    }

    static List<ProcessingEvent> generateProcessingEvents(Instant startTime) {
        List<ProcessingEvent> output = new ArrayList<>();
        // Unit 01
        output.addAll(createEventsForStation(0, 0, 1L, startTime, Duration.ofSeconds(30)));
        output.addAll(createEventsForStation(0, 1, 1L, startTime.plus(Duration.ofSeconds(35)), Duration.ofSeconds(30)));
        // Unit 02
        output.addAll(createEventsForStation(1, 0, 2L, startTime.plus(Duration.ofSeconds(10)), Duration.ofSeconds(10)));
        output.addAll(createEventsForStation(1, 1, 2L, startTime.plus(Duration.ofSeconds(22)), Duration.ofSeconds(28)));
        return output;
    }

    private static List<ProcessingEvent> createEventsForStation(int line, int station, long unitId, Instant startTime, Duration duration) {
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
                    Map<SensorReading.Type, SensorReading> readingsMap = new HashMap<>(2);
                    readingsMap.put(ENERGY_CONSUMPTION, new SensorReading(ENERGY_CONSUMPTION, iteration * 10.0d + random.nextDouble(-1.0d, 1.0d), "kWh"));
                    readingsMap.put(TEMPERATURE, new SensorReading(TEMPERATURE, random.nextDouble(10.0d, 70.0d), "C"));
                    readings.setSensorReadingsMap(readingsMap);
                    output.add(readings);
                }
            }
            iteration++;
            currentTime = currentTime.plus(Duration.ofSeconds(1));
        }
        return output;
    }
}
