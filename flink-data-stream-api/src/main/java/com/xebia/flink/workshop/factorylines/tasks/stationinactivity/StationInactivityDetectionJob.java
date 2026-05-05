package com.xebia.flink.workshop.factorylines.tasks.stationinactivity;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.ProcessingEvent.Action;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

class StationInactivityDetectionJob {

    private static final int LINES_COUNT = 2;
    private static final int STATIONS_COUNT = 2;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ProcessingEvent> sourceStream = env.fromData(
                generateProcessingEvents(Instant.parse("2026-01-30T12:00:01.000Z"))
        );
        DataStream<InactivityAlert> inactivityAlerts = getPipeline(sourceStream);
        inactivityAlerts.print();

        env.execute();
    }

    static DataStream<InactivityAlert> getPipeline(DataStream<ProcessingEvent> sourceStream) {
        DataStream<ProcessingEvent> eventsWithWatermark = sourceStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ProcessingEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                                .withTimestampAssigner((SerializableTimestampAssigner<ProcessingEvent>) (element, recordTimestamp) -> element.getTimestamp().toEpochMilli())
                );

        return eventsWithWatermark
                .keyBy(e -> Tuple2.of(e.getLine(), e.getStation()), Types.TUPLE(Types.INT, Types.INT))
                .process(new StationInactivityDetectionV1(Duration.ofMinutes(5).toMillis()));
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
}
