package com.xebia.flink.workshop.factorylines.tasks.windowing;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.ProcessingEvent.Action;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

class WindowingJob {

    private static final int LINES_COUNT = 2;
    private static final int STATIONS_COUNT = 2;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ProcessingEvent> sourceStream = env.fromData(
                generateProcessingEvents(Instant.parse("2026-01-30T12:00:01.000Z"))
        );
        DataStream<Statistics> aggregated = getPipeline(sourceStream);
        aggregated.print();

        env.execute();
    }

    static DataStream<Statistics> getPipeline(DataStream<ProcessingEvent> sourceStream) {
        DataStream<ProcessingEvent> eventsWithWatermark = sourceStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ProcessingEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                                .withTimestampAssigner((SerializableTimestampAssigner<ProcessingEvent>) (element, recordTimestamp) -> element.getTimestamp().toEpochMilli())
                );

        return eventsWithWatermark
                .filter(new FilterLastStationOutEvents())
                .keyBy(ProcessingEvent::getLine)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5L)))
                .aggregate(new CountEventsAggregateFunction(), new CountEventsWindowFunction());
    }

    static class FilterLastStationOutEvents implements FilterFunction<ProcessingEvent> {
        @Override
        public boolean filter(ProcessingEvent value) {
            // station numbers starts from 0.
            return value.getStation() == STATIONS_COUNT - 1 && value.getAction() == Action.OUT;
        }
    }

    static class CountEventsAggregateFunction implements AggregateFunction<ProcessingEvent, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(ProcessingEvent value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    static class CountEventsWindowFunction implements WindowFunction<Integer, Statistics, Integer, TimeWindow> {
        @Override
        public void apply(Integer key, TimeWindow window, Iterable<Integer> input, Collector<Statistics> out) {
            // There is only one input element - accumulator value.
            int count = 0;
            for (Integer i : input) {
                count += i;
            }
            out.collect(new Statistics(window.getStart(), window.getEnd(), count));
        }
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
