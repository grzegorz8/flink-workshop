package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.SensorReadings;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class EnrichWithEnergyConsumption
        extends KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent> {

    private transient ValueState<Long> nextLeftIndexState;
    private transient MapState<Long, ProcessingEvent> leftBuffer;
    private transient MapState<Long, SensorReadings> rightBuffer;

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.nextLeftIndexState = getRuntimeContext().getState(new ValueStateDescriptor<>("left-index", Types.LONG));
        this.leftBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>("left-buffer", Types.LONG, Types.POJO(ProcessingEvent.class)));
        this.rightBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>("right-buffer", Types.LONG, Types.POJO(SensorReadings.class)));
    }

    @Override
    public void processElement1(ProcessingEvent value,
                                KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.Context ctx,
                                Collector<EnrichedProcessingEvent> out) throws Exception {
        Long timestamp = ctx.timestamp();
        long leftIndex = getNextLeftIndex();
        leftBuffer.put(leftIndex, value);
        ctx.timerService().registerEventTimeTimer(timestamp);
    }

    private long getNextLeftIndex() throws IOException {
        Long index = nextLeftIndexState.value();
        if (index == null) {
            index = 0L;
        }
        nextLeftIndexState.update(index + 1);
        return index;
    }

    @Override
    public void processElement2(SensorReadings value,
                                KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.Context ctx,
                                Collector<EnrichedProcessingEvent> out) throws Exception {
        Long timestamp = ctx.timestamp();
        rightBuffer.put(timestamp, value);
        ctx.timerService().registerEventTimeTimer(timestamp);
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.OnTimerContext ctx,
                        Collector<EnrichedProcessingEvent> out) throws Exception {
        List<SensorReadings> sensorReadings = getSortedSensorReadings();
        List<ProcessingEvent> eventsToEmit = findEventsToEmit(ctx);
        matchEventsWithSensorReadingsAndEmit(out, sensorReadings, eventsToEmit);
        cleanUpVersionedState(sensorReadings, timestamp);

    }

    private List<SensorReadings> getSortedSensorReadings() throws Exception {
        List<SensorReadings> result = new ArrayList<>();
        for (SensorReadings sr : rightBuffer.values()) {
            result.add(sr);
        }
        result.sort(Comparator.comparing(SensorReadings::getTimestamp));
        return result;
    }

    private List<ProcessingEvent> findEventsToEmit(KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.OnTimerContext ctx) throws Exception {
        List<ProcessingEvent> eventsToEmit = new ArrayList<>();
        Iterator<Map.Entry<Long, ProcessingEvent>> iterator = leftBuffer.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, ProcessingEvent> next = iterator.next();
            Long eventTimestamp = next.getKey();
            ProcessingEvent event = next.getValue();
            if (eventTimestamp <= ctx.timerService().currentWatermark()) {
                eventsToEmit.add(event);
                iterator.remove();
            }
        }
        return eventsToEmit;
    }

    private void matchEventsWithSensorReadingsAndEmit(Collector<EnrichedProcessingEvent> out,
                                                      List<SensorReadings> sensorReadings,
                                                      List<ProcessingEvent> eventsToEmit) {
        // Both sensorReadings and eventsToEmit are sorted in ascending order by timestamp.
        for (ProcessingEvent event : eventsToEmit) {
            SensorReadings readings = findLatestLowerOrEqual(event.getTimestamp(), sensorReadings);
            out.collect(new EnrichedProcessingEvent(event, readings));
        }
    }

    private void cleanUpVersionedState(List<SensorReadings> sensorReadings, long timestamp) throws Exception {
        // Keep all sensorReadings with timestamp > current timer timestamp
        // AND the last sensorReading with timestamp <= current timer timestamp

        // 1. Find the highest timestamp <= current timer timestamp
        SensorReadings latestLower = findLatestLowerOrEqual(Instant.ofEpochMilli(timestamp), sensorReadings);
        // 2. Remove all versions with timestamp lower than "latestLower".
        if (latestLower != null) {
            for (SensorReadings sr : sensorReadings) {
                if (sr.getTimestamp().isBefore(latestLower.getTimestamp())) {
                    rightBuffer.remove(sr.getTimestamp().toEpochMilli());
                }
            }
        }
    }

    private SensorReadings findLatestLowerOrEqual(Instant timestamp, List<SensorReadings> sensorReadings) {
        // TODO: use binary search if sensorReadings list may be long. For a short list, linear complexity is fine.
        for (int i = sensorReadings.size() - 1; i >= 0; i--) {
            SensorReadings readings = sensorReadings.get(i);
            Instant r = readings.getTimestamp();
            if (!r.isAfter(timestamp)) {    // r <= timestamp
                return readings;
            }
        }
        return null;
    }
}
