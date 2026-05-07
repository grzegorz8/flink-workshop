package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.SensorReadings;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class EnrichWithEnergyConsumptionWithLateness
        extends KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent> {

    private final Duration allowedLateness;

    private transient MapState<Long, List<ProcessingEvent>> leftBuffer;
    private transient MapState<Long, SensorReadings> rightBuffer;

    EnrichWithEnergyConsumptionWithLateness(Duration allowedLateness) {
        this.allowedLateness = allowedLateness;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.leftBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>("left-buffer", Types.LONG, Types.LIST(Types.POJO(ProcessingEvent.class))));
        this.rightBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>("right-buffer", Types.LONG, Types.POJO(SensorReadings.class)));
    }

    @Override
    public void processElement1(ProcessingEvent value,
                                KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.Context ctx,
                                Collector<EnrichedProcessingEvent> out) throws Exception {
        long timestamp = ctx.timestamp();
        long watermark = ctx.timerService().currentWatermark();

        if (timestamp <= watermark) {
            // Late event: emit immediately if within allowed lateness, otherwise drop.
            long latenessThreshold = watermark - allowedLateness.toMillis();
            if (timestamp > latenessThreshold) {
                List<SensorReadings> sensorReadings = getSortedSensorReadings();
                SensorReadings readings = findLatestLowerOrEqual(value.getTimestamp(), sensorReadings);
                out.collect(new EnrichedProcessingEvent(value, readings));
            }
            // else: beyond allowed lateness - silently drop.
        } else {
            // On-time event: buffer and wait until the watermark reaches this timestamp.
            List<ProcessingEvent> events = leftBuffer.get(timestamp);
            if (events == null) {
                events = new ArrayList<>();
            }
            events.add(value);
            leftBuffer.put(timestamp, events);
            ctx.timerService().registerEventTimeTimer(timestamp);
        }
    }

    @Override
    public void processElement2(SensorReadings value,
                                KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.Context ctx,
                                Collector<EnrichedProcessingEvent> out) throws Exception {
        long timestamp = ctx.timestamp();
        rightBuffer.put(timestamp, value);
        ctx.timerService().registerEventTimeTimer(timestamp);
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.OnTimerContext ctx,
                        Collector<EnrichedProcessingEvent> out) throws Exception {
        List<SensorReadings> sensorReadings = getSortedSensorReadings();
        matchAndEmit(ctx, out, sensorReadings);
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

    private void matchAndEmit(KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.OnTimerContext ctx,
                              Collector<EnrichedProcessingEvent> out,
                              List<SensorReadings> sensorReadings) throws Exception {
        long watermark = ctx.timerService().currentWatermark();
        Iterator<Map.Entry<Long, List<ProcessingEvent>>> iterator = leftBuffer.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<ProcessingEvent>> next = iterator.next();
            Long eventTimestamp = next.getKey();
            if (eventTimestamp <= watermark) {
                for (ProcessingEvent event : next.getValue()) {
                    SensorReadings readings = findLatestLowerOrEqual(event.getTimestamp(), sensorReadings);
                    out.collect(new EnrichedProcessingEvent(event, readings));
                }
                iterator.remove();
            }
        }
    }

    private void cleanUpVersionedState(List<SensorReadings> sensorReadings, long timestamp) throws Exception {
        long cleanupBoundary = timestamp - allowedLateness.toMillis();
        SensorReadings latestLower = findLatestLowerOrEqual(Instant.ofEpochMilli(cleanupBoundary), sensorReadings);
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