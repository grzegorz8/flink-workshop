package com.xebia.flink.workshop.optimisations.statemanagement;

import com.xebia.flink.workshop.optimisations.statemanagement.model.Event;
import com.xebia.flink.workshop.optimisations.statemanagement.model.JoinedEvent;
import com.xebia.flink.workshop.optimisations.statemanagement.model.SensorReading;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class IterateEntriesSortedRememberKeyJoin
        extends KeyedCoProcessFunction<Tuple2<Integer, Integer>, Event, SensorReading, JoinedEvent> {

    private transient MapState<Long, SensorReading> rightBuffer;

    @Override
    public void open(OpenContext openContext) {
        rightBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>("sensor-readings", Types.LONG, Types.POJO(SensorReading.class)));
    }

    @Override
    public void processElement1(Event event, Context ctx, Collector<JoinedEvent> out) throws Exception {
        Long closestKey = null;
        long minDelta = Long.MAX_VALUE;
        long iterations = 0;

        for (Map.Entry<Long, SensorReading> entry : rightBuffer.entries()) {
            iterations++;
            long key = entry.getKey();

            // All remaining keys are larger and their delta can only grow — stop early.
            if (minDelta != Long.MAX_VALUE && key > event.getTimestamp() + minDelta) {
                break;
            }

            long delta = Math.abs(event.getTimestamp() - key);
            if (delta < minDelta) {
                minDelta = delta;
                closestKey = key;
            }
        }

        if (closestKey != null) {
            out.collect(new JoinedEvent(event, rightBuffer.get(closestKey), minDelta, iterations));
        }
    }

    @Override
    public void processElement2(SensorReading reading, Context ctx, Collector<JoinedEvent> out) throws Exception {
        rightBuffer.put(reading.getTimestamp(), reading);
    }
}