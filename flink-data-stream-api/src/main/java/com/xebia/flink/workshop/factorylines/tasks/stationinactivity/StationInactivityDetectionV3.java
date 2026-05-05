package com.xebia.flink.workshop.factorylines.tasks.stationinactivity;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

@Slf4j
class StationInactivityDetectionV3 extends KeyedProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, InactivityAlert> {

    private static final OutputTag<ProcessingEvent> LATE_EVENTS = new OutputTag<>("late-events") {
    };

    private transient MapState<Long, ProcessingEvent> buffer;
    private transient ValueState<Long> lastSeenState;

    private final long inactivityPeriodMs;

    public StationInactivityDetectionV3(long inactivityPeriodMs) {
        this.inactivityPeriodMs = inactivityPeriodMs;
    }

    @Override
    public void open(OpenContext openContext) {
        buffer = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("buffer", Types.LONG, Types.POJO(ProcessingEvent.class)));
        lastSeenState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-seen", Types.LONG));
    }

    @Override
    public void processElement(ProcessingEvent value,
                               KeyedProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, InactivityAlert>.Context ctx,
                               Collector<InactivityAlert> out) throws Exception {
        // Truncate timestamp to full second.
        long timestamp = coalesce(ctx.timestamp());
        if (timestamp < ctx.timerService().currentWatermark()) {
            log.debug("Late event: {}.", value);
            ctx.output(LATE_EVENTS, value);
            return;
        }

        if (!buffer.contains(timestamp)) {
            buffer.put(timestamp, value);
        }
        ctx.timerService().registerEventTimeTimer(timestamp);
    }

    private long coalesce(Long timestamp) {
        if (timestamp % 1000L == 0) {
            return timestamp;
        } else {
            return (timestamp / 1000L) * 1000L + 1000L;
        }
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, InactivityAlert>.OnTimerContext ctx,
                        Collector<InactivityAlert> out) throws Exception {
        // Event-time timer can be fired:
        // (1) when the event is ready for processing; by buffering events we ensure they are processed in order.
        // (2) when inactivity period has expired.
        checkInactivityPeriod(timestamp, ctx, out);
        processBufferedEvents(timestamp, ctx);
    }

    private void checkInactivityPeriod(long timestamp,
                                       KeyedProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, InactivityAlert>.OnTimerContext ctx,
                                       Collector<InactivityAlert> out) throws IOException {
        Long lastSeen = lastSeenState.value();
        if (lastSeen != null && lastSeen + inactivityPeriodMs <= timestamp) {
            Tuple2<Integer, Integer> currentKey = ctx.getCurrentKey();
            InactivityAlert inactivityAlert = new InactivityAlert();
            inactivityAlert.setLine(currentKey.f0);
            inactivityAlert.setStation(currentKey.f1);
            inactivityAlert.setTimestamp(lastSeenState.value());
            out.collect(inactivityAlert);

            lastSeenState.clear();
        }
    }

    private void processBufferedEvents(long timestamp,
                                       KeyedProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, InactivityAlert>.OnTimerContext ctx) throws Exception {
        ProcessingEvent event = buffer.get(timestamp);
        if (event != null) {
            buffer.remove(timestamp);
            lastSeenState.update(timestamp);
            ctx.timerService().registerEventTimeTimer(timestamp + inactivityPeriodMs);
        }
    }

}
