package com.xebia.flink.workshop.factorylines.tasks.stationinactivity;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
class StationInactivityDetectionV1 extends KeyedProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, InactivityAlert> {
    // Assumption: events are in perfect ordering

    private final long inactivityPeriodMs;

    private transient ValueState<Long> lastSeenState;

    public StationInactivityDetectionV1(long inactivityPeriodMs) {
        this.inactivityPeriodMs = inactivityPeriodMs;
    }

    @Override
    public void open(OpenContext openContext) {
        lastSeenState = getRuntimeContext().getState(new ValueStateDescriptor<>("last-seen", Types.LONG));
    }

    @Override
    public void processElement(ProcessingEvent value,
                               KeyedProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, InactivityAlert>.Context ctx,
                               Collector<InactivityAlert> out) throws Exception {
        long timestamp = ctx.timestamp();
        Long lastSeen = lastSeenState.value();
        if (lastSeen == null) {
            lastSeenState.update(timestamp);
            ctx.timerService().registerEventTimeTimer(timestamp + inactivityPeriodMs);
        } else {
            lastSeenState.update(timestamp);
            ctx.timerService().deleteEventTimeTimer(lastSeen + inactivityPeriodMs);
            ctx.timerService().registerEventTimeTimer(timestamp + inactivityPeriodMs);
        }
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, InactivityAlert>.OnTimerContext ctx,
                        Collector<InactivityAlert> out) throws Exception {
        Tuple2<Integer, Integer> currentKey = ctx.getCurrentKey();
        InactivityAlert inactivityAlert = new InactivityAlert();
        inactivityAlert.setLine(currentKey.f0);
        inactivityAlert.setStation(currentKey.f1);
        inactivityAlert.setTimestamp(lastSeenState.value());
        out.collect(inactivityAlert);

        lastSeenState.clear();
    }
}
