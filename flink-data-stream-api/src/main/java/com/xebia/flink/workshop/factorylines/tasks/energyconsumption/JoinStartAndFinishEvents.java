package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

class JoinStartAndFinishEvents extends KeyedProcessFunction<Tuple3<Integer, Integer, Long>, EnrichedProcessingEvent, StationProcessingEvent> {

    private transient ValueState<EnrichedProcessingEvent> processingEventState;

    @Override
    public void open(OpenContext openContext) {
        this.processingEventState = getRuntimeContext().getState(new ValueStateDescriptor<>("event", TypeInformation.of(EnrichedProcessingEvent.class)));
    }

    @Override
    public void processElement(EnrichedProcessingEvent currentEvent,
                               KeyedProcessFunction<Tuple3<Integer, Integer, Long>, EnrichedProcessingEvent, StationProcessingEvent>.Context ctx,
                               Collector<StationProcessingEvent> out) throws IOException {
        // TODO: Store the first event (IN or OUT) in state.
        // When the second event arrives, combine them into a StationProcessingEvent:
    }
}