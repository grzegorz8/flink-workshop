package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.StationProcessingEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

class JoinStartAndFinishEvents extends KeyedProcessFunction<Tuple3<Integer, Integer, Long>, ProcessingEvent, StationProcessingEvent> {

    private transient ValueState<ProcessingEvent> processingEventState;

    @Override
    public void open(OpenContext openContext) {
        this.processingEventState = getRuntimeContext()
                .getState(new ValueStateDescriptor<>("event", TypeInformation.of(ProcessingEvent.class)));
    }

    @Override
    public void processElement(ProcessingEvent currentEvent,
                               KeyedProcessFunction<Tuple3<Integer, Integer, Long>, ProcessingEvent, StationProcessingEvent>.Context ctx,
                               Collector<StationProcessingEvent> out) throws IOException {
        // TODO: what if second currentEvent never arrives?
        ProcessingEvent previousEvent = processingEventState.value();
        if (previousEvent == null) {
            processingEventState.update(currentEvent);
        } else {
            StationProcessingEvent result = new StationProcessingEvent();
            result.setLine(currentEvent.getLine());
            result.setStation(currentEvent.getStation());
            result.setUnitId(currentEvent.getUnitId());

            if (previousEvent.getAction().equals(ProcessingEvent.Action.IN)) {
                result.setStartTimestamp(previousEvent.getTimestamp());
                result.setEndTimestamp(currentEvent.getTimestamp());
            } else {
                result.setEndTimestamp(previousEvent.getTimestamp());
                result.setStartTimestamp(currentEvent.getTimestamp());
            }
            out.collect(result);
        }
    }
}
