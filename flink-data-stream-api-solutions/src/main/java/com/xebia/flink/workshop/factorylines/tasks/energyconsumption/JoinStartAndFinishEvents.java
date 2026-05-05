package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

import static com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading.Type.ENERGY_CONSUMPTION;

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
        // TODO: what if second currentEvent never arrives?
        EnrichedProcessingEvent previousEvent = processingEventState.value();
        if (previousEvent == null) {
            processingEventState.update(currentEvent);
        } else {
            StationProcessingEvent result = new StationProcessingEvent();
            result.setLine(currentEvent.getProcessingEvent().getLine());
            result.setStation(currentEvent.getProcessingEvent().getStation());
            result.setUnitId(currentEvent.getProcessingEvent().getUnitId());
            if (previousEvent.getProcessingEvent().getAction().equals(ProcessingEvent.Action.IN)) {
                result.setStartTimestamp(previousEvent.getProcessingEvent().getTimestamp());
                result.setEndTimestamp(currentEvent.getProcessingEvent().getTimestamp());
                result.setEnergyConsumption(calculateEnergyConsumption(previousEvent, currentEvent));
            } else {
                result.setEndTimestamp(previousEvent.getProcessingEvent().getTimestamp());
                result.setStartTimestamp(currentEvent.getProcessingEvent().getTimestamp());
                result.setEnergyConsumption(calculateEnergyConsumption(currentEvent, previousEvent));
            }
            out.collect(result);
        }
    }

    private Double calculateEnergyConsumption(EnrichedProcessingEvent startEvent, EnrichedProcessingEvent endEvent) {
        if (startEvent.getSensorReadings() == null || endEvent.getSensorReadings() == null) {
            return null;
        }
        return endEvent.getSensorReadings().getSensorReadingsMap().get(ENERGY_CONSUMPTION).getValue()
                - startEvent.getSensorReadings().getSensorReadingsMap().get(ENERGY_CONSUMPTION).getValue();
    }
}
