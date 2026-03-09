package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

class SumEnergyConsumption extends KeyedProcessFunction<Long, StationProcessingEvent, UnitSummary> {

    @Data
    @AllArgsConstructor
    public static class AggregationState {
        private int count;
        private double energyConsumption;
    }

    /**
     * Expected number of events per unit.
     */
    private final int stationCount;

    private ValueState<AggregationState> aggregationState;

    public SumEnergyConsumption(int stationCount) {
        this.stationCount = stationCount;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.aggregationState = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", TypeInformation.of(AggregationState.class)));
    }

    @Override
    public void processElement(StationProcessingEvent value,
                               KeyedProcessFunction<Long, StationProcessingEvent, UnitSummary>.Context ctx, Collector<UnitSummary> out) throws Exception {
        AggregationState currentState = aggregationState.value();
        if (currentState == null) {
            currentState = new AggregationState(0, 0.0d);
        }
        currentState.count++;
        currentState.energyConsumption += value.getEnergyConsumption() == null ? 0.0d : value.getEnergyConsumption();

        if (currentState.count == stationCount) {
            out.collect(new UnitSummary(ctx.getCurrentKey(), currentState.energyConsumption));
            aggregationState.clear();
        } else {
            aggregationState.update(currentState);
        }
    }
}
