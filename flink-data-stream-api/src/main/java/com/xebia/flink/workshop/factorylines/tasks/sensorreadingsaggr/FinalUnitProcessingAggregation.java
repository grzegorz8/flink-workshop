package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr;

import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.EnrichedStationProcessingEvent;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.UnitSummary;
import lombok.Data;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;


class FinalUnitProcessingAggregation extends KeyedProcessFunction<Long, EnrichedStationProcessingEvent, UnitSummary> {

    private final int stationsCount;

    @Data
    public static class AggregationState {
        private int count = 0;
        private double energyConsumption = 0.0d;
        private Instant startTimestamp = Instant.MAX;
        private Instant endTimestamp = Instant.MIN;
    }

    private ValueState<AggregationState> aggregationState;
    private MapState<Integer, Double> avgTemperaturesState;

    public FinalUnitProcessingAggregation(int stationsCount) {
        this.stationsCount = stationsCount;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.aggregationState = getRuntimeContext().getState(new ValueStateDescriptor<>("aggr", Types.POJO(AggregationState.class)));
        this.avgTemperaturesState = getRuntimeContext().getMapState(new MapStateDescriptor<>("avg-temperatures", Types.INT, Types.DOUBLE));
    }

    @Override
    public void processElement(EnrichedStationProcessingEvent value,
                               KeyedProcessFunction<Long, EnrichedStationProcessingEvent, UnitSummary>.Context ctx,
                               Collector<UnitSummary> out) throws Exception {
        // TODO: Implement logic
    }
}
