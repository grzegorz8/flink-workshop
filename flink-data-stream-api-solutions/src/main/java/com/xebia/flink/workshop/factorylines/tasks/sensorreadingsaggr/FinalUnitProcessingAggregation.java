package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr;

import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.EnrichedStationProcessingEvent;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.StationProcessingEvent;
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
import java.util.Map;
import java.util.stream.StreamSupport;


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
        StationProcessingEvent processingEvent = value.getStationProcessingEvent();

        AggregationState state = aggregationState.value();
        if (state == null) {
            state = new AggregationState();
        }
        state.count++;
        state.energyConsumption += value.getStatistics().getEnergyConsumption();
        if (processingEvent.getStartTimestamp().isBefore(state.startTimestamp)) {
            state.startTimestamp = processingEvent.getStartTimestamp();
        }
        if (processingEvent.getEndTimestamp().isAfter(state.endTimestamp)) {
            state.endTimestamp = processingEvent.getEndTimestamp();
        }
        aggregationState.update(state);

        avgTemperaturesState.put(processingEvent.getStation(), value.getStatistics().getAvgTemperature());

        if (state.count == stationsCount) {
            Double[] avgTemperatures = StreamSupport.stream(avgTemperaturesState.entries().spliterator(), false)
                    .sorted(Map.Entry.comparingByKey())
                    .map(Map.Entry::getValue)
                    .toArray(Double[]::new);

            out.collect(new UnitSummary(
                    processingEvent.getUnitId(),
                    processingEvent.getLine(),
                    state.getStartTimestamp(),
                    state.getEndTimestamp(),
                    state.energyConsumption,
                    avgTemperatures
            ));
            aggregationState.clear();
            avgTemperaturesState.clear();
        }
    }
}
