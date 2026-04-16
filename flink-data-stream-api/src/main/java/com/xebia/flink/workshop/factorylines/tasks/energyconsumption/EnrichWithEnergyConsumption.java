package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.SensorReadings;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class EnrichWithEnergyConsumption
        extends KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent> {

    private transient ValueState<Long> nextLeftIndexState;
    private transient MapState<Long, ProcessingEvent> leftBuffer;
    private transient MapState<Long, SensorReadings> rightBuffer;

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.nextLeftIndexState = getRuntimeContext().getState(new ValueStateDescriptor<>("left-index", Types.LONG));
        this.leftBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>("left-buffer", Types.LONG, Types.POJO(ProcessingEvent.class)));
        this.rightBuffer = getRuntimeContext().getMapState(new MapStateDescriptor<>("right-buffer", Types.LONG, Types.POJO(SensorReadings.class)));
    }

    @Override
    public void processElement1(ProcessingEvent value,
                                KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.Context ctx,
                                Collector<EnrichedProcessingEvent> out) throws Exception {
        Long timestamp = ctx.timestamp();
        long leftIndex = getNextLeftIndex();
        leftBuffer.put(leftIndex, value);
        ctx.timerService().registerEventTimeTimer(timestamp);
    }

    private long getNextLeftIndex() throws IOException {
        Long index = nextLeftIndexState.value();
        if (index == null) {
            index = 0L;
        }
        nextLeftIndexState.update(index + 1);
        return index;
    }

    @Override
    public void processElement2(SensorReadings value,
                                KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.Context ctx,
                                Collector<EnrichedProcessingEvent> out) throws Exception {
        // TODO: Buffer the SensorReadings in rightBuffer keyed by timestamp.
        //       Register an event-time timer for rightBuffer cleanup.
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.OnTimerContext ctx,
                        Collector<EnrichedProcessingEvent> out) throws Exception {
        // TODO: When the timer fires:
        //   * Match ProcessingEvents with SensorReadings
        //   * Clean up buffers
    }

}
