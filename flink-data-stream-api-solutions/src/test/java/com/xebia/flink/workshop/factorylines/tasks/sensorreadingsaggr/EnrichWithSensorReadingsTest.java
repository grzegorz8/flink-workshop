package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr;

import com.xebia.flink.workshop.factorylines.model.SensorReadings;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.EnrichedStationProcessingEvent;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.StationProcessingEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;

class EnrichWithSensorReadingsTest {

    private KeyedTwoInputStreamOperatorTestHarness<Tuple2<Integer, Integer>, StationProcessingEvent, SensorReadings, EnrichedStationProcessingEvent> testHarness;

    @BeforeEach
    public void setupTestHarness() throws Exception {
        EnrichWithSensorReadings function = new EnrichWithSensorReadings(Duration.ofSeconds(60).toMillis());

        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(function),
                (KeySelector<StationProcessingEvent, Tuple2<Integer, Integer>>) e -> Tuple2.of(e.getLine(), e.getStation()),
                (KeySelector<SensorReadings, Tuple2<Integer, Integer>>) r -> Tuple2.of(r.getLine(), r.getStation()),
                Types.TUPLE(Types.INT, Types.INT)
        );
        testHarness.open();
    }

}