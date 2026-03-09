package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.SensorReadings;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.UnitSummary;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.xebia.flink.workshop.factorylines.tasks.TestUtils.createEventsForStation;
import static com.xebia.flink.workshop.factorylines.tasks.TestUtils.createSensorReadings;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SensorReadingsAggregationJobIntegrationTest {

    public static MiniClusterWithClientResource flinkCluster;

    @BeforeAll
    static void startCluster() throws Exception {
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
        flinkCluster.before();
    }

    @BeforeEach
    void clearCollector() {
        CollectSink.values.clear();
    }

    @AfterAll
    static void stopCluster() {
        flinkCluster.after();
    }

    @Test
    void test() throws Exception {
        // given
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // and: processing events (line 0, station: 0+1, unitId={1}).
        Instant startTime = Instant.parse("2026-01-30T12:00:01.000Z");

        List<ProcessingEvent> processingEvents = Stream.of(
                        createEventsForStation(0, 0, 1L, startTime, Duration.ofSeconds(15)),
                        createEventsForStation(0, 1, 1L, startTime.plus(Duration.ofSeconds(16)), Duration.ofSeconds(15))
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        // and: sensor readings for L0S0
        List<SensorReadings> sensorReadings = new ArrayList<>();
        sensorReadings.add(createSensorReadings(0, 0, startTime, 100.0d, 25.0d));
        sensorReadings.add(createSensorReadings(0, 0, startTime.plus(Duration.ofSeconds(5L)), 110.0d, 25.0d));
        sensorReadings.add(createSensorReadings(0, 0, startTime.plus(Duration.ofSeconds(10L)), 120.0d, 25.0d));
        sensorReadings.add(createSensorReadings(0, 0, startTime.plus(Duration.ofSeconds(15L)), 130.0d, 25.0d));
        sensorReadings.add(createSensorReadings(0, 0, startTime.plus(Duration.ofSeconds(20L)), 140.0d, 25.0d));
        // and sensor readings for L0S1
        sensorReadings.add(createSensorReadings(0, 1, startTime.plus(Duration.ofSeconds(15L)), 200.0d, 35.0d));
        sensorReadings.add(createSensorReadings(0, 1, startTime.plus(Duration.ofSeconds(20L)), 200.0d, 35.0d));
        sensorReadings.add(createSensorReadings(0, 1, startTime.plus(Duration.ofSeconds(25L)), 220.0d, 35.0d));
        sensorReadings.add(createSensorReadings(0, 1, startTime.plus(Duration.ofSeconds(30L)), 240.0d, 35.0d));
        sensorReadings.add(createSensorReadings(0, 1, startTime.plus(Duration.ofSeconds(35L)), 260.0d, 35.0d));

        // when
        DataStream<UnitSummary> pipeline = SensorReadingsAggregationJob.getPipeline(env.fromData(processingEvents), env.fromData(sensorReadings));
        pipeline.addSink(new CollectSink());
        env.execute();

        // then
        assertEquals(1, CollectSink.values.size());
        assertEquals(60.0d, CollectSink.values.get(0).getEnergyConsumption());
        assertArrayEquals(new Double[]{25.0d, 35.0d}, CollectSink.values.get(0).getAvgTemperatures());

    }

    @SuppressWarnings("deprecation")
    public static class CollectSink implements SinkFunction<UnitSummary> {

        public static final List<UnitSummary> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(UnitSummary value, Context ctx) {
            values.add(value);
        }
    }

}
