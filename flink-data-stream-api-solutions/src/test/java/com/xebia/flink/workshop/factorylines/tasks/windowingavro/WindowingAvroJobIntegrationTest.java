package com.xebia.flink.workshop.factorylines.tasks.windowingavro;

import com.xebia.flink.workshop.factorylines.model.Action;
import com.xebia.flink.workshop.factorylines.model.ProcessingEventAvro;
import com.xebia.flink.workshop.factorylines.model.StatisticsAvro;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.xebia.flink.workshop.factorylines.model.Action.OUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowingAvroJobIntegrationTest {

    static MiniClusterWithClientResource flinkCluster;

    @BeforeAll
    static void startCluster() throws Exception {
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
        flinkCluster.before();
    }

    @AfterAll
    static void stopCluster() {
        flinkCluster.after();
    }

    @BeforeEach
    void clearCollector() {
        CollectSink.values.clear();
    }

    @Test
    void shouldProduceOneResultForTwoUnitsOnSameLineInTheSameWindow() throws Exception {
        // given
        List<ProcessingEventAvro> events = List.of(
                ProcessingEventAvro.newBuilder().setLine(0).setStation(1).setUnitId(1L).setAction(OUT).setTimestamp(Instant.parse("2026-01-30T12:00:29.000Z")).build(),
                ProcessingEventAvro.newBuilder().setLine(0).setStation(1).setUnitId(2L).setAction(OUT).setTimestamp(Instant.parse("2026-01-30T12:01:06.000Z")).build()
        );

        // when
        runPipeline(events);

        // then
        assertEquals(1, CollectSink.values.size());
        assertEquals(
                CollectSink.values.get(0),
                StatisticsAvro.newBuilder()
                        .setLine(0)
                        .setWindowStart(Instant.parse("2026-01-30T12:00:00.000Z"))
                        .setWindowEnd(Instant.parse("2026-01-30T12:05:00.000Z"))
                        .setCount(2L)
                        .build()
        );
    }

    @Test
    void shouldProduceTwoResultsForTwoUnitsInDifferentWindows() throws Exception {
        // given
        Instant firstWindow  = Instant.parse("2026-01-30T12:02:00.000Z");
        Instant secondWindow = Instant.parse("2026-01-30T12:07:00.000Z");

        List<ProcessingEventAvro> events = List.of(
                ProcessingEventAvro.newBuilder().setLine(0).setStation(1).setUnitId(1L).setAction(OUT).setTimestamp(firstWindow).build(),
                ProcessingEventAvro.newBuilder().setLine(0).setStation(1).setUnitId(2L).setAction(OUT).setTimestamp(secondWindow).build()
        );

        // when
        runPipeline(events);

        // then
        assertEquals(2, CollectSink.values.size());
        assertTrue(CollectSink.values.contains(
                StatisticsAvro.newBuilder()
                        .setLine(0)
                        .setWindowStart(Instant.parse("2026-01-30T12:00:00.000Z"))
                        .setWindowEnd(Instant.parse("2026-01-30T12:05:00.000Z"))
                        .setCount(1L)
                        .build()
        ));
        assertTrue(CollectSink.values.contains(
                StatisticsAvro.newBuilder()
                        .setLine(0)
                        .setWindowStart(Instant.parse("2026-01-30T12:05:00.000Z"))
                        .setWindowEnd(Instant.parse("2026-01-30T12:10:00.000Z"))
                        .setCount(1L)
                        .build()
        ));
    }

    @Test
    void shouldProduceTwoResultsForTwoItemsOnDifferentLinesInSameWindow() throws Exception {
        // given
        Instant eventTime = Instant.parse("2026-01-30T12:02:00.000Z");

        List<ProcessingEventAvro> events = List.of(
                ProcessingEventAvro.newBuilder().setLine(0).setStation(1).setUnitId(1L).setAction(OUT).setTimestamp(eventTime).build(),
                ProcessingEventAvro.newBuilder().setLine(1).setStation(1).setUnitId(2L).setAction(OUT).setTimestamp(eventTime).build()
        );

        // when
        runPipeline(events);

        // then
        assertEquals(2, CollectSink.values.size());
        assertTrue(CollectSink.values.contains(
                StatisticsAvro.newBuilder()
                        .setLine(0)
                        .setWindowStart(Instant.parse("2026-01-30T12:00:00.000Z"))
                        .setWindowEnd(Instant.parse("2026-01-30T12:05:00.000Z"))
                        .setCount(1L)
                        .build()
        ));
        assertTrue(CollectSink.values.contains(
                StatisticsAvro.newBuilder()
                        .setLine(1)
                        .setWindowStart(Instant.parse("2026-01-30T12:00:00.000Z"))
                        .setWindowEnd(Instant.parse("2026-01-30T12:05:00.000Z"))
                        .setCount(1L)
                        .build()
        ));
    }

    private static void runPipeline(List<ProcessingEventAvro> events) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<ProcessingEventAvro> source = env.fromData(events);
        WindowingAvroJob.getPipeline(source).addSink(new CollectSink());
        env.execute();
    }

    @SuppressWarnings("deprecation")
    static class CollectSink implements SinkFunction<StatisticsAvro> {

        static final List<StatisticsAvro> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(StatisticsAvro value, Context ctx) {
            values.add(value);
        }
    }
}