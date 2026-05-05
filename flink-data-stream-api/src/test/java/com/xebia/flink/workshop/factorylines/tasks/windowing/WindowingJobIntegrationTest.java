package com.xebia.flink.workshop.factorylines.tasks.windowing;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.xebia.flink.workshop.factorylines.model.ProcessingEvent.Action.OUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WindowingJobIntegrationTest {

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

    @Disabled   // TODO remove when code is implemented
    @Test
    void shouldProduceOneResultForTwoUnitsOnSameLineInTheSameWindow() throws Exception {
        // given
        List<ProcessingEvent> events = List.of(
                ProcessingEvent.builder().line(0).station(1).unitId(1L).action(OUT).timestamp(Instant.parse("2026-01-30T12:00:29.000Z")).build(),
                ProcessingEvent.builder().line(0).station(1).unitId(2L).action(OUT).timestamp(Instant.parse("2026-01-30T12:01:06.000Z")).build()
        );

        // when
        runPipeline(events);

        // then
        assertEquals(1, CollectSink.values.size());
        assertEquals(
                CollectSink.values.get(0),
                Statistics.builder()
                        .line(0)
                        .windowStart(Instant.parse("2026-01-30T12:00:00.000Z").toEpochMilli())
                        .windowEnd(Instant.parse("2026-01-30T12:05:00.000Z").toEpochMilli())
                        .count(2)
                        .build()
        );
    }

    @Disabled   // TODO remove when code is implemented
    @Test
    void shouldProduceTwoResultsForTwoUnitsInDifferentWindows() throws Exception {
        // given
        Instant firstWindow = Instant.parse("2026-01-30T12:02:00.000Z");
        Instant secondWindow = Instant.parse("2026-01-30T12:07:00.000Z");

        List<ProcessingEvent> events = List.of(
                ProcessingEvent.builder().line(0).station(1).unitId(1L).action(OUT).timestamp(firstWindow).build(),
                ProcessingEvent.builder().line(0).station(1).unitId(2L).action(OUT).timestamp(secondWindow).build()
        );

        // when
        runPipeline(events);

        // then
        List<Statistics> results = CollectSink.values;

        assertEquals(2, results.size());
        assertTrue(CollectSink.values.contains(
                Statistics.builder()
                        .line(0)
                        .windowStart(Instant.parse("2026-01-30T12:00:00.000Z").toEpochMilli())
                        .windowEnd(Instant.parse("2026-01-30T12:05:00.000Z").toEpochMilli())
                        .count(1)
                        .build()
        ));
        assertTrue(CollectSink.values.contains(
                Statistics.builder()
                        .line(0)
                        .windowStart(Instant.parse("2026-01-30T12:05:00.000Z").toEpochMilli())
                        .windowEnd(Instant.parse("2026-01-30T12:10:00.000Z").toEpochMilli())
                        .count(1)
                        .build()
        ));
    }

    @Disabled   // TODO remove when code is implemented
    @Test
    void shouldProduceTwoResultsForTwoItemsOnDifferentLinesInSameWindow() throws Exception {
        // given
        Instant eventTime = Instant.parse("2026-01-30T12:02:00.000Z");

        List<ProcessingEvent> events = List.of(
                ProcessingEvent.builder().line(0).station(1).unitId(1L).action(OUT).timestamp(eventTime).build(),
                ProcessingEvent.builder().line(1).station(1).unitId(2L).action(OUT).timestamp(eventTime).build()
        );

        // when
        runPipeline(events);

        // then
        List<Statistics> results = CollectSink.values;

        assertEquals(2, results.size());
        assertTrue(CollectSink.values.contains(
                Statistics.builder()
                        .line(0)
                        .windowStart(Instant.parse("2026-01-30T12:00:00.000Z").toEpochMilli())
                        .windowEnd(Instant.parse("2026-01-30T12:05:00.000Z").toEpochMilli())
                        .count(1)
                        .build()
        ));
        assertTrue(CollectSink.values.contains(
                Statistics.builder()
                        .line(1)
                        .windowStart(Instant.parse("2026-01-30T12:00:00.000Z").toEpochMilli())
                        .windowEnd(Instant.parse("2026-01-30T12:05:00.000Z").toEpochMilli())
                        .count(1)
                        .build()
        ));
    }

    private static void runPipeline(List<ProcessingEvent> events) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<ProcessingEvent> source = env.fromData(events);
        WindowingJob.getPipeline(source).addSink(new CollectSink());
        env.execute();
    }

    @SuppressWarnings("deprecation")
    static class CollectSink implements SinkFunction<Statistics> {

        static final List<Statistics> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(Statistics value, Context ctx) {
            values.add(value);
        }
    }
}