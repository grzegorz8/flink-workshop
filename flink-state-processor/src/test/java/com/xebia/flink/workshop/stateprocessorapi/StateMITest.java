package com.xebia.flink.workshop.stateprocessorapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.xebia.flink.workshop.stateprocessorapi.model.ProcessingEvent;
import com.xebia.flink.workshop.stateprocessorapi.model.StationCount;
import com.xebia.flink.workshop.stateprocessorapi.model.StationStats;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.table.SavepointTypeInformationFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class StateMITest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(WRITE_DATES_AS_TIMESTAMPS);

    private static MiniClusterWithClientResource flinkCluster;
    private static ClusterClient<?> clusterClient;

    @TempDir
    private Path savepointDir;
    @TempDir
    private Path savepointV2Dir;

    @BeforeAll
    static void startCluster() throws Exception {
        flinkCluster = new MiniClusterWithClientResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(2)
                        .setNumberTaskManagers(1)
                        .build());
        flinkCluster.before();
        clusterClient = flinkCluster.getClusterClient();
    }

    @AfterAll
    static void stopCluster() {
        flinkCluster.after();
    }

    @Test
    void should() throws Exception {
        // streaming environment
        // File containing test data
        File inputFile = Files.createTempFile("input-file", ".txt").toFile();
        inputFile.deleteOnExit();


        // PHASE 1 - Run Job V1
        writeEvents(inputFile, List.of(
                event(0, 0, ProcessingEvent.Action.IN),
                event(0, 0, ProcessingEvent.Action.OUT),   // -> "Station (0, 0) - processed units: 1"
                event(0, 1, ProcessingEvent.Action.IN),
                event(0, 1, ProcessingEvent.Action.OUT),   // -> "Station (0, 1) - processed units: 1"
                event(0, 0, ProcessingEvent.Action.IN),
                event(0, 0, ProcessingEvent.Action.OUT),   // -> "Station (0, 0) - processed units: 2"
                event(0, 1, ProcessingEvent.Action.IN),
                event(0, 1, ProcessingEvent.Action.OUT)    // -> "Station (0, 1) - processed units: 2"
        ));

        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.setParallelism(1);
        env1.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStream<ProcessingEvent> source = env1.fromSource(
                        FileSource.forRecordStreamFormat(new TextLineInputFormat(), org.apache.flink.core.fs.Path.fromLocalFile(inputFile))
                                .monitorContinuously(Duration.ofMillis(200))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "test-source"
                ).uid("test-source")
                .map(line -> MAPPER.readValue(line, ProcessingEvent.class)).uid("parsing");

        ProcessingEventJobV1
                .buildPipeline(source)
                .addSink(new CollectSinkV1()).uid("sink-v1");

        JobClient jobClientV1 = env1.executeAsync("ProcessingEventJobV1-test");
        await().atMost(Duration.ofSeconds(30)).until(() -> jobClientV1.getJobStatus().get() == JobStatus.RUNNING);
        await().atMost(Duration.ofSeconds(30)).until(() -> CollectSinkV1.values.size() >= 3);
        log.info("V1 results:   {}", CollectSinkV1.values);

        // PHASE 2 - Stop job V1 with savepoint
        String savepointPathV1 = jobClientV1
                .stopWithSavepoint(false, savepointDir.toString(), SavepointFormatType.CANONICAL)
                .get(30, TimeUnit.SECONDS);
        log.info("V1 savepoint: {}", savepointPathV1);
        //inspectSavepoint("V1", savepointPathV1);
        env1.close();

        // PHASE 3 - Migrate savepoint state using State Processor API
        String savepointPathV2 = savepointV2Dir.toString();
        log.info("Migrated savepoint: {}", savepointPathV2);
        ProcessingEventStateMigration.migrate(savepointPathV1, savepointPathV2);
        assertTrue(Files.exists(Path.of(savepointPathV2)), "Migrated savepoint directory should exist");
        //inspectSavepoint("V2", savepointPathV2);
    }

    private void writeEvents(File file, List<ProcessingEvent> events) throws IOException {
        try (FileWriter fileWriter = new FileWriter(file, true)) {
            for (ProcessingEvent event : events) {
                fileWriter.append(MAPPER.writeValueAsString(event)).append("\n");
            }
            fileWriter.flush();
        }
    }

    private static ProcessingEvent event(int line, int station, ProcessingEvent.Action action) {
        ProcessingEvent e = new ProcessingEvent();
        e.setTimestamp(Instant.now());
        e.setLine(line);
        e.setStation(station);
        e.setUnitId(1L);
        e.setAction(action);
        return e;
    }

    private static void inspectSavepoint(String label, String savepointPath) {
        log.info("=== Inspecting {} savepoint: {} ===", label, savepointPath);
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tableEnv.executeSql("LOAD MODULE state");

        TableResult metadataResult = tableEnv.executeSql(
                "SELECT * FROM savepoint_metadata('" + savepointPath + "')");
        log.info("{} savepoint metadata:", label);
        metadataResult.print();

        // Inspect keyed state for station-event-counter* operators
        if ("V1".equals(label)) {
            tableEnv.executeSql(
                    "CREATE TABLE station_event_counter (" +
                            "  k ROW<f0 INTEGER, f1 INTEGER>," +
                            "  `unit-count` BIGINT," +
                            "  PRIMARY KEY (k) NOT ENFORCED" +
                            ") WITH (" +
                            "  'connector' = 'savepoint'," +
                            "  'state.backend.type' = 'hashmap'," +
                            "  'state.path' = '" + savepointPath + "'," +
                            "  'operator.uid' = 'station-event-counter'," +
                            "  'fields.k.value-type-factory' = '" + Tuple2IntIntTypeFactory.class.getName() + "'" +
                            ")");
            log.info("{} keyed state (station-event-counter):", label);
            tableEnv.executeSql("SELECT * FROM station_event_counter").print();
        } else {
            tableEnv.executeSql(
                    "CREATE TABLE station_event_counter_v2 (" +
                            "  k ROW<f0 INTEGER, f1 INTEGER>," +
                            "  `station-stats` ROW<unitCount BIGINT, minDurationMs BIGINT, maxDurationMs BIGINT>," +
                            "  `in-progress` MAP<BIGINT, BIGINT>," +
                            "  PRIMARY KEY (k) NOT ENFORCED" +
                            ") WITH (" +
                            "  'connector' = 'savepoint'," +
                            "  'state.backend.type' = 'hashmap'," +
                            "  'state.path' = '" + savepointPath + "'," +
                            "  'operator.uid' = 'station-event-counter-v2'," +
                            "  'fields.k.value-type-factory' = '" + Tuple2IntIntTypeFactory.class.getName() + "'," +
                            "  'fields.station-stats.value-type-factory' = '" + StationStatsTypeFactory.class.getName() + "'" +
                            ")");
            log.info("{} keyed state (station-event-counter-v2):", label);
            tableEnv.executeSql("SELECT * FROM station_event_counter_v2").print();
        }
    }

    @SuppressWarnings("deprecation")
    static class CollectSinkV1 implements SinkFunction<StationCount> {

        static final List<StationCount> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(StationCount value, Context context) {
            values.add(value);
        }
    }

    public static class Tuple2IntIntTypeFactory implements SavepointTypeInformationFactory {
        @Override
        public TypeInformation<?> getTypeInformation() {
            return TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {
            });
        }
    }

    public static class StationStatsTypeFactory implements SavepointTypeInformationFactory {
        @Override
        public TypeInformation<?> getTypeInformation() {
            return TypeInformation.of(StationStats.class);
        }
    }
}
