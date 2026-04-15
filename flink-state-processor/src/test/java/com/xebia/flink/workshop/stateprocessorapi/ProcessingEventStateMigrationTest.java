package com.xebia.flink.workshop.stateprocessorapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.xebia.flink.workshop.stateprocessorapi.model.ProcessingEvent;
import com.xebia.flink.workshop.stateprocessorapi.model.StationCount;
import com.xebia.flink.workshop.stateprocessorapi.model.StationReport;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
public class ProcessingEventStateMigrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(WRITE_DATES_AS_TIMESTAMPS);

    private static MiniClusterWithClientResource flinkCluster;

    private final SavepointInspector savepointInspector = new SavepointInspector(
            "test-source", "parsing", "station-event-counter", "station-event-counter-v2", "sink");

    @TempDir
    private Path inputDir;
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
                        .build()
        );
        flinkCluster.before();
    }

    @AfterAll
    static void stopCluster() {
        flinkCluster.after();
    }

    @Test
    void shouldStartJobV2FromMigratedSavepoint() throws Exception {
        // PHASE 1 - Run Job V1
        JobClient jobClientV1 = phaseOne_StartJobV1();
        // PHASE 2 - Stop job V1 with savepoint
        String savepointPathV1 = phaseTwo_StopJobV1WithSavepoint(jobClientV1);
        // PHASE 3 - Migrate savepoint state using State Processor API
        String savepointPathV2 = phaseThree_MigrateSavepoint(savepointPathV1);
        // PHASE 4 - Start JobV2 from migrated savepoint, push new messages, await results
        phaseFour_StartJobV2FromMigratedSavepoint(savepointPathV2);
    }

    private JobClient phaseOne_StartJobV1() throws Exception {
        File inputFile1 = inputDir.resolve("input-file-1.txt").toFile();
        writeEvents(inputFile1, List.of(
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

        DataStream<ProcessingEvent> source = env1.fromSource(
                        FileSource.forRecordStreamFormat(new TextLineInputFormat(), org.apache.flink.core.fs.Path.fromLocalFile(inputDir.toFile()))
                                .monitorContinuously(Duration.ofMillis(200))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "test-source"
                ).uid("test-source")
                .map(line -> MAPPER.readValue(line, ProcessingEvent.class)).uid("parsing");

        ProcessingEventJobV1
                .buildPipeline(source)
                .addSink(new CollectSinkV1()).uid("sink");

        JobClient jobClientV1 = env1.executeAsync("ProcessingEventJobV1-test");
        await().atMost(Duration.ofSeconds(30)).until(() -> jobClientV1.getJobStatus().get() == JobStatus.RUNNING);
        await().atMost(Duration.ofSeconds(30)).until(() -> CollectSinkV1.values.size() >= 3);
        log.info("V1 results:   {}", CollectSinkV1.values);
        return jobClientV1;
    }

    private String phaseTwo_StopJobV1WithSavepoint(JobClient jobClientV1) throws Exception {
        String savepointPathV1 = jobClientV1
                .stopWithSavepoint(false, savepointDir.toString(), SavepointFormatType.CANONICAL)
                .get(30, TimeUnit.SECONDS);
        log.info("V1 savepoint: {}", savepointPathV1);
        savepointInspector.inspect("V1", savepointPathV1);
        return savepointPathV1;
    }

    private String phaseThree_MigrateSavepoint(String savepointPathV1) throws Exception {
        String savepointPathV2 = savepointV2Dir.toString();
        log.info("Migrated savepoint: {}", savepointPathV2);
        ProcessingEventStateMigration.migrate(savepointPathV1, savepointPathV2);
        assertTrue(Files.exists(Path.of(savepointPathV2)), "Migrated savepoint directory should exist");
        savepointInspector.inspect("V2", savepointPathV2);
        return savepointPathV2;
    }

    private void phaseFour_StartJobV2FromMigratedSavepoint(String savepointPathV2) throws Exception {
        Configuration configV2 = new Configuration();
        configV2.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPathV2);
        configV2.set(StateRecoveryOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment(configV2);
        env2.setParallelism(1);

        DataStream<ProcessingEvent> sourceV2 = env2.fromSource(
                        FileSource.forRecordStreamFormat(new TextLineInputFormat(), org.apache.flink.core.fs.Path.fromLocalFile(inputDir.toFile()))
                                .monitorContinuously(Duration.ofMillis(200))
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "test-source"
                ).uid("test-source")
                .map(line -> MAPPER.readValue(line, ProcessingEvent.class)).uid("parsing");

        ProcessingEventJobV2
                .buildPipeline(sourceV2)
                .addSink(new CollectSinkV2()).uid("sink");

        JobClient jobClient = env2.executeAsync("ProcessingEventJobV2-test");
        await().atMost(Duration.ofSeconds(30)).until(() -> jobClient.getJobStatus().get(30, TimeUnit.SECONDS).equals(JobStatus.RUNNING));

        File inputFile2 = inputDir.resolve("input-file-2.txt").toFile();
        writeEvents(inputFile2, List.of(
                event(0, 0, ProcessingEvent.Action.IN),
                event(0, 0, ProcessingEvent.Action.OUT),   // -> StationReport(line=0, station=0, unitCount=3, ...) (count carried over from V1)
                event(0, 1, ProcessingEvent.Action.IN),
                event(0, 1, ProcessingEvent.Action.OUT),   // -> StationReport(line=0, station=1, unitCount=3, ...) (count carried over from V1)
                event(0, 0, ProcessingEvent.Action.IN),
                event(0, 0, ProcessingEvent.Action.OUT),   // -> StationReport(line=0, station=0, unitCount=4, ...) (count carried over from V1)
                event(0, 1, ProcessingEvent.Action.IN)
        ));

        await().atMost(Duration.ofSeconds(30)).until(() -> CollectSinkV2.values.size() >= 3);
        jobClient.cancel().get(30, TimeUnit.SECONDS);

        log.info("V2 results: {}", CollectSinkV2.values);
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


    @SuppressWarnings("deprecation")
    static class CollectSinkV1 implements SinkFunction<StationCount> {

        static final List<StationCount> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(StationCount value, Context context) {
            values.add(value);
        }
    }

    @SuppressWarnings("deprecation")
    static class CollectSinkV2 implements SinkFunction<StationReport> {

        static final List<StationReport> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(StationReport value, Context context) {
            values.add(value);
        }
    }

}
