package com.xebia.flink.workshop.stateprocessorapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.xebia.flink.workshop.stateprocessorapi.model.ProcessingEvent;
import com.xebia.flink.workshop.stateprocessorapi.model.StationCount;
import com.xebia.flink.workshop.stateprocessorapi.model.StationReport;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class StateMigrationIntegrationTest {

    private static final String TOPIC = "processing-events";
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule()).disable(WRITE_DATES_AS_TIMESTAMPS);

    @TempDir
    private Path tempDir;

    @BeforeAll
    static void startKafka() throws Exception {
        KAFKA_CONTAINER.start();
        createTopic(TOPIC);
    }

    private static void createTopic(String topicName) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
    }

    @AfterAll
    static void stopKafka() {
        KAFKA_CONTAINER.stop();
    }

    @Test
    void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // --- PHASE 1 - Run ProcessingEventJobV1
        sendMessages(List.of(
                event(0, 0, ProcessingEvent.Action.IN),
                event(0, 0, ProcessingEvent.Action.OUT),   // → "Station (0, 0) — processed units: 1"
                event(0, 1, ProcessingEvent.Action.IN),
                event(0, 1, ProcessingEvent.Action.OUT),   // → "Station (0, 1) — processed units: 1"
                event(0, 0, ProcessingEvent.Action.IN),
                event(0, 0, ProcessingEvent.Action.OUT),   // → "Station (0, 0) — processed units: 2"
                event(0, 1, ProcessingEvent.Action.IN),
                event(0, 1, ProcessingEvent.Action.OUT)    // → "Station (0, 1) — processed units: 2"
        ));

        DataStream<ProcessingEvent> sourceV1 = env.fromSource(buildKafkaSource("test", OffsetsInitializer.earliest()), WatermarkStrategy.noWatermarks(), "source");
        ProcessingEventJobV1
                .buildPipeline(sourceV1)
                .addSink(new CollectSinkV1());

        JobClient jobClientV1 = env.executeAsync("ProcessingEventJobV1-test");
        await().atMost(Duration.ofSeconds(30)).until(() -> jobClientV1.getJobStatus().get() == JobStatus.RUNNING);
        await().atMost(Duration.ofSeconds(30)).until(() -> CollectSinkV1.values.size() >= 2);

        String savepointPathV1 = jobClientV1
                .stopWithSavepoint(false, tempDir.toString(), SavepointFormatType.CANONICAL)
                .get(30, TimeUnit.SECONDS);
        log.info("V1 savepoint: {}", savepointPathV1);
        log.info("V1 results:   {}", CollectSinkV1.values);

        // -- PHASE 2 - Migrate savepoint state using State Processor API
        String savepointPathV2 = tempDir.resolve("savepoint-v2").toString();
        ProcessingEventStateMigration.migrate(savepointPathV1, savepointPathV2);
        assertTrue(Files.exists(Path.of(savepointPathV2)), "Migrated savepoint directory should exist");
        log.info("Migrated savepoint: {}", savepointPathV2);

        // -- PHASE 3 - Start JobV2 from migrated savepoint, push new messages, await results
        Configuration configV2 = new Configuration();
        configV2.set(StateRecoveryOptions.SAVEPOINT_PATH, savepointPathV2);
        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment(configV2);
        env2.setParallelism(1);

        DataStream<ProcessingEvent> sourceV2 = env2.fromSource(buildKafkaSource("test", OffsetsInitializer.latest()), WatermarkStrategy.noWatermarks(), "source");
        ProcessingEventJobV2
                .buildPipeline(sourceV2)
                .addSink(new CollectSinkV2());

        JobClient jobClientV2 = env2.executeAsync("ProcessingEventJobV2-test");
        await().atMost(Duration.ofSeconds(30)).until(() -> jobClientV2.getJobStatus().get() == JobStatus.RUNNING);

        sendMessages(List.of(
                event(0, 0, ProcessingEvent.Action.IN),
                event(0, 0, ProcessingEvent.Action.OUT),   // → StationReport(line=0, station=0, unitCount=3, ...) (count carried over from V1)
                event(0, 1, ProcessingEvent.Action.IN),
                event(0, 1, ProcessingEvent.Action.OUT),   // → StationReport(line=0, station=1, unitCount=3, ...) (count carried over from V1)
                event(0, 0, ProcessingEvent.Action.IN),
                event(0, 0, ProcessingEvent.Action.OUT),   // → StationReport(line=0, station=0, unitCount=4, ...) (count carried over from V1)
                event(0, 1, ProcessingEvent.Action.IN)
        ));

        await().atMost(Duration.ofSeconds(30)).until(() -> CollectSinkV2.values.size() >= 3);
        jobClientV2.cancel().get(30, TimeUnit.SECONDS);

        log.info("V2 results: {}", CollectSinkV2.values);

        long maxUnitCount00 = CollectSinkV2.values.stream()
                .filter(r -> r.getLine() == 0 && r.getStation() == 0)
                .mapToLong(StationReport::getUnitCount)
                .max().orElseThrow();
        long maxUnitCount01 = CollectSinkV2.values.stream()
                .filter(r -> r.getLine() == 0 && r.getStation() == 1)
                .mapToLong(StationReport::getUnitCount)
                .max().orElseThrow();

        assertEquals(4, maxUnitCount00, "Station (0,0) max unitCount should be 4");
        assertEquals(3, maxUnitCount01, "Station (0,1) max unitCount should be 3");
    }

    private KafkaSource<ProcessingEvent> buildKafkaSource(String groupId, OffsetsInitializer startingOffsets) {
        return KafkaSource.<ProcessingEvent>builder()
                .setBootstrapServers(KAFKA_CONTAINER.getBootstrapServers())
                .setTopics(TOPIC)
                .setGroupId(groupId)
                .setStartingOffsets(startingOffsets)
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(
                        new ProcessingEventDeserializationSchema()))
                .build();
    }

    private void sendMessages(List<ProcessingEvent> events) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ))) {
            for (ProcessingEvent event : events) {
                producer.send(new ProducerRecord<>(TOPIC, MAPPER.writeValueAsString(event))).get();
            }
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

    static class ProcessingEventDeserializationSchema implements DeserializationSchema<ProcessingEvent> {

        private static final ObjectMapper MAPPER = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(WRITE_DATES_AS_TIMESTAMPS);

        @Override
        public ProcessingEvent deserialize(byte[] message) throws IOException {
            return MAPPER.readValue(message, ProcessingEvent.class);
        }

        @Override
        public boolean isEndOfStream(ProcessingEvent nextElement) {
            return false;
        }

        @Override
        public TypeInformation<ProcessingEvent> getProducedType() {
            return TypeInformation.of(ProcessingEvent.class);
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

    @SuppressWarnings("deprecation")
    static class CollectSinkV2 implements SinkFunction<StationReport> {

        static final List<StationReport> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(StationReport value, Context context) {
            values.add(value);
        }
    }
}