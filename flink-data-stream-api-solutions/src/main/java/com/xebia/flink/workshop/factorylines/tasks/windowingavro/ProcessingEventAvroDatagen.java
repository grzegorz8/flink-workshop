package com.xebia.flink.workshop.factorylines.tasks.windowingavro;

import com.xebia.flink.workshop.factorylines.model.Action;
import com.xebia.flink.workshop.factorylines.model.ProcessingEventAvro;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

import java.time.Instant;

class ProcessingEventAvroDatagen {

    private static final int NUM_LINES = 2;
    private static final int NUM_STATIONS = 2;
    private static final String BOOTSTRAP_SERVERS = "localhost:9094";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8070";
    private static final String SINK_TOPIC = "processing-events-avro";

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        double recordsPerSecond = params.getDouble("records-per-second", 5);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataGeneratorSource<ProcessingEventAvro> source = new DataGeneratorSource<>(
                new ProcessingEventAvroGenerator(),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(recordsPerSecond),
                TypeInformation.of(ProcessingEventAvro.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "datagen")
                .sinkTo(
                        KafkaSink.<ProcessingEventAvro>builder()
                                .setBootstrapServers(BOOTSTRAP_SERVERS)
                                .setRecordSerializer(KafkaRecordSerializationSchema.<ProcessingEventAvro>builder()
                                        .setTopic(SINK_TOPIC)
                                        .setValueSerializationSchema(
                                                ConfluentRegistryAvroSerializationSchema.forSpecific(
                                                        ProcessingEventAvro.class,
                                                        SINK_TOPIC + "-value",
                                                        SCHEMA_REGISTRY_URL))
                                        .build())
                                .build()
                );

        env.execute("ProcessingEventAvroDatagen");
    }

    static class ProcessingEventAvroGenerator implements GeneratorFunction<Long, ProcessingEventAvro> {

        @Override
        public void open(SourceReaderContext context) {
        }

        @Override
        public ProcessingEventAvro map(Long index) {
            long unitId = index / 2;
            boolean isIn = index % 2 == 0;

            Instant timestamp = isIn
                    ? Instant.now()
                    : Instant.now().plusMillis((unitId % 5 + 1) * 100L);

            return ProcessingEventAvro.newBuilder()
                    .setUnitId(unitId)
                    .setLine((int) (unitId % NUM_LINES))
                    .setStation((int) (unitId % NUM_STATIONS))
                    .setAction(isIn ? Action.IN : Action.OUT)
                    .setTimestamp(timestamp)
                    .build();
        }
    }
}