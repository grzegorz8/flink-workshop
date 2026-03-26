package com.xebia.flink.workshop.autoscaler;

import com.xebia.flink.workshop.model.Event;
import com.xebia.flink.workshop.serde.JsonSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

import java.time.Instant;

import static com.xebia.flink.workshop.utils.RandomStringGenerator.generateRandomString;


public class EventsGenerator {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new InputGenerator(),
                Integer.MAX_VALUE,
                RateLimiterStrategy.perSecond(parameters.getDouble("records-per-second")),
                TypeInformation.of(Event.class)
        );

        env
                .fromSource(
                        source,
                        WatermarkStrategy.forMonotonousTimestamps(),
                        "Input"
                )
                .sinkTo(
                        KafkaSink.<Event>builder()
                                .setBootstrapServers("rta-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
                                .setRecordSerializer(KafkaRecordSerializationSchema.<Event>builder()
                                        .setValueSerializationSchema(new JsonSerializationSchema<>())
                                        .setTopic("events")
                                        .build())
                                .build()
                );

        env.execute("Test Input Generator");

    }

    static class InputGenerator implements GeneratorFunction<Long, Event> {

        @Override
        public Event map(Long value) {

            return new Event(
                    value,
                    Instant.now(),
                    "value" + value,
                    generateRandomString(8),
                    generateRandomString(12),
                    generateRandomString(2),
                    generateRandomString(20),
                    generateRandomString(10)
            );
        }
    }
}