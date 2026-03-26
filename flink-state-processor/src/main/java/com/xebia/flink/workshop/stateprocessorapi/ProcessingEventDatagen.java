package com.xebia.flink.workshop.stateprocessorapi;

import com.xebia.flink.workshop.serde.JsonSerializationSchema;
import com.xebia.flink.workshop.stateprocessorapi.model.ProcessingEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

import java.time.Instant;

public class ProcessingEventDatagen {

    private static final int NUM_LINES = 3;
    private static final int NUM_STATIONS = 4;

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        double recordsPerSecond = params.getDouble("records-per-second", 5);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataGeneratorSource<ProcessingEvent> source = new DataGeneratorSource<>(
                new ProcessingEventGenerator(),
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(recordsPerSecond),
                TypeInformation.of(ProcessingEvent.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "datagen")
                .sinkTo(
                        KafkaSink.<ProcessingEvent>builder()
                                .setBootstrapServers("rta-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
                                .setRecordSerializer(KafkaRecordSerializationSchema.<ProcessingEvent>builder()
                                        .setValueSerializationSchema(new JsonSerializationSchema<>())
                                        .setTopic("processing-events")
                                        .build())
                                .build()
                );

        env.execute("ProcessingEventDatagen");
    }

    static class ProcessingEventGenerator implements GeneratorFunction<Long, ProcessingEvent> {

        private ProcessingEvent template;

        @Override
        public void open(SourceReaderContext context) {
            template = new ProcessingEvent();
        }

        @Override
        public ProcessingEvent map(Long index) {
            long unitId = index / 2;
            boolean isIn = index % 2 == 0;

            template.setUnitId(unitId);
            template.setLine((int) (unitId % NUM_LINES));
            template.setStation((int) (unitId % NUM_STATIONS));
            template.setAction(isIn ? ProcessingEvent.Action.IN : ProcessingEvent.Action.OUT);
            // OUT timestamp is IN + 100–500 ms depending on unitId
            template.setTimestamp(
                    isIn
                            ? Instant.now()
                            : Instant.now().plusMillis((unitId % 5 + 1) * 100L)
            );

            return template;
        }
    }
}