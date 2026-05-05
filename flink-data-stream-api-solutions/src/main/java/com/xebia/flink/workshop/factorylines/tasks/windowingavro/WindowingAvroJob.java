package com.xebia.flink.workshop.factorylines.tasks.windowingavro;

import com.xebia.flink.workshop.factorylines.model.Action;
import com.xebia.flink.workshop.factorylines.model.ProcessingEventAvro;
import com.xebia.flink.workshop.factorylines.model.StatisticsAvro;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

class WindowingAvroJob {

    private static final int STATIONS_COUNT = 2;
    private static final String BOOTSTRAP_SERVERS = "localhost:9094";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8070";
    private static final String SOURCE_TOPIC = "processing-events-avro";
    private static final String SINK_TOPIC = "statistics-avro";
    private static final String GROUP_ID = "windowing-avro";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<ProcessingEventAvro> source = KafkaSource.<ProcessingEventAvro>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(SOURCE_TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(
                        ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                ProcessingEventAvro.class, SCHEMA_REGISTRY_URL)
                ))
                .build();

        KafkaSink<StatisticsAvro> sink = KafkaSink.<StatisticsAvro>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<StatisticsAvro>builder()
                        .setTopic(SINK_TOPIC)
                        .setValueSerializationSchema(
                                ConfluentRegistryAvroSerializationSchema.forSpecific(
                                        StatisticsAvro.class, SINK_TOPIC + "-value", SCHEMA_REGISTRY_URL))
                        .build())
                .build();

        DataStream<ProcessingEventAvro> events = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "processing-events-avro");

        getPipeline(events).sinkTo(sink);
        env.execute("WindowingAvroJob");
    }

    static DataStream<StatisticsAvro> getPipeline(DataStream<ProcessingEventAvro> sourceStream) {
        DataStream<ProcessingEventAvro> eventsWithWatermark = sourceStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ProcessingEventAvro>forBoundedOutOfOrderness(Duration.ofSeconds(10L))
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<ProcessingEventAvro>)
                                                (element, recordTimestamp) -> element.getTimestamp().toEpochMilli())
                );

        return eventsWithWatermark
                .filter(new FilterLastStationOutEvents())
                .keyBy(ProcessingEventAvro::getLine)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5L)))
                .aggregate(new CountEventsAggregateFunction(), new CountEventsWindowFunction());
    }

    static class FilterLastStationOutEvents implements FilterFunction<ProcessingEventAvro> {
        @Override
        public boolean filter(ProcessingEventAvro value) {
            return value.getStation() == STATIONS_COUNT - 1 && value.getAction() == Action.OUT;
        }
    }

    static class CountEventsAggregateFunction implements AggregateFunction<ProcessingEventAvro, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(ProcessingEventAvro value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    static class CountEventsWindowFunction implements WindowFunction<Integer, StatisticsAvro, Integer, TimeWindow> {
        @Override
        public void apply(Integer key, TimeWindow window, Iterable<Integer> input, Collector<StatisticsAvro> out) {
            int count = 0;
            for (Integer i : input) {
                count += i;
            }
            StatisticsAvro stats = new StatisticsAvro();
            stats.setLine(key);
            stats.setWindowStart(Instant.ofEpochMilli(window.getStart()));
            stats.setWindowEnd(Instant.ofEpochMilli(window.getEnd()));
            stats.setCount(count);
            out.collect(stats);
        }
    }
}