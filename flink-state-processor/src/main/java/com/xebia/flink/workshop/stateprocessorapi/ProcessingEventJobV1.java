package com.xebia.flink.workshop.stateprocessorapi;

import com.xebia.flink.workshop.stateprocessorapi.model.ProcessingEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import static java.lang.String.format;

public class ProcessingEventJobV1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<ProcessingEvent> source = KafkaSource.<ProcessingEvent>builder()
                .setBootstrapServers("rta-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
                .setTopics("processing-events")
                .setGroupId("processing-events-v1")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JsonDeserializationSchema<>(ProcessingEvent.class)))
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "processing-events")
                .keyBy(e -> Tuple2.of(e.getLine(), e.getStation()), Types.TUPLE(Types.INT, Types.INT))
                .process(new StationUnitCounter())
                .uid("station-event-counter")
                .name("station-event-counter")
                .print();

        env.execute("ProcessingEventJobV1");
    }

    static class StationUnitCounter extends KeyedProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, String> {

        private ValueState<Long> unitCount;

        @Override
        public void open(OpenContext ctx) {
            unitCount = getRuntimeContext().getState(new ValueStateDescriptor<>("unit-count", Types.LONG));
        }

        @Override
        public void processElement(ProcessingEvent event, Context ctx, Collector<String> out)
                throws Exception {
            if (event.getAction() == ProcessingEvent.Action.OUT) {
                Long count = unitCount.value();
                if (count == null) {
                    count = 0L;
                }
                unitCount.update(++count);
                out.collect(format("Station (%d, %d) — processed units: %d", event.getLine(), event.getStation(), count));
            }
        }
    }
}