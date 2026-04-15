package com.xebia.flink.workshop.stateprocessorapi;

import com.xebia.flink.workshop.stateprocessorapi.model.ProcessingEvent;
import com.xebia.flink.workshop.stateprocessorapi.model.StationReport;
import com.xebia.flink.workshop.stateprocessorapi.model.StationStats;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

@Slf4j
public class ProcessingEventJobV2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<ProcessingEvent> source = KafkaSource.<ProcessingEvent>builder()
                .setBootstrapServers("rta-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
                .setTopics("processing-events")
                .setGroupId("processing-events-v2")
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new JsonDeserializationSchema<>(ProcessingEvent.class)))
                .build();

        DataStream<ProcessingEvent> events = env.fromSource(source, WatermarkStrategy.noWatermarks(), "processing-events");
        buildPipeline(events).print();

        env.execute("ProcessingEventJobV2");
    }

    public static DataStream<StationReport> buildPipeline(DataStream<ProcessingEvent> events) {
        return events
                .keyBy(e -> Tuple2.of(e.getLine(), e.getStation()), Types.TUPLE(Types.INT, Types.INT))
                .process(new StationDurationTracker())
                .uid("station-event-counter")
                .name("station-event-counter");
    }

    static class StationDurationTracker extends KeyedProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, StationReport> {

        static final ValueStateDescriptor<StationStats> stationStatsValueStateDescriptor = new ValueStateDescriptor<>("station-stats", TypeInformation.of(StationStats.class));
        static final MapStateDescriptor<Long, Long> inProgressStateDescriptor = new MapStateDescriptor<>("in-progress", Types.LONG, Types.LONG);
        private MapState<Long, Long> inProgress;
        private ValueState<StationStats> stationStats;

        @Override
        public void open(OpenContext ctx) {
            inProgress = getRuntimeContext().getMapState(inProgressStateDescriptor);
            stationStats = getRuntimeContext().getState(stationStatsValueStateDescriptor);
        }

        @Override
        public void processElement(ProcessingEvent event, Context ctx, Collector<StationReport> out)
                throws Exception {
            log.debug("Processing event {}", event);
            if (event.getAction() == ProcessingEvent.Action.IN) {
                inProgress.put(event.getUnitId(), event.getTimestamp().toEpochMilli());
            } else {
                Long arrivalMs = inProgress.get(event.getUnitId());
                if (arrivalMs == null) {
                    // IN event arrived before the migration — no arrival time available, skip
                    return;
                }

                long durationMs = event.getTimestamp().toEpochMilli() - arrivalMs;
                inProgress.remove(event.getUnitId());

                StationStats stats = stationStats.value();
                if (stats == null) {
                    stats = new StationStats(0L, Long.MAX_VALUE, Long.MIN_VALUE);
                }
                stats.setUnitCount(stats.getUnitCount() + 1);
                stats.setMinDurationMs(Math.min(stats.getMinDurationMs(), durationMs));
                stats.setMaxDurationMs(Math.max(stats.getMaxDurationMs(), durationMs));
                stationStats.update(stats);


                StationReport stationReport = new StationReport(event.getLine(), event.getStation(),
                        stats.getUnitCount(), stats.getMinDurationMs(), stats.getMaxDurationMs());
                log.debug("Emitting station report: {}", stationReport);
                out.collect(stationReport);
            }
        }
    }
}