package com.xebia.flink.workshop.stateprocessorapi;

import com.xebia.flink.workshop.stateprocessorapi.model.StationStats;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Reads and prints all keyed state from a V1 or V2 savepoint to stdout.
 *
 * <p>Usage:
 * <pre>
 *   java -cp ... PrintSavepointState &lt;savepoint-path&gt; &lt;v1|v2&gt;
 *
 *   # V1 — prints unit counts per station:
 *   java -cp ... PrintSavepointState /tmp/savepoints/v1 v1
 *
 *   # V2 — prints StationStats (unitCount, minDuration, maxDuration) and in-progress units:
 *   java -cp ... PrintSavepointState /tmp/savepoints/v2 v2
 * </pre>
 */
public class PrintSavepointState {

    private static final OperatorIdentifier OPERATOR_ID =
            OperatorIdentifier.forUid("station-event-counter");

    public static void main(String[] args) throws Exception {
        String savepointPath = args.length > 0 ? args[0] : "/tmp/savepoints/v1";
        String version       = args.length > 1 ? args[1] : "v1";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        SavepointReader reader = SavepointReader.read(env, savepointPath, new EmbeddedRocksDBStateBackend());

        if ("v2".equalsIgnoreCase(version)) {
            reader.readKeyedState(OPERATOR_ID, new V2StateReader(),
                            Types.TUPLE(Types.INT, Types.INT),
                            TypeInformation.of(String.class))
                    .print();
        } else {
            reader.readKeyedState(OPERATOR_ID, new V1StateReader(),
                            Types.TUPLE(Types.INT, Types.INT),
                            TypeInformation.of(String.class))
                    .print();
        }

        env.execute("PrintSavepointState (" + version + ")");
    }

    // -----------------------------------------------------------------------------------------
    // V1 reader — ValueState<Long> "unit-count"
    // -----------------------------------------------------------------------------------------

    static class V1StateReader extends KeyedStateReaderFunction<Tuple2<Integer, Integer>, String> {

        private ValueState<Long> unitCount;

        @Override
        public void open(OpenContext ctx) {
            unitCount = getRuntimeContext().getState(new ValueStateDescriptor<>("unit-count", Types.LONG));
        }

        @Override
        public void readKey(Tuple2<Integer, Integer> key, Context ctx, Collector<String> out)
                throws Exception {
            Long count = unitCount.value();
            if (count != null) {
                out.collect("station=(%d,%d)  unit-count=%d".formatted(key.f0, key.f1, count));
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // V2 reader — ValueState<StationStats> "station-stats" + MapState<Long, Long> "in-progress"
    // -----------------------------------------------------------------------------------------

    static class V2StateReader extends KeyedStateReaderFunction<Tuple2<Integer, Integer>, String> {

        private ValueState<StationStats> stationStats;
        private MapState<Long, Long> inProgress;

        @Override
        public void open(OpenContext ctx) {
            stationStats = getRuntimeContext().getState(new ValueStateDescriptor<>("station-stats", TypeInformation.of(StationStats.class)));
            inProgress = getRuntimeContext().getMapState(new MapStateDescriptor<>("in-progress", Types.LONG, Types.LONG));
        }

        @Override
        public void readKey(Tuple2<Integer, Integer> key, Context ctx, Collector<String> out) throws Exception {
            StationStats stats = stationStats.value();
            if (stats != null) {
                String minStr = stats.getMinDurationMs() == Long.MAX_VALUE ? "n/a" : stats.getMinDurationMs() + " ms";
                String maxStr = stats.getMaxDurationMs() == Long.MIN_VALUE ? "n/a" : stats.getMaxDurationMs() + " ms";
                out.collect("station=(%d,%d)  unit-count=%d  min=%s  max=%s"
                        .formatted(key.f0, key.f1, stats.getUnitCount(), minStr, maxStr));
            }

            int inProgressCount = 0;
            for (Map.Entry<Long, Long> entry : inProgress.entries()) {
                out.collect("station=(%d,%d)  in-progress  unit-id=%d  arrival-ms=%d"
                        .formatted(key.f0, key.f1, entry.getKey(), entry.getValue()));
                inProgressCount++;
            }
            if (inProgressCount > 0) {
                out.collect("station=(%d,%d)  in-progress-count=%d"
                        .formatted(key.f0, key.f1, inProgressCount));
            }
        }
    }
}