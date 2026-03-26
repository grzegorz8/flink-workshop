package com.xebia.flink.workshop.stateprocessorapi;

import com.xebia.flink.workshop.stateprocessorapi.model.StationStats;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.StateBootstrapTransformation;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Offline state migration from V1 to V2.
 *
 * <p>Reads the V1 savepoint and produces a new V2 savepoint with evolved state:
 * <ul>
 *   <li>V1 state: {@code ValueState<Long>} "unit-count"</li>
 *   <li>V2 state: {@code ValueState<StationStats>} "station-stats" — unitCount is carried
 *       over from V1; duration fields are initialised to 0 and will accumulate from the point
 *       of migration onwards.</li>
 * </ul>
 *
 * <p>In-flight units present at migration time (i.e., units whose Action.IN has been seen but
 * Action.OUT has not yet arrived) are lost. V2 will simply ignore their Action.OUT events
 * because no arrival timestamp will be found in the empty "in-progress" map.
 *
 * <p>Usage:
 * <pre>
 *   java -cp ... ProcessingEventStateMigration /tmp/savepoints/v1 /tmp/savepoints/v2
 * </pre>
 */
public class ProcessingEventStateMigration {

    public static void main(String[] args) throws Exception {
        String sourceSavepointPath = args.length > 0 ? args[0] : "/tmp/savepoints/v1";
        String targetSavepointPath = args.length > 1 ? args[1] : "/tmp/savepoints/v2";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH); // It has to be BATCH.

        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend();
        OperatorIdentifier operatorId = OperatorIdentifier.forUid("station-event-counter");

        // Read V1 unit counts from the existing savepoint
        SavepointReader reader = SavepointReader.read(env, sourceSavepointPath, stateBackend);

        StateBootstrapTransformation<StationKeyedState> transformation = OperatorTransformation
                .bootstrapWith(reader.readKeyedState(
                        operatorId,
                        new V1StateReader(),
                        Types.TUPLE(Types.INT, Types.INT),
                        TypeInformation.of(StationKeyedState.class)
                ))
                .keyBy(ks -> ks.key, Types.TUPLE(Types.INT, Types.INT))
                .transform(new V2StateBootstrap());

        // Write a brand-new V2 savepoint — the "in-progress" map is intentionally left empty
        SavepointWriter.newSavepoint(env, stateBackend, 128)
                .withOperator(operatorId, transformation)
                .write(targetSavepointPath);
    }

    // -----------------------------------------------------------------------------------------
    // Transfer object
    // -----------------------------------------------------------------------------------------

    public static class StationKeyedState {
        public Tuple2<Integer, Integer> key;
        public long unitCount;
    }

    // -----------------------------------------------------------------------------------------
    // Reader: extracts V1 ValueState<Long> "unit-count"
    // -----------------------------------------------------------------------------------------

    static class V1StateReader extends KeyedStateReaderFunction<Tuple2<Integer, Integer>, StationKeyedState> {

        private ValueState<Long> unitCount;

        @Override
        public void open(OpenContext ctx) {
            unitCount = getRuntimeContext().getState(new ValueStateDescriptor<>("unit-count", Types.LONG));
        }

        @Override
        public void readKey(Tuple2<Integer, Integer> key, Context ctx, Collector<StationKeyedState> out) throws Exception {
            Long count = unitCount.value();
            if (count != null) {
                StationKeyedState ks = new StationKeyedState();
                ks.key = key;
                ks.unitCount = count;
                out.collect(ks);
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Bootstrap: writes V2 ValueState<StationStats> "station-stats"
    // -----------------------------------------------------------------------------------------

    static class V2StateBootstrap extends KeyedStateBootstrapFunction<Tuple2<Integer, Integer>, StationKeyedState> {

        private ValueState<StationStats> stationStats;

        @Override
        public void open(OpenContext ctx) {
            stationStats = getRuntimeContext().getState(new ValueStateDescriptor<>("station-stats", TypeInformation.of(StationStats.class)));
        }

        @Override
        public void processElement(StationKeyedState ks, Context ctx) throws Exception {
            // Carry over the V1 unit count; min/max will accumulate from the point of migration onwards
            stationStats.update(new StationStats(ks.unitCount, Long.MAX_VALUE, Long.MIN_VALUE));
        }
    }
}