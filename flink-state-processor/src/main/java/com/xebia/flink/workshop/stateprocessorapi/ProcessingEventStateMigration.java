package com.xebia.flink.workshop.stateprocessorapi;

import com.xebia.flink.workshop.stateprocessorapi.model.StationStats;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.SavepointReader;
import org.apache.flink.state.api.SavepointWriter;
import org.apache.flink.state.api.StateBootstrapTransformation;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import static com.xebia.flink.workshop.stateprocessorapi.ProcessingEventJobV1.StationUnitCounter.unitCountStateDescriptor;
import static com.xebia.flink.workshop.stateprocessorapi.ProcessingEventJobV2.StationDurationTracker.stationStatsValueStateDescriptor;

/**
 * Offline state migration from V1 to V2. Reads the V1 savepoint and produces a new V2 savepoint with evolved state.
 */
public class ProcessingEventStateMigration {

    public static void main(String[] args) throws Exception {
        String sourceSavepointPath = args.length > 0 ? args[0] : "/tmp/savepoints/v1/";
        String targetSavepointPath = args.length > 1 ? args[1] : "/tmp/savepoints/v2/";
        migrate(sourceSavepointPath, targetSavepointPath);
    }

    public static void migrate(String sourceSavepointPath, String targetSavepointPath) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH); // It has to be BATCH.

        OperatorIdentifier operatorId = OperatorIdentifier.forUid("station-event-counter");
        OperatorIdentifier operatorIdV2 = OperatorIdentifier.forUid("station-event-counter-v2");

        // Read V1 unit counts from the existing savepoint
        SavepointReader reader = SavepointReader.read(env, sourceSavepointPath);

        StateBootstrapTransformation<StationKeyedState> transformation = OperatorTransformation
                .bootstrapWith(reader.readKeyedState(operatorId, new V1StateReader()))
                .keyBy(new KeySelector<StationKeyedState, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(StationKeyedState ks) {
                        return ks.key;
                    }
                }, Types.TUPLE(Types.INT, Types.INT))
                .transform(new V2StateBootstrap());

        SavepointWriter.fromExistingSavepoint(env, sourceSavepointPath, new HashMapStateBackend())
                .withOperator(operatorIdV2, transformation)
                .removeOperator(operatorId)
                .write(targetSavepointPath);
//        SavepointWriter.newSavepoint(env, 120)
//                .withOperator(operatorId, transformation)
//                .write(targetSavepointPath);

        env.execute();
    }

    public static class StationKeyedState {
        public Tuple2<Integer, Integer> key;
        public long unitCount;
    }

    static class V1StateReader extends KeyedStateReaderFunction<Tuple2<Integer, Integer>, StationKeyedState> {

        private ValueState<Long> unitCount;

        @Override
        public void open(Configuration configuration) {
            unitCount = getRuntimeContext().getState(unitCountStateDescriptor);
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

    static class V2StateBootstrap extends KeyedStateBootstrapFunction<Tuple2<Integer, Integer>, StationKeyedState> {

        private ValueState<StationStats> stationStats;

        @Override
        public void open(OpenContext ctx) {
            stationStats = getRuntimeContext().getState(stationStatsValueStateDescriptor);
        }

        @Override
        public void processElement(StationKeyedState ks, Context ctx) throws Exception {
            // Carry over the V1 unit count; min/max will accumulate from the point of migration onwards
            stationStats.update(new StationStats(ks.unitCount, Long.MAX_VALUE, Long.MIN_VALUE));
        }
    }
}