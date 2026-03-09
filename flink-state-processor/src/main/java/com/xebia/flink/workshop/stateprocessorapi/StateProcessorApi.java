package com.xebia.flink.workshop.stateprocessorapi;

import com.xebia.flink.workshop.model.Event;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
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

import java.io.IOException;

public class StateProcessorApi {

    public static void main(String[] args) throws IOException {
        String sourceSavepointPath = "";
        String targetSavepointPath = "";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        SavepointReader reader = SavepointReader.read(env, sourceSavepointPath, new EmbeddedRocksDBStateBackend());
        SavepointWriter writer = SavepointWriter.fromExistingSavepoint(env, sourceSavepointPath, new EmbeddedRocksDBStateBackend());

        OperatorIdentifier myProcessFunId = OperatorIdentifier.forUid("my-process-function");
        StateBootstrapTransformation<KeyedState> transformation = OperatorTransformation
                .bootstrapWith(reader.readKeyedState(myProcessFunId, new MyProcessFunctionStateReader(), Types.LONG, TypeInformation.of(KeyedState.class)))
                .keyBy(ks -> ks.key)
                .transform(new MyProcessFunctionBootstrap());

        writer.withOperator(myProcessFunId, transformation)
                .write(targetSavepointPath);
    }

    public static class KeyedState {
        public long key;
        public Event event;
    }

    public static class MyProcessFunctionStateReader extends KeyedStateReaderFunction<Long, KeyedState> {

        private ValueState<Event> keyedState1;

        @Override
        public void open(OpenContext openContext) throws Exception {
            keyedState1 = getRuntimeContext().getState(new ValueStateDescriptor<>("keyed-state-1", TypeInformation.of(Event.class)));
        }

        @Override
        public void readKey(Long key, Context context, Collector<KeyedState> collector) throws Exception {
            KeyedState keyedState = new KeyedState();
            keyedState.key = key;
            keyedState.event = keyedState1.value();
            collector.collect(keyedState);
        }
    }

    public static class MyProcessFunctionBootstrap extends KeyedStateBootstrapFunction<Long, KeyedState> {

        private ValueState<Event> keyedState1;

        @Override
        public void open(OpenContext openContext) throws Exception {
            keyedState1 = getRuntimeContext().getState(new ValueStateDescriptor<>("keyed-state-1", TypeInformation.of(Event.class)));
        }

        @Override
        public void processElement(KeyedState keyedState, KeyedStateBootstrapFunction<Long, KeyedState>.Context context) throws Exception {

        }
    }

}
