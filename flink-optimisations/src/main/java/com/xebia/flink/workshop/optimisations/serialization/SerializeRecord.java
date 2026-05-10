package com.xebia.flink.workshop.optimisations.serialization;


import com.xebia.flink.workshop.optimisations.serialization.model.EventRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

public class SerializeRecord {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(createConfiguration());
        env.setParallelism(2);
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(EventRecord.class);
        serializerConfig.registerPojoType(EventRecord.NestedObject.class);

        DataGeneratorSource<EventRecord> source = new DataGeneratorSource<>(
                new SerializationBenchmarks.RecordInputGenerator(),
                Integer.MAX_VALUE,
                TypeInformation.of(EventRecord.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "record-source")
                .rebalance()
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

    protected static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.GENERIC_TYPES, false);
        configuration.setString("rest.flamegraph.enabled", "true");
        configuration.setString("rest.profiling.enabled", "true");
        configuration.setString("rest.port", "8083");
        return configuration;
    }

}
