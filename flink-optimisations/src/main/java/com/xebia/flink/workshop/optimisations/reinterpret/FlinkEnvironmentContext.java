package com.xebia.flink.workshop.optimisations.reinterpret;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
public class FlinkEnvironmentContext {

    public final StreamExecutionEnvironment env = getStreamExecutionEnvironment();

    @Setup
    public void setUp() {
        env.setParallelism(3);
    }

    public void execute() throws Exception {
        env.execute();
    }

    protected Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.GENERIC_TYPES, false);
        return configuration;
    }

    private StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return StreamExecutionEnvironment.createLocalEnvironment(createConfiguration());
    }
}
