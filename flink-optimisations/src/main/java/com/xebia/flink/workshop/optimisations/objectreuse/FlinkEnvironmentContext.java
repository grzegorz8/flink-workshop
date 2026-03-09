package com.xebia.flink.workshop.optimisations.objectreuse;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;

import static org.openjdk.jmh.annotations.Scope.Thread;

@State(Thread)
public class FlinkEnvironmentContext {

    public final StreamExecutionEnvironment env = getStreamExecutionEnvironment();

    protected final int parallelism = 1;
    protected final boolean objectReuse = true;

    @Setup
    public void setUp() throws IOException {
        // set up the execution environment
        env.setParallelism(parallelism);
        if (objectReuse) {
            env.getConfig().enableObjectReuse();
        }

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
        return StreamExecutionEnvironment.createLocalEnvironment(1, createConfiguration());
    }
}
