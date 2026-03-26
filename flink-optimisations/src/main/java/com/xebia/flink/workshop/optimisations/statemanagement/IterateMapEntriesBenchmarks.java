package com.xebia.flink.workshop.optimisations.statemanagement;

import com.xebia.flink.workshop.optimisations.statemanagement.model.Event;
import com.xebia.flink.workshop.optimisations.statemanagement.model.JoinedEvent;
import com.xebia.flink.workshop.optimisations.statemanagement.model.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import org.apache.flink.api.connector.source.SourceReaderContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(value = 1, jvmArgsAppend = {
        "-Djava.rmi.server.hostname=127.0.0.1",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.ssl"}
)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class IterateMapEntriesBenchmarks {

    private static final Random RANDOM = new Random();

    private static final int NUM_LINES = 3;
    private static final int NUM_STATIONS = 3;
    private static final long BASE_TIMESTAMP = Instant.parse("2026-01-01T00:00:00.000Z").toEpochMilli();
    private static final int SIMULATION_LENGTH_SECONDS = 20;
    private static final int EVENTS_PER_SECOND = 5_000;
    private static final int SENSOR_READINGS_COUNT = NUM_LINES * NUM_STATIONS * 50;

    private static final int RECORDS_PER_INVOCATION = EVENTS_PER_SECOND * SIMULATION_LENGTH_SECONDS;


    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + IterateMapEntriesBenchmarks.class.getCanonicalName() + ".*")
                .build();
        new Runner(options).run();
    }

    private void buildPipeline(StreamExecutionEnvironment env, KeyedCoProcessFunction<Tuple2<Integer, Integer>, Event, SensorReading, JoinedEvent> processFunction) {
        WatermarkStrategy<Event> eventWatermarks = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, recordTimestamp) -> event.getTimestamp());

        WatermarkStrategy<SensorReading> readingWatermarks = WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((reading, recordTimestamp) -> Long.MAX_VALUE);

        KeyedStream<Event, Tuple2<Integer, Integer>> eventStream = env
                .fromSource(
                        new DataGeneratorSource<>(new EventGenerator(), SIMULATION_LENGTH_SECONDS * EVENTS_PER_SECOND, RateLimiterStrategy.noOp(), TypeInformation.of(Event.class)),
                        eventWatermarks, "Events")
                .keyBy(event -> Tuple2.of(event.getLineId(), event.getStationId()), Types.TUPLE(Types.INT, Types.INT));

        KeyedStream<SensorReading, Tuple2<Integer, Integer>> readingStream = env
                .fromSource(
                        new DataGeneratorSource<>(new SensorReadingGenerator(), SENSOR_READINGS_COUNT, RateLimiterStrategy.noOp(), TypeInformation.of(SensorReading.class)),
                        readingWatermarks, "SensorReadings")
                .keyBy(event -> Tuple2.of(event.getLineId(), event.getStationId()), Types.TUPLE(Types.INT, Types.INT));

        eventStream.connect(readingStream)
                .process(processFunction)
                .sinkTo(new DiscardingSink<>());
    }

    static class EventGenerator implements GeneratorFunction<Long, Event> {
        private Event template;

        @Override
        public void open(SourceReaderContext context) {
            template = new Event(0, 0, 0L, "event", "unit-type-0", "operator-0", 1.0, 0);
        }

        @Override
        public Event map(Long value) {
            template.setLineId((int) (value % NUM_LINES));
            template.setStationId((int) (value % NUM_STATIONS));
            template.setTimestamp(BASE_TIMESTAMP + (long) (((double) value * 1000.0d) / ((double) EVENTS_PER_SECOND)));
            return template;
        }
    }

    static class SensorReadingGenerator implements GeneratorFunction<Long, SensorReading> {
        private SensorReading template;

        @Override
        public void open(SourceReaderContext context) {
            template = new SensorReading(
                    0, 0, 0L,
                    25.0, 120.0, 1.2, 55.0, "sensor-0",
                    0.5, 220.0, 10.0, 1500.0, 50.0, 5.0,
                    400.0, 70.0, 300.0, 7.0, 0.1, 1.0,
                    500.0, 80.0, 60.0, 350.0, 0.7, 0.85,
                    0, 0, false
            );
        }

        @Override
        public SensorReading map(Long value) {
            template.setLineId((int) (value % NUM_LINES));
            template.setStationId((int) (value % NUM_STATIONS));
            template.setTimestamp(BASE_TIMESTAMP + RANDOM.nextInt(SIMULATION_LENGTH_SECONDS * 1000));
            return template;
        }
    }

    @State(Scope.Thread)
    public static class RocksDBStateContext {
        public StreamExecutionEnvironment env;

        @Setup
        public void setUp() {
            Configuration config = new Configuration();
            config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
            env = StreamExecutionEnvironment.createLocalEnvironment(2, config);
        }
    }

    @State(Scope.Thread)
    public static class HashmapStateContext {
        public StreamExecutionEnvironment env;

        @Setup
        public void setUp() {
            Configuration config = new Configuration();
            config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
            env = StreamExecutionEnvironment.createLocalEnvironment(2, config);
        }
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS_PER_INVOCATION)
    public void mapEntriesIterationRememberKeyHashmap(HashmapStateContext context) throws Exception {
        buildPipeline(context.env, new IterateEntriesRememberKeyJoin());
        context.env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS_PER_INVOCATION)
    public void mapEntriesIterationRememberValueHashmap(HashmapStateContext context) throws Exception {
        buildPipeline(context.env, new IterateEntriesRememberValueJoin());
        context.env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS_PER_INVOCATION)
    public void mapEntriesIterationRememberKeyRocksDB(RocksDBStateContext context) throws Exception {
        buildPipeline(context.env, new IterateEntriesRememberKeyJoin());
        context.env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS_PER_INVOCATION)
    public void mapEntriesIterationRememberValueRocksDB(RocksDBStateContext context) throws Exception {
        buildPipeline(context.env, new IterateEntriesRememberValueJoin());
        context.env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS_PER_INVOCATION)
    public void mapEntriesIterationRememberKeyRocksDBSorted(RocksDBStateContext context) throws Exception {
        buildPipeline(context.env, new IterateEntriesSortedRememberKeyJoin());
        context.env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS_PER_INVOCATION)
    public void mapEntriesIterationRememberValueRocksDBSorted(RocksDBStateContext context) throws Exception {
        buildPipeline(context.env, new IterateEntriesSortedRememberValueJoin());
        context.env.execute();
    }

}