package com.xebia.flink.workshop.optimisations.reinterpret;

import com.xebia.flink.workshop.optimisations.reinterpret.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.xebia.flink.workshop.utils.RandomStringGenerator.generateRandomString;
import static org.apache.flink.streaming.api.datastream.DataStreamUtils.reinterpretAsKeyedStream;

@Fork(value = 2, jvmArgsAppend = {
        "-Djava.rmi.server.hostname=127.0.0.1",
        "-Dcom.sun.management.jmxremote.authenticate=false",
        "-Dcom.sun.management.jmxremote.ssl=false",
        "-Dcom.sun.management.jmxremote.ssl"}
)
@Warmup(iterations = 3)
@Measurement(iterations = 7)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class ReinterpretBenchmarks {

    private static final int RECORDS_PER_INVOCATION = 15_000_000;
    private static final Random RANDOM = new Random();


    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + ReinterpretBenchmarks.class.getCanonicalName() + ".*")
                .build();

        new Runner(options).run();
    }

    static class InputGenerator implements GeneratorFunction<Long, Event> {

        private Event template;

        @Override
        public void open(SourceReaderContext readerContext) {
            template = new Event();
            template.setId(0L);
            template.setLongValue1(200L);
            template.setLongValue2(300L);
            template.setStringValue1(generateRandomString(10));
            template.setStringValue2(generateRandomString(15));
            template.setStringValue3(generateRandomString(8));
            template.setBooleanValue1(RANDOM.nextBoolean());
            template.setBooleanValue2(RANDOM.nextBoolean());
            template.setNestedObjectList(
                    IntStream.range(0, RANDOM.nextInt(2, 3)).boxed()
                            .map(i -> {
                                Event.NestedObject nested = new Event.NestedObject();
                                nested.setStringValue1(generateRandomString(15));
                                nested.setStringValue2(generateRandomString(5));
                                nested.setLongValue1((long) i);
                                return nested;
                            })
                            .collect(Collectors.toList())
            );
        }

        @Override
        public Event map(Long value) {
            template.setId(value % 1000L);
            return template;
        }
    }

    static class PassThrough<T> implements MapFunction<T, T> {
        @Override
        public T map(T value) {
            return value;
        }
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void withoutReinterpretWithObjectReuseDisabled(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(3);
        env.getConfig().disableObjectReuse();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(Event.class);

        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new InputGenerator(),
                RECORDS_PER_INVOCATION,
                TypeInformation.of(Event.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "test source")
                .keyBy(Event::getId)
                .map(new PassThrough<Event>())
                .keyBy(Event::getId)
                .map(new PassThrough<Event>())
                .keyBy(Event::getId)
                .map(new PassThrough<Event>())
                .sinkTo(new DiscardingSink<>());
        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void withoutReinterpretWithObjectReuseEnabled(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(3);
        env.getConfig().enableObjectReuse();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(Event.class);

        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new InputGenerator(),
                RECORDS_PER_INVOCATION,
                TypeInformation.of(Event.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "test source")
                .keyBy(Event::getId)
                .map(new PassThrough<Event>())
                .keyBy(Event::getId)
                .map(new PassThrough<Event>())
                .keyBy(Event::getId)
                .map(new PassThrough<Event>())
                .sinkTo(new DiscardingSink<>());
        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void withReinterpretWithObjectReuseDisabled(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(3);
        env.getConfig().disableObjectReuse();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(Event.class);

        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new InputGenerator(),
                RECORDS_PER_INVOCATION,
                TypeInformation.of(Event.class)
        );

        SingleOutputStreamOperator<Event> stream1 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "test source")
                .keyBy(Event::getId)
                .map(new PassThrough<Event>());
        SingleOutputStreamOperator<Event> stream2 = reinterpretAsKeyedStream(stream1, Event::getId)
                .map(new PassThrough<Event>());
        reinterpretAsKeyedStream(stream2, Event::getId)
                .map(new PassThrough<Event>())
                .sinkTo(new DiscardingSink<>());
        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void withReinterpretWithObjectReuseEnabled(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(3);
        env.getConfig().enableObjectReuse();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(Event.class);

        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new InputGenerator(),
                RECORDS_PER_INVOCATION,
                TypeInformation.of(Event.class)
        );

        SingleOutputStreamOperator<Event> stream1 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "test source")
                .keyBy(Event::getId)
                .map(new PassThrough<Event>());
        SingleOutputStreamOperator<Event> stream2 = reinterpretAsKeyedStream(stream1, Event::getId)
                .map(new PassThrough<Event>());
        reinterpretAsKeyedStream(stream2, Event::getId)
                .map(new PassThrough<Event>())
                .sinkTo(new DiscardingSink<>());
        env.execute();
    }

}
