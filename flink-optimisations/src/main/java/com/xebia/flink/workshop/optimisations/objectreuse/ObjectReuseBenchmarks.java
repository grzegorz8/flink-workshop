package com.xebia.flink.workshop.optimisations.objectreuse;

import com.xebia.flink.workshop.optimisations.objectreuse.model.Event;
import com.xebia.flink.workshop.optimisations.objectreuse.model.Statistics;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.xebia.flink.workshop.utils.RandomStringGenerator.generateRandomString;
import static java.util.stream.Collectors.toList;

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
@State(Scope.Thread)
public class ObjectReuseBenchmarks {

    private static final int RECORDS_PER_INVOCATION = 2_000_000;
    private static final Random RANDOM = new Random();

    private List<Event> pojos;

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + ObjectReuseBenchmarks.class.getCanonicalName() + ".*")
                .build();

        new Runner(options).run();
    }

    @Setup
    public void setup() {
        this.pojos = IntStream.range(0, RECORDS_PER_INVOCATION).boxed().map(this::createEvent).collect(toList());
    }

    private Event createEvent(long index) {
        Event pojo = new Event();
        pojo.setId(index);
        pojo.setLongValue1(index * 2);
        pojo.setLongValue2(index * 3);
        pojo.setStringValue1(generateRandomString(10));
        pojo.setStringValue2(generateRandomString(15));
        pojo.setStringValue3(generateRandomString(8));
        pojo.setBooleanValue1(RANDOM.nextBoolean());
        pojo.setBooleanValue2(RANDOM.nextBoolean());
        pojo.setNestedObjectList(
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
        return pojo;
    }

    static class PassThrough<T> implements MapFunction<T, T> {
        @Override
        public T map(T value) {
            return value;
        }
    }

    static void countLettersAndDigits(Event event, Statistics result) {
        int letterCount = 0;
        int digitCount = 0;

        for (int i = 0; i < event.getStringValue1().length(); i++) {
            char ch = event.getStringValue1().charAt(i);

            if (Character.isLetter(ch)) {
                letterCount++;
            } else if (Character.isDigit(ch)) {
                digitCount++;
            }
        }
        for (int i = 0; i < event.getStringValue2().length(); i++) {
            char ch = event.getStringValue2().charAt(i);

            if (Character.isLetter(ch)) {
                letterCount++;
            } else if (Character.isDigit(ch)) {
                digitCount++;
            }
        }
        for (int i = 0; i < event.getStringValue3().length(); i++) {
            char ch = event.getStringValue3().charAt(i);

            if (Character.isLetter(ch)) {
                letterCount++;
            } else if (Character.isDigit(ch)) {
                digitCount++;
            }
        }
        result.setDigitCount(digitCount);
        result.setLetterCount(letterCount);
    }

    static class CountLettersAndDigitsNewObject implements MapFunction<Event, Tuple2<Event, Statistics>> {

        @Override
        public Tuple2<Event, Statistics> map(Event event) {
            Statistics output = new Statistics();
            countLettersAndDigits(event, output);
            return Tuple2.of(event, output);
        }
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void withObjectReuseDisabled(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(1);
        env.getConfig().disableObjectReuse();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(Event.class);
        serializerConfig.registerPojoType(Statistics.class);

        env.fromData(pojos)
                .map(new PassThrough<Event>())
                .map(new CountLettersAndDigitsNewObject())
                .map(new PassThrough<Tuple2<Event, Statistics>>())
                .sinkTo(new DiscardingSink<>());
        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void withObjectReuseEnabled(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(1);
        env.getConfig().enableObjectReuse();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(Event.class);
        serializerConfig.registerPojoType(Statistics.class);

        env.fromData(pojos)
                .map(new PassThrough<Event>())
                .map(new CountLettersAndDigitsNewObject())
                .map(new PassThrough<Tuple2<Event, Statistics>>())
                .sinkTo(new DiscardingSink<>());
        env.execute();
    }

}
