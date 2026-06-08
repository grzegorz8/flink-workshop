package com.xebia.flink.workshop.optimisations.serialization;

import com.xebia.flink.workshop.optimisations.serialization.model.EventNonPojo;
import com.xebia.flink.workshop.optimisations.serialization.model.EventPojo;
import com.xebia.flink.workshop.optimisations.serialization.model.EventRecord;
import com.twitter.chill.protobuf.ProtobufSerializer;
import com.xebia.flink.workshop.optimisations.serialization.proto.EventProto;
import com.xebia.flink.workshop.optimisations.serialization.proto.NestedObjectProto;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
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

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.xebia.flink.workshop.utils.RandomStringGenerator.generateRandomString;

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
public class SerializationBenchmarks {

    private static final Random RANDOM = new Random();
    private static final int RECORDS_PER_INVOCATION = 10_000_000;

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + SerializationBenchmarks.class.getCanonicalName() + ".*")
                .build();

        new Runner(options).run();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serializerPojo(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(4);
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(EventPojo.class);
        serializerConfig.registerPojoType(EventPojo.NestedObject.class);

        DataGeneratorSource<EventPojo> source = new DataGeneratorSource<>(
                new PojoInputGenerator(),
                RECORDS_PER_INVOCATION,
                TypeInformation.of(EventPojo.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "pojo-source")
                .rebalance()
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serializerRecord(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(4);
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(EventRecord.class);
        serializerConfig.registerPojoType(EventRecord.NestedObject.class);

        DataGeneratorSource<EventRecord> source = new DataGeneratorSource<>(
                new RecordInputGenerator(),
                RECORDS_PER_INVOCATION,
                TypeInformation.of(EventRecord.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "record-source")
                .rebalance()
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serializerTuple(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(4);

        DataGeneratorSource<Tuple9<Long, Long, Long, String, String, String, List<Tuple3<String, String, Long>>, Boolean, Boolean>> source = new DataGeneratorSource<>(
                new TupleInputGenerator(),
                RECORDS_PER_INVOCATION,
                Types.TUPLE(Types.LONG, Types.LONG, Types.LONG, Types.STRING, Types.STRING, Types.STRING, Types.LIST(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)), Types.BOOLEAN, Types.BOOLEAN)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "tuple-source")
                .rebalance()
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serializerSpecificAvro(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.setForceAvro(true);
        env.setParallelism(4);

        DataGeneratorSource<EventAvro> source = new DataGeneratorSource<>(
                new AvroInputGenerator(),
                RECORDS_PER_INVOCATION,
                TypeInformation.of(EventAvro.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "avro-source")
                .rebalance()
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serializerNonPojo(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(4);

        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.setGenericTypes(true);

        DataGeneratorSource<EventNonPojo> source = new DataGeneratorSource<>(
                new NonPojoInputGenerator(),
                RECORDS_PER_INVOCATION,
                TypeInformation.of(EventNonPojo.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "non-pojo-source")
                .rebalance()
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serializerFastAvro(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(4);

        DataGeneratorSource<EventAvro> source = new DataGeneratorSource<>(
                new AvroInputGenerator(),
                RECORDS_PER_INVOCATION,
                new FastAvroTypeInfo<>(EventAvro.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "fast-avro-source")
                .rebalance()
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serializerProtobuf(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(4);
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.setGenericTypes(true);
        serializerConfig.setForceKryo(true);
        serializerConfig.registerTypeWithKryoSerializer(EventProto.class, ProtobufSerializer.class);
        serializerConfig.registerTypeWithKryoSerializer(NestedObjectProto.class, ProtobufSerializer.class);

        DataGeneratorSource<EventProto> source = new DataGeneratorSource<>(
                new ProtobufInputGenerator(),
                RECORDS_PER_INVOCATION,
                TypeInformation.of(EventProto.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "protobuf-source")
                .rebalance()
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serializerKryo(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(4);
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.setForceKryo(true);
        serializerConfig.registerKryoType(EventPojo.class);
        serializerConfig.registerKryoType(EventPojo.NestedObject.class);

        DataGeneratorSource<EventPojo> source = new DataGeneratorSource<>(
                new PojoInputGenerator(),
                RECORDS_PER_INVOCATION,
                TypeInformation.of(EventPojo.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kryo-source")
                .rebalance()
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

    static class PojoInputGenerator implements GeneratorFunction<Long, EventPojo> {
        private EventPojo template;

        @Override
        public void open(SourceReaderContext readerContext) {
            template = new EventPojo(
                    0L,
                    200L,
                    300L,
                    generateRandomString(10),
                    generateRandomString(15),
                    generateRandomString(8),
                    IntStream.range(0, 3).boxed()
                            .map(i -> new EventPojo.NestedObject(generateRandomString(15), generateRandomString(5), (long) i))
                            .collect(Collectors.toList()),
                    RANDOM.nextBoolean(),
                    RANDOM.nextBoolean()
            );
        }

        @Override
        public EventPojo map(Long value) {
            template.setId(value % 1000L);
            return template;
        }
    }

    static class NonPojoInputGenerator implements GeneratorFunction<Long, EventNonPojo> {
        private EventNonPojo template;

        @Override
        public void open(SourceReaderContext readerContext) {
            template = new EventNonPojo(
                    0L,
                    200L,
                    300L,
                    generateRandomString(10),
                    generateRandomString(15),
                    generateRandomString(8),
                    IntStream.range(0, 3).boxed()
                            .map(i -> new EventNonPojo.NestedObject(generateRandomString(15), generateRandomString(5), (long) i))
                            .collect(Collectors.toList()),
                    RANDOM.nextBoolean(),
                    RANDOM.nextBoolean()
            );
        }

        @Override
        public EventNonPojo map(Long value) {
            template.setId(value % 1000L);
            return template;
        }
    }

    static class RecordInputGenerator implements GeneratorFunction<Long, EventRecord> {
        private EventRecord template;

        @Override
        public void open(SourceReaderContext readerContext) {
            template = new EventRecord(
                    0L,
                    200L,
                    300L,
                    generateRandomString(10),
                    generateRandomString(15),
                    generateRandomString(8),
                    IntStream.range(0, 3).boxed()
                            .map(i -> new EventRecord.NestedObject(generateRandomString(15), generateRandomString(5), (long) i))
                            .collect(Collectors.toList()),
                    RANDOM.nextBoolean(),
                    RANDOM.nextBoolean()
            );
        }

        @Override
        public EventRecord map(Long value) {
            return template;
        }
    }

    static class TupleInputGenerator implements GeneratorFunction<Long, Tuple9<Long, Long, Long, String, String, String, List<Tuple3<String, String, Long>>, Boolean, Boolean>> {
        private Tuple9<Long, Long, Long, String, String, String, List<Tuple3<String, String, Long>>, Boolean, Boolean> template;

        @Override
        public void open(SourceReaderContext readerContext) {
            template = Tuple9.of(0L,
                    200L,
                    300L,
                    generateRandomString(10),
                    generateRandomString(15),
                    generateRandomString(8),
                    IntStream.range(0, 3).boxed()
                            .map(i -> Tuple3.of(generateRandomString(15), generateRandomString(5), (long) i))
                            .collect(Collectors.toList()),
                    RANDOM.nextBoolean(),
                    RANDOM.nextBoolean()
            );
        }

        @Override
        public Tuple9<Long, Long, Long, String, String, String, List<Tuple3<String, String, Long>>, Boolean, Boolean> map(Long value) {
            return template;
        }
    }

    static class ProtobufInputGenerator implements GeneratorFunction<Long, EventProto> {
        private EventProto template;

        @Override
        public void open(SourceReaderContext readerContext) {
            EventProto.Builder builder = EventProto.newBuilder()
                    .setId(0L)
                    .setLongValue1(200L)
                    .setLongValue2(300L)
                    .setStringValue1(generateRandomString(10))
                    .setStringValue2(generateRandomString(15))
                    .setStringValue3(generateRandomString(8))
                    .setBooleanValue1(RANDOM.nextBoolean())
                    .setBooleanValue2(RANDOM.nextBoolean());
            IntStream.range(0, 3).forEach(i -> builder.addNestedObjectList(
                    NestedObjectProto.newBuilder()
                            .setStringValue1(generateRandomString(15))
                            .setStringValue2(generateRandomString(5))
                            .setLongValue1(i)
                            .build()
            ));
            template = builder.build();
        }

        @Override
        public EventProto map(Long value) {
            return template;
        }
    }

    static class AvroInputGenerator implements GeneratorFunction<Long, EventAvro> {
        private EventAvro template;

        @Override
        public void open(SourceReaderContext readerContext) {
            template = new EventAvro(
                    0L,
                    200L,
                    300L,
                    generateRandomString(10),
                    generateRandomString(15),
                    generateRandomString(8),
                    IntStream.range(0, 3).boxed()
                            .map(i -> new NestedObject(generateRandomString(15), generateRandomString(5), (long) i))
                            .collect(Collectors.toList()),
                    RANDOM.nextBoolean(),
                    RANDOM.nextBoolean()
            );
        }

        @Override
        public EventAvro map(Long value) {
            template.setId(value % 1000L);
            return template;
        }
    }

}
