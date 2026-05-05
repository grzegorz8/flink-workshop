package com.xebia.flink.workshop.optimisations.serialization;

import com.xebia.flink.workshop.optimisations.serialization.model.EventPojo;
import com.xebia.flink.workshop.optimisations.serialization.model.EventRecord;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
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

import java.io.IOException;
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
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class PojoSerializationBenchmarks {

    private static final Random RANDOM = new Random();
    private static final int RECORDS_PER_INVOCATION = 500_000;

    private DataOutputSerializer output;

    private EventPojo pojo;
    private TypeSerializer<EventPojo> pojoSerializer;

    private EventRecord record;
    private TypeSerializer<EventRecord> recordSerializer;

    public static void main(String[] args) throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + PojoSerializationBenchmarks.class.getCanonicalName() + ".*")
                .build();

        new Runner(options).run();
    }

    @Setup
    public void setup() throws IOException {
        ExecutionConfig config = new ExecutionConfig();
        this.output = new DataOutputSerializer(1024);

        this.pojoSerializer = TypeInformation.of(EventPojo.class).createSerializer(config.getSerializerConfig());
        this.pojo = new EventPojo(
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
        pojoSerializer.serialize(pojo, output);

        this.recordSerializer = TypeInformation.of(EventRecord.class).createSerializer(config.getSerializerConfig());
        this.record = new EventRecord(
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
        recordSerializer.serialize(record, output);

    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serializerPojo() throws Exception {
        output.clear();
        pojoSerializer.serialize(pojo, output);
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serializerRecord() throws Exception {
        output.clear();
        recordSerializer.serialize(record, output);
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serdePojo() throws Exception {
        output.clear();
        pojoSerializer.serialize(pojo, output);
        DataInputDeserializer input = new DataInputDeserializer(output.getSharedBuffer(), 0, output.length());
        pojoSerializer.deserialize(input);
    }

    @Benchmark
    @OperationsPerInvocation(value = RECORDS_PER_INVOCATION)
    public void serdeRecord() throws Exception {
        output.clear();
        recordSerializer.serialize(record, output);
        DataInputDeserializer input = new DataInputDeserializer(output.getSharedBuffer(), 0, output.length());
        recordSerializer.deserialize(input);
    }

}
