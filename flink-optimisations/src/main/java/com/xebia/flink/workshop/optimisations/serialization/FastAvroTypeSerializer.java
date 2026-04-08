package com.xebia.flink.workshop.optimisations.serialization;

import com.linkedin.avro.fastserde.FastSpecificDatumReader;
import com.linkedin.avro.fastserde.FastSpecificDatumWriter;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FastAvroTypeSerializer<T extends SpecificRecordBase> extends TypeSerializer<T> {

    private final Class<T> type;
    private final Schema schema;

    private transient FastSpecificDatumWriter<T> writer;
    private transient FastSpecificDatumReader<T> reader;
    private transient BinaryEncoder encoder;
    private transient BinaryDecoder decoder;
    private transient DataOutputViewOutputStream outputStream;
    private transient DataInputViewInputStream inputStream;

    public FastAvroTypeSerializer(Class<T> type) {
        this.type = type;
        try {
            this.schema = (Schema) type.getField("SCHEMA$").get(null);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot read SCHEMA$ from " + type.getName(), e);
        }
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        if (writer == null) {
            writer = new FastSpecificDatumWriter<>(schema);
            outputStream = new DataOutputViewOutputStream();
        }
        outputStream.target = target;
        encoder = EncoderFactory.get().directBinaryEncoder(outputStream, encoder);
        writer.write(record, encoder);
        encoder.flush();
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        if (reader == null) {
            reader = new FastSpecificDatumReader<>(schema, schema);
            inputStream = new DataInputViewInputStream();
        }
        inputStream.source = source;
        decoder = DecoderFactory.get().directBinaryDecoder(inputStream, decoder);
        return reader.read(null, decoder);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public T copy(T from) {
        try {
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            BinaryEncoder enc = EncoderFactory.get().directBinaryEncoder(baos, null);
            new FastSpecificDatumWriter<T>(schema).write(from, enc);
            enc.flush();
            BinaryDecoder dec = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null);
            return new FastSpecificDatumReader<T>(schema, schema).read(null, dec);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T copy(T from, T reuse) {
        return copy(from);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public T createInstance() {
        try {
            return type.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Cannot instantiate " + type.getName(), e);
        }
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return new FastAvroTypeSerializer<>(type);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new Snapshot<>(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FastAvroTypeSerializer<?> that)) return false;
        return type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    // --- Adapters ---

    private static final class DataOutputViewOutputStream extends OutputStream {
        DataOutputView target;

        @Override
        public void write(int b) throws IOException {
            target.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            target.write(b, off, len);
        }
    }

    private static final class DataInputViewInputStream extends InputStream {
        DataInputView source;

        @Override
        public int read() throws IOException {
            return source.readUnsignedByte();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            source.readFully(b, off, len);
            return len;
        }
    }

    // --- Snapshot ---

    public static class Snapshot<T extends SpecificRecordBase> implements TypeSerializerSnapshot<T> {

        private Class<T> type;

        @SuppressWarnings("unused") // required by Flink for deserialization
        public Snapshot() {}

        public Snapshot(Class<T> type) {
            this.type = type;
        }

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeUTF(type.getName());
        }

        @Override
        @SuppressWarnings("unchecked")
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader cl) throws IOException {
            try {
                type = (Class<T>) Class.forName(in.readUTF(), false, cl);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            return new FastAvroTypeSerializer<>(type);
        }

        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializerSnapshot<T> oldSerializerSnapshot) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
    }
}