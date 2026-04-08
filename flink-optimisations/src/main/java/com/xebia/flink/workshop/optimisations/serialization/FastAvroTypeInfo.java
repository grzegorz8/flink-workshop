package com.xebia.flink.workshop.optimisations.serialization;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;

public class FastAvroTypeInfo<T extends SpecificRecordBase> extends TypeInformation<T> {

    private final Class<T> type;

    public  FastAvroTypeInfo(Class<T> type) {
        this.type = type;
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<T> getTypeClass() {
        return type;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<T> createSerializer(SerializerConfig config) {
        return new FastAvroTypeSerializer<>(type);
    }

    @Override
    public String toString() {
        return "FastAvroTypeInfo<" + type.getSimpleName() + ">";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FastAvroTypeInfo<?> that)) return false;
        return Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return type.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof FastAvroTypeInfo;
    }
}