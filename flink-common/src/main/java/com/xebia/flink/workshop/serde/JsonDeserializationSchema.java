package com.xebia.flink.workshop.serde;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.function.SerializableSupplier;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import java.io.IOException;

@PublicEvolving
public class JsonDeserializationSchema<T> extends AbstractDeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> clazz;
    private final SerializableSupplier<ObjectMapper> mapperFactory;
    protected transient ObjectMapper mapper;

    public JsonDeserializationSchema(Class<T> clazz) {
        this(clazz, JacksonMapperFactory::createObjectMapper);
    }

    public JsonDeserializationSchema(TypeInformation<T> typeInformation) {
        this(typeInformation, JacksonMapperFactory::createObjectMapper);
    }

    public JsonDeserializationSchema(
            Class<T> clazz, SerializableSupplier<ObjectMapper> mapperFactory) {
        super(clazz);
        this.clazz = clazz;
        this.mapperFactory = mapperFactory;
    }

    public JsonDeserializationSchema(
            TypeInformation<T> typeInformation, SerializableSupplier<ObjectMapper> mapperFactory) {
        super(typeInformation);
        this.clazz = typeInformation.getTypeClass();
        this.mapperFactory = mapperFactory;
    }

    @Override
    public void open(InitializationContext context) {
        mapper = mapperFactory.get();
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, clazz);
    }
}
