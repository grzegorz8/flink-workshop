package com.xebia.flink.workshop.serde;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.util.function.SerializableSupplier;

@PublicEvolving
public class JsonSerializationSchema<T> implements SerializationSchema<T> {

    private static final long serialVersionUID = 1L;
    private final SerializableSupplier<ObjectMapper> mapperFactory;
    protected transient ObjectMapper mapper;

    public JsonSerializationSchema() {
        this(() -> {
            ObjectMapper m = new ObjectMapper();
            m.registerModule(new JavaTimeModule());
            return m;
        });
    }

    public JsonSerializationSchema(SerializableSupplier<ObjectMapper> mapperFactory) {
        this.mapperFactory = mapperFactory;
    }

    public void open(InitializationContext context) {
        this.mapper = (ObjectMapper) this.mapperFactory.get();
    }

    public byte[] serialize(T element) {
        try {
            return this.mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(String.format("Could not serialize value '%s'.", element), e);
        }
    }
}

