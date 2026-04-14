package com.xebia.flink.workshop.serde;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.util.function.SerializableSupplier;

@PublicEvolving
public class JsonSerializationSchema<T> implements SerializationSchema<T>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;
    private final SerializableSupplier<ObjectMapper> mapperFactory;
    private final TypeInformation<T> typeInformation;
    protected transient ObjectMapper mapper;

    public JsonSerializationSchema(Class<T> clazz) {
        this(TypeInformation.of(clazz));
    }

    public JsonSerializationSchema(TypeInformation<T> typeInformation) {
        this(typeInformation, () -> {
            ObjectMapper m = new ObjectMapper();
            m.registerModule(new JavaTimeModule());
            return m;
        });
    }

    public JsonSerializationSchema(TypeInformation<T> typeInformation, SerializableSupplier<ObjectMapper> mapperFactory) {
        this.typeInformation = typeInformation;
        this.mapperFactory = mapperFactory;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return typeInformation;
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