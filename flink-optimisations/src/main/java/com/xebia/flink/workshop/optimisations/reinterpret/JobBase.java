package com.xebia.flink.workshop.optimisations.reinterpret;

import com.xebia.flink.workshop.optimisations.reinterpret.model.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.xebia.flink.workshop.utils.RandomStringGenerator.generateRandomString;

abstract class JobBase {

    protected static final Random RANDOM = new Random();


    protected static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.GENERIC_TYPES, false);
        configuration.setString("rest.flamegraph.enabled", "true");
        return configuration;
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

}
