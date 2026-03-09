package com.xebia.flink.workshop.optimisations.reinterpret;

import com.xebia.flink.workshop.optimisations.reinterpret.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

public class JobWithoutReinterpret extends JobBase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(createConfiguration());
        env.setParallelism(3);
        env.getConfig().disableObjectReuse();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(Event.class);

        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new InputGenerator(),
                Integer.MAX_VALUE,
                TypeInformation.of(Event.class)
        );

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "test source")
                .keyBy(Event::getId)
                .map(new CountLettersAndDigits(Event::getStringValue1, Event::setLettersCount1, Event::setDigitsCount1))
                .keyBy(Event::getId)
                .map(new CountLettersAndDigits(Event::getStringValue2, Event::setLettersCount2, Event::setDigitsCount2))
                .keyBy(Event::getId)
                .map(new CountLettersAndDigits(Event::getStringValue3, Event::setLettersCount3, Event::setDigitsCount3))
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }
}