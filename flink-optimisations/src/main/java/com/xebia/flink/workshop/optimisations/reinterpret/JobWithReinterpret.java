package com.xebia.flink.workshop.optimisations.reinterpret;

import com.xebia.flink.workshop.optimisations.reinterpret.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import static org.apache.flink.streaming.api.datastream.DataStreamUtils.reinterpretAsKeyedStream;

public class JobWithReinterpret extends JobBase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(createConfiguration());
        env.setParallelism(3);
        // TODO: toggle object reuse config
        env.getConfig().disableObjectReuse();
//         env.getConfig().enableObjectReuse();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) env.getConfig().getSerializerConfig();
        serializerConfig.registerPojoType(Event.class);

        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new InputGenerator(),
                Long.MAX_VALUE,
                TypeInformation.of(Event.class)
        );

        SingleOutputStreamOperator<Event> stream1 = env.fromSource(source, WatermarkStrategy.noWatermarks(), "test source")
                .keyBy(Event::getId)
                .map(new CountLettersAndDigits(Event::getStringValue1, Event::setLettersCount1, Event::setDigitsCount1));
        SingleOutputStreamOperator<Event> stream2 = reinterpretAsKeyedStream(stream1, Event::getId)
                .map(new CountLettersAndDigits(Event::getStringValue2, Event::setLettersCount2, Event::setDigitsCount2));
        reinterpretAsKeyedStream(stream2, Event::getId)
                .map(new CountLettersAndDigits(Event::getStringValue3, Event::setLettersCount3, Event::setDigitsCount3))
                .sinkTo(new DiscardingSink<>());

        env.execute();
    }

}
