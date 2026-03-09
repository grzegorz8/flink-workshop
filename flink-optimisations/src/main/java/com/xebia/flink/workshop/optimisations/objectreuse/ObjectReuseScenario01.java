package com.xebia.flink.workshop.optimisations.objectreuse;

import com.xebia.flink.workshop.optimisations.objectreuse.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;

public class ObjectReuseScenario01 {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // Toggle object-reuse setting.
        // env.getConfig().enableObjectReuse();
        env.getConfig().disableObjectReuse();

        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new InputGenerator(),
                5,
                RateLimiterStrategy.perSecond(parameters.getDouble("records-per-second", 1.0d)),
                TypeInformation.of(Event.class)
        );

        env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Input")
                .keyBy(Event::getId)
                .process(new EmitEventTwice())
                .map(new DoubleLongValue())
                .print();

        env.execute("Object Reuse Test");

    }

    static class InputGenerator implements GeneratorFunction<Long, Event> {
        @Override
        public Event map(Long value) {
            Event event = new Event();
            event.setId(value);
            event.setLongValue2(value);
            return event;
        }
    }

    static class EmitEventTwice extends KeyedProcessFunction<Long, Event, Event> {
        @Override
        public void processElement(Event event,
                                   KeyedProcessFunction<Long, Event, Event>.Context context,
                                   Collector<Event> collector) {
            event.setStringValue1("emitted first time");
            collector.collect(event);
            // Note: If object-reuse is enabled, event is already modified by DoubleLongValue!
            event.setStringValue1("emitted second time");
            collector.collect(event);
        }
    }

    static class DoubleLongValue implements MapFunction<Event, Event> {
        @Override
        public Event map(Event event) {
            event.setLongValue2(event.getLongValue2() * 2);
            return event;
        }
    }

}
