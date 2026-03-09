package com.xebia.flink.workshop.optimisations.objectreuse;

import com.xebia.flink.workshop.optimisations.objectreuse.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;

public class ObjectReuseScenario02 {

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // Toggle object-reuse setting.
         env.getConfig().enableObjectReuse();
//        env.getConfig().disableObjectReuse();

        DataGeneratorSource<Event> source = new DataGeneratorSource<>(
                new InputGenerator(),
                5,
                RateLimiterStrategy.perSecond(parameters.getDouble("records-per-second", 1.0d)),
                TypeInformation.of(Event.class)
        );

        // Pipeline:
        //                               Input
        //                                 |
        //                                 |
        //                               keyBy
        //                                 |
        //                        SomeProcessFunction (value := "process")
        //                                 |
        //                                 |
        //                           FooMapFunction  (value := "foo")
        //                                 |
        //         --------------------------------------------
        //         |                                          |
        //   BarMapFunction (value := "bar")           BazMapFunction (value := "baz")
        //         |                                          |
        //       Print                                      Print


        SingleOutputStreamOperator<Event> processedAndMapped = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Input")
                .keyBy(Event::getId)
                .process(new SomeProcessFunction())
                .map(new FooMapFunction());

        processedAndMapped.map(new BarMapFunction()).print();
        processedAndMapped.map(new BazMapFunction()).print();

        env.execute("Object Reuse Test");

    }

    static class InputGenerator implements GeneratorFunction<Long, Event> {
        @Override
        public Event map(Long value) {
            Event event = new Event();
            event.setId(value);
            event.setStringValue1("value-" + value);
            return event;
        }
    }

    static class SomeProcessFunction extends KeyedProcessFunction<Long, Event, Event> {
        @Override
        public void processElement(Event event,
                                   KeyedProcessFunction<Long, Event, Event>.Context context,
                                   Collector<Event> collector) {
            event.setStringValue1("process");
            collector.collect(event);
        }
    }

    static class FooMapFunction implements MapFunction<Event, Event> {
        @Override
        public Event map(Event event) {
            event.setStringValue1("foo");
            return event;
        }
    }

    static class BarMapFunction implements MapFunction<Event, Event> {
        @Override
        public Event map(Event event) {
            if (!event.getStringValue1().equals("foo")) {
                throw new RuntimeException("Unexpected value in BarMapFunction");
            }
            event.setStringValue1("bar");
            return event;
        }
    }

    static class BazMapFunction implements MapFunction<Event, Event> {
        @Override
        public Event map(Event event) {
            // Note: if object-reuse is enabled, event was already modified by BarMapFunction in "parallel" branch.
            if (!event.getStringValue1().equals("foo")) {
                throw new RuntimeException("Unexpected value in BazMapFunction");
            }
            event.setStringValue1("baz");
            return event;
        }
    }
}
