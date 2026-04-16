/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xebia.flink.workshop.autoscaler;

import com.xebia.flink.workshop.model.Event;
import com.xebia.flink.workshop.serde.JsonSerializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class BusyJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> source1 = env
                .fromSource(
                        KafkaSource.<Event>builder()
                                .setBootstrapServers("rta-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
                                .setDeserializer(KafkaRecordDeserializationSchema.<Event>valueOnly(new JsonDeserializationSchema<>(Event.class)))
                                .setTopics("events")
                                .build(),
                        WatermarkStrategy.forMonotonousTimestamps(),
                        "events"
                )
                .keyBy(Event::getId)
                .process(new SomeProcessFunction());

        source1
                .keyBy(Event::getId)
                .process(new SomeProcessFunction()).name("some-process-function")
                .keyBy(Event::getId)
                .process(new AnotherProcessFunction()).name("another-process-function")
                .sinkTo(
                        KafkaSink.<Event>builder()
                                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                                .setBootstrapServers("rta-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
                                .setRecordSerializer(KafkaRecordSerializationSchema.<Event>builder()
                                        .setValueSerializationSchema(new JsonSerializationSchema<>(Event.class))
                                        .setTopic("events2")
                                        .build())
                                .build()
                );
        env.execute("Busy job");
    }

    static class SomeProcessFunction extends KeyedProcessFunction<Long, Event, Event> {

        @Override
        public void processElement(Event event,
                                   KeyedProcessFunction<Long, Event, Event>.Context context,
                                   Collector<Event> collector) throws Exception {
            Thread.sleep(20L);
            collector.collect(event);
        }
    }

    static class AnotherProcessFunction extends KeyedProcessFunction<Long, Event, Event> {

        @Override
        public void processElement(Event event,
                                   KeyedProcessFunction<Long, Event, Event>.Context context,
                                   Collector<Event> collector) throws Exception {
            Thread.sleep(10L);
            collector.collect(event);
        }
    }
}
