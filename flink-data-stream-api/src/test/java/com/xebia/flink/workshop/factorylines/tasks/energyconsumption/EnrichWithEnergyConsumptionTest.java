package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.SensorReadings;
import com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading.Type.ENERGY_CONSUMPTION;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class EnrichWithEnergyConsumptionTest {

    private KeyedTwoInputStreamOperatorTestHarness<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent> testHarness;

    @BeforeEach
    public void setupTestHarness() throws Exception {
        EnrichWithEnergyConsumption function = new EnrichWithEnergyConsumption();

        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(function),
                (KeySelector<ProcessingEvent, Tuple2<Integer, Integer>>) e -> Tuple2.of(e.getLine(), e.getStation()),
                (KeySelector<SensorReadings, Tuple2<Integer, Integer>>) r -> Tuple2.of(r.getLine(), r.getStation()),
                Types.TUPLE(Types.INT, Types.INT)
        );
        testHarness.open();
    }

    @Disabled   // TODO: Enable when logic is implemented
    @Test
    void shouldMatchCorrectSensorReading() throws Exception {
        // given
        ProcessingEvent event01 = new ProcessingEvent();
        event01.setTimestamp(Instant.parse("2025-01-30T12:00:02.500Z"));
        event01.setLine(1);
        event01.setStation(1);
        event01.setAction(ProcessingEvent.Action.IN);

        SensorReadings readings01 = new SensorReadings();
        readings01.setTimestamp(Instant.parse("2025-01-30T12:00:00.000Z"));
        readings01.setLine(1);
        readings01.setStation(1);
        readings01.setSensorReadingsMap(singletonMap(ENERGY_CONSUMPTION, new SensorReading(ENERGY_CONSUMPTION, 100.0, "kWh")));

        SensorReadings readings02 = new SensorReadings();
        readings02.setTimestamp(Instant.parse("2025-01-30T12:00:01.000Z"));
        readings02.setLine(1);
        readings02.setStation(1);
        readings02.setSensorReadingsMap(singletonMap(ENERGY_CONSUMPTION, new SensorReading(ENERGY_CONSUMPTION, 110.0, "kWh")));

        SensorReadings readings03 = new SensorReadings();
        readings03.setTimestamp(Instant.parse("2025-01-30T12:00:02.000Z"));
        readings03.setLine(1);
        readings03.setStation(1);
        readings03.setSensorReadingsMap(singletonMap(ENERGY_CONSUMPTION, new SensorReading(ENERGY_CONSUMPTION, 120.0, "kWh")));

        SensorReadings readings04 = new SensorReadings();
        readings04.setTimestamp(Instant.parse("2025-01-30T12:00:03.000Z"));
        readings04.setLine(1);
        readings04.setStation(1);
        readings04.setSensorReadingsMap(singletonMap(ENERGY_CONSUMPTION, new SensorReading(ENERGY_CONSUMPTION, 130.0, "kWh")));

        // when
        testHarness.processElement2(readings01, readings01.getTimestamp().toEpochMilli());
        testHarness.processElement2(readings02, readings02.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:02.000Z").toEpochMilli()));
        testHarness.processElement1(event01, event01.getTimestamp().toEpochMilli());
        testHarness.processElement2(readings03, readings03.getTimestamp().toEpochMilli());
        testHarness.processElement2(readings04, readings04.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:04.000Z").toEpochMilli()));

        // then
        List<EnrichedProcessingEvent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertEquals(120.0, outputs.get(0).getSensorReadings().getSensorReadingsMap().get(ENERGY_CONSUMPTION).getValue());
    }

    @Disabled   // TODO: Enable when logic is implemented
    @Test
    void shouldMatchEvenOldSensorReading() throws Exception {
        // given
        ProcessingEvent event01 = new ProcessingEvent();
        event01.setTimestamp(Instant.parse("2025-01-30T12:00:02.500Z"));
        event01.setLine(1);
        event01.setStation(1);
        event01.setAction(ProcessingEvent.Action.IN);

        SensorReadings readings01 = new SensorReadings();
        readings01.setTimestamp(Instant.parse("2025-01-30T11:00:00.000Z"));
        readings01.setLine(1);
        readings01.setStation(1);
        readings01.setSensorReadingsMap(singletonMap(ENERGY_CONSUMPTION, new SensorReading(ENERGY_CONSUMPTION, 100.0, "kWh")));

        // when
        testHarness.processElement2(readings01, readings01.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T11:00:02.000Z").toEpochMilli()));
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:00.000Z").toEpochMilli()));
        testHarness.processElement1(event01, event01.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:04.000Z").toEpochMilli()));

        // then
        List<EnrichedProcessingEvent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertEquals(100.0, outputs.get(0).getSensorReadings().getSensorReadingsMap().get(ENERGY_CONSUMPTION).getValue());
    }

    @Disabled   // TODO: Enable when logic is implemented
    @Test
    void shouldNotMatchSensorReadingFromTheFuture() throws Exception {
        // given
        ProcessingEvent event01 = new ProcessingEvent();
        event01.setTimestamp(Instant.parse("2025-01-30T12:00:02.500Z"));
        event01.setLine(1);
        event01.setStation(1);
        event01.setAction(ProcessingEvent.Action.IN);

        SensorReadings readings01 = new SensorReadings();
        readings01.setTimestamp(Instant.parse("2025-01-30T12:00:03.000Z"));
        readings01.setLine(1);
        readings01.setStation(1);
        readings01.setSensorReadingsMap(singletonMap(ENERGY_CONSUMPTION, new SensorReading(ENERGY_CONSUMPTION, 100.0, "kWh")));

        // when
        testHarness.processElement1(event01, event01.getTimestamp().toEpochMilli());
        testHarness.processElement2(readings01, readings01.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:04.000Z").toEpochMilli()));

        // then
        List<EnrichedProcessingEvent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertNull(outputs.get(0).getSensorReadings());
    }

    @Disabled   // TODO: Enable when logic is implemented
    @Test
    void shouldReturnEmptySensorReadingIfNoneArrived() throws Exception {
        // given
        ProcessingEvent event01 = new ProcessingEvent();
        event01.setTimestamp(Instant.parse("2025-01-30T12:00:02.500Z"));
        event01.setLine(1);
        event01.setStation(1);
        event01.setAction(ProcessingEvent.Action.IN);

        // when
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:02.000Z").toEpochMilli()));
        testHarness.processElement1(event01, event01.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:04.000Z").toEpochMilli()));

        // then
        List<EnrichedProcessingEvent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertNull(outputs.get(0).getSensorReadings());
    }

}