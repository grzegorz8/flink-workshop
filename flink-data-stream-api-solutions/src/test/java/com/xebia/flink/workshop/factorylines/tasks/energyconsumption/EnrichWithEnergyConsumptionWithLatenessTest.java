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
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading.Type.ENERGY_CONSUMPTION;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class EnrichWithEnergyConsumptionWithLatenessTest {

    private static final Duration ALLOWED_LATENESS = Duration.ofSeconds(5);

    private KeyedTwoInputStreamOperatorTestHarness<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent> testHarness;

    @BeforeEach
    public void setupTestHarness() throws Exception {
        EnrichWithEnergyConsumptionWithLateness function =
                new EnrichWithEnergyConsumptionWithLateness(ALLOWED_LATENESS);

        testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                new KeyedCoProcessOperator<>(function),
                (KeySelector<ProcessingEvent, Tuple2<Integer, Integer>>) e -> Tuple2.of(e.getLine(), e.getStation()),
                (KeySelector<SensorReadings, Tuple2<Integer, Integer>>) r -> Tuple2.of(r.getLine(), r.getStation()),
                Types.TUPLE(Types.INT, Types.INT)
        );
        testHarness.open();
    }

    // -------------------------------------------------------------------------
    // Tests inherited from the base class — on-time events must work identically
    // -------------------------------------------------------------------------

    @Test
    void shouldMatchCorrectSensorReadingForOnTimeEvent() throws Exception {
        ProcessingEvent event01 = processingEvent("2025-01-30T12:00:02.500Z");

        SensorReadings readings01 = sensorReadings("2025-01-30T12:00:00.000Z", 100.0);
        SensorReadings readings02 = sensorReadings("2025-01-30T12:00:01.000Z", 110.0);
        SensorReadings readings03 = sensorReadings("2025-01-30T12:00:02.000Z", 120.0);
        SensorReadings readings04 = sensorReadings("2025-01-30T12:00:03.000Z", 130.0);

        testHarness.processElement2(readings01, readings01.getTimestamp().toEpochMilli());
        testHarness.processElement2(readings02, readings02.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:02.000Z").toEpochMilli()));
        testHarness.processElement1(event01, event01.getTimestamp().toEpochMilli());
        testHarness.processElement2(readings03, readings03.getTimestamp().toEpochMilli());
        testHarness.processElement2(readings04, readings04.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:04.000Z").toEpochMilli()));

        List<EnrichedProcessingEvent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertEquals(120.0, outputs.get(0).getSensorReadings().getSensorReadingsMap().get(ENERGY_CONSUMPTION).getValue());
    }

    @Test
    void shouldNotMatchSensorReadingFromTheFuture() throws Exception {
        ProcessingEvent event01 = processingEvent("2025-01-30T12:00:02.500Z");
        SensorReadings readings01 = sensorReadings("2025-01-30T12:00:03.000Z", 100.0);

        testHarness.processElement1(event01, event01.getTimestamp().toEpochMilli());
        testHarness.processElement2(readings01, readings01.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:04.000Z").toEpochMilli()));

        List<EnrichedProcessingEvent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertNull(outputs.get(0).getSensorReadings());
    }

    @Test
    void shouldReturnNullSensorReadingIfNoneArrived() throws Exception {
        ProcessingEvent event01 = processingEvent("2025-01-30T12:00:02.500Z");

        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:02.000Z").toEpochMilli()));
        testHarness.processElement1(event01, event01.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:04.000Z").toEpochMilli()));

        List<EnrichedProcessingEvent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertNull(outputs.get(0).getSensorReadings());
    }

    // -------------------------------------------------------------------------
    // Tests specific to allowed lateness
    // -------------------------------------------------------------------------

    @Test
    void shouldEmitLateEventImmediatelyWhenWithinAllowedLateness() throws Exception {
        // Sensor reading arrives before the event.
        SensorReadings readings01 = sensorReadings("2025-01-30T12:00:00.000Z", 100.0);

        // Watermark advances past the event timestamp — event at T=2.5s, watermark at T=6s.
        // Lateness = 5s, so events at T > 6-5 = 1s are still within allowed lateness.
        testHarness.processElement2(readings01, readings01.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:06.000Z").toEpochMilli()));

        // Late event arrives (2.5s < watermark 6s, but 2.5s > 6s - 5s = 1s → within lateness).
        ProcessingEvent lateEvent = processingEvent("2025-01-30T12:00:02.500Z");
        testHarness.processElement1(lateEvent, lateEvent.getTimestamp().toEpochMilli());

        List<EnrichedProcessingEvent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size());
        assertEquals(100.0, outputs.get(0).getSensorReadings().getSensorReadingsMap().get(ENERGY_CONSUMPTION).getValue());
    }

    @Test
    void shouldDropLateEventBeyondAllowedLateness() throws Exception {
        SensorReadings readings01 = sensorReadings("2025-01-30T12:00:00.000Z", 100.0);

        // Watermark at T=10s. Allowed lateness = 5s → threshold = 5s.
        // Event at T=2.5s is beyond the threshold (2.5s < 5s) → dropped.
        testHarness.processElement2(readings01, readings01.getTimestamp().toEpochMilli());
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:10.000Z").toEpochMilli()));

        ProcessingEvent tooLateEvent = processingEvent("2025-01-30T12:00:02.500Z");
        testHarness.processElement1(tooLateEvent, tooLateEvent.getTimestamp().toEpochMilli());

        List<EnrichedProcessingEvent> outputs = testHarness.extractOutputValues();
        assertEquals(0, outputs.size());
    }

    @Test
    void shouldMatchCorrectSensorReadingForLateEvent() throws Exception {
        // Multiple sensor readings; late event should match the latest reading at or before its timestamp.
        SensorReadings readings01 = sensorReadings("2025-01-30T12:00:00.000Z", 100.0);
        SensorReadings readings02 = sensorReadings("2025-01-30T12:00:02.000Z", 120.0);
        SensorReadings readings03 = sensorReadings("2025-01-30T12:00:04.000Z", 140.0);

        testHarness.processElement2(readings01, readings01.getTimestamp().toEpochMilli());
        testHarness.processElement2(readings02, readings02.getTimestamp().toEpochMilli());
        testHarness.processElement2(readings03, readings03.getTimestamp().toEpochMilli());

        // Watermark at T=8s. Late event at T=2.5s → within lateness (8-5=3s threshold? No, 2.5 < 3).
        // Let's use watermark=7s so threshold=2s, and event at T=2.5s → 2.5 > 2 → within lateness.
        testHarness.processBothWatermarks(new Watermark(Instant.parse("2025-01-30T12:00:07.000Z").toEpochMilli()));

        ProcessingEvent lateEvent = processingEvent("2025-01-30T12:00:02.500Z");
        testHarness.processElement1(lateEvent, lateEvent.getTimestamp().toEpochMilli());

        List<EnrichedProcessingEvent> outputs = testHarness.extractOutputValues();
        assertEquals(1, outputs.size());
        // Latest reading at or before 2.5s is readings02 (at 2s) with value 120.0.
        assertEquals(120.0, outputs.get(0).getSensorReadings().getSensorReadingsMap().get(ENERGY_CONSUMPTION).getValue());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static ProcessingEvent processingEvent(String timestamp) {
        ProcessingEvent event = new ProcessingEvent();
        event.setTimestamp(Instant.parse(timestamp));
        event.setLine(1);
        event.setStation(1);
        event.setAction(ProcessingEvent.Action.IN);
        return event;
    }

    private static SensorReadings sensorReadings(String timestamp, double value) {
        SensorReadings readings = new SensorReadings();
        readings.setTimestamp(Instant.parse(timestamp));
        readings.setLine(1);
        readings.setStation(1);
        readings.setSensorReadingsMap(singletonMap(ENERGY_CONSUMPTION, new SensorReading(ENERGY_CONSUMPTION, value, "kWh")));
        return readings;
    }
}