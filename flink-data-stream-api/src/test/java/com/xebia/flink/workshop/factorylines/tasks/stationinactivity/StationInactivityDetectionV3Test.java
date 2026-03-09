package com.xebia.flink.workshop.factorylines.tasks.stationinactivity;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StationInactivityDetectionV3Test {

    private KeyedOneInputStreamOperatorTestHarness<Tuple2<Integer, Integer>, ProcessingEvent, InactivityAlert> testHarness;

    @BeforeEach
    public void setupTestHarness() throws Exception {
        StationInactivityDetectionV3 function = new StationInactivityDetectionV3(Duration.ofMinutes(10L).toMillis());

        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(function),
                (KeySelector<ProcessingEvent, Tuple2<Integer, Integer>>) e -> Tuple2.of(e.getLine(), e.getStation()),
                Types.TUPLE(Types.INT, Types.INT)
        );
        testHarness.open();
    }

    @Test
    void shouldEmitInactivityAlertAfter10Minutes() throws Exception {
        // given
        ProcessingEvent event01 = new ProcessingEvent();
        event01.setLine(1);
        event01.setStation(2);
        event01.setTimestamp(Instant.parse("2026-01-30T12:00:00Z"));

        testHarness.processElement(event01, event01.getTimestamp().toEpochMilli());

        // when
        testHarness.processWatermark(Instant.parse("2026-01-30T12:10:01Z").toEpochMilli());

        // then
        List<InactivityAlert> inactivityAlerts = testHarness.extractOutputValues();
        assertEquals(1, inactivityAlerts.size());
        assertEquals(1, inactivityAlerts.get(0).getLine());
        assertEquals(2, inactivityAlerts.get(0).getStation());
    }

    @Test
    void shouldNotEmitInactivityIfNewEventArrivesWithin10Minutes() throws Exception {
        // given
        ProcessingEvent event01 = new ProcessingEvent();
        event01.setLine(1);
        event01.setStation(2);
        event01.setTimestamp(Instant.parse("2026-01-30T12:00:00Z"));

        ProcessingEvent event02 = new ProcessingEvent();
        event02.setLine(1);
        event02.setStation(2);
        event02.setTimestamp(Instant.parse("2026-01-30T12:09:50Z"));

        testHarness.processElement(event01, event01.getTimestamp().toEpochMilli());
        testHarness.processElement(event02, event02.getTimestamp().toEpochMilli());

        // when
        testHarness.processWatermark(Instant.parse("2026-01-30T12:10:01Z").toEpochMilli());

        // then
        List<InactivityAlert> inactivityAlerts = testHarness.extractOutputValues();
        assertEquals(0, inactivityAlerts.size());
    }

    @Test
    void shouldEmitProperInactivityAlertsIfEventsComeOutOfOrder() throws Exception {
        // given
        ProcessingEvent event01 = new ProcessingEvent();
        event01.setLine(1);
        event01.setStation(2);
        event01.setTimestamp(Instant.parse("2026-01-30T12:00:00Z"));

        ProcessingEvent event02 = new ProcessingEvent();
        event02.setLine(1);
        event02.setStation(2);
        event02.setTimestamp(Instant.parse("2026-01-30T12:10:05Z"));

        ProcessingEvent event03 = new ProcessingEvent();
        event03.setLine(1);
        event03.setStation(2);
        event03.setTimestamp(Instant.parse("2026-01-30T12:09:50Z"));

        testHarness.processElement(event01, event01.getTimestamp().toEpochMilli());
        testHarness.processElement(event02, event02.getTimestamp().toEpochMilli());
        testHarness.processElement(event03, event03.getTimestamp().toEpochMilli());

        // when
        testHarness.processWatermark(Instant.parse("2026-01-30T12:10:01Z").toEpochMilli());
        testHarness.processWatermark(Instant.parse("2026-01-30T12:10:06Z").toEpochMilli());
        testHarness.processWatermark(Instant.parse("2026-01-30T12:20:06Z").toEpochMilli());

        // then
        List<InactivityAlert> inactivityAlerts = testHarness.extractOutputValues();
        assertEquals(1, inactivityAlerts.size());
        assertEquals(event02.getTimestamp().toEpochMilli(), inactivityAlerts.get(0).getTimestamp());
    }

    @Test
    void shouldEmitProperInactivityAlertsWhenWatermarkProgressesSignificantlyAtOnce() throws Exception {
        // given
        ProcessingEvent event01 = new ProcessingEvent();
        event01.setLine(1);
        event01.setStation(2);
        event01.setTimestamp(Instant.parse("2026-01-30T12:00:00Z"));

        ProcessingEvent event02 = new ProcessingEvent();
        event02.setLine(1);
        event02.setStation(2);
        event02.setTimestamp(Instant.parse("2026-01-30T12:10:05Z"));

        ProcessingEvent event03 = new ProcessingEvent();
        event03.setLine(1);
        event03.setStation(2);
        event03.setTimestamp(Instant.parse("2026-01-30T12:20:10Z"));

        testHarness.processElement(event01, event01.getTimestamp().toEpochMilli());
        testHarness.processElement(event02, event02.getTimestamp().toEpochMilli());
        testHarness.processElement(event03, event03.getTimestamp().toEpochMilli());

        // when
        testHarness.processWatermark(Instant.parse("2026-01-30T12:00:00Z").toEpochMilli());
        testHarness.processWatermark(Instant.parse("2026-01-30T12:30:15Z").toEpochMilli());

        // then
        List<InactivityAlert> inactivityAlerts = testHarness.extractOutputValues();
        assertEquals(3, inactivityAlerts.size());
        assertEquals(event01.getTimestamp().toEpochMilli(), inactivityAlerts.get(0).getTimestamp());
        assertEquals(event02.getTimestamp().toEpochMilli(), inactivityAlerts.get(1).getTimestamp());
        assertEquals(event03.getTimestamp().toEpochMilli(), inactivityAlerts.get(2).getTimestamp());
    }

}
