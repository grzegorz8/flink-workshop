package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.ProcessingEvent.Action;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.StationProcessingEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JoinStartAndFinishEventsTest {

    private KeyedOneInputStreamOperatorTestHarness<Tuple3<Integer, Integer, Long>, ProcessingEvent, StationProcessingEvent> testHarness;

    @BeforeEach
    public void setupTestHarness() throws Exception {
        JoinStartAndFinishEvents function = new JoinStartAndFinishEvents();

        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(function),
                (KeySelector<ProcessingEvent, Tuple3<Integer, Integer, Long>>) e -> Tuple3.of(e.getLine(), e.getStation(), e.getUnitId()),
                Types.TUPLE(Types.INT, Types.INT, Types.LONG)
        );
        testHarness.open();
    }

    @Test
    void shouldMatchProcessingEventsByLineStationUnitId() throws Exception {
        // given
        // Unit: 1, L0S0 + L0S1; L0S1 events come out of order
        ProcessingEvent event01L0S0In = new ProcessingEvent(Instant.parse("2025-01-30T12:00:00.000Z"), 0, 0, 1L, Action.IN);
        ProcessingEvent event01L0S0Out = new ProcessingEvent(Instant.parse("2025-01-30T12:00:05.000Z"), 0, 0, 1L, Action.OUT);
        ProcessingEvent event01L0S1Out = new ProcessingEvent(Instant.parse("2025-01-30T12:00:15.000Z"), 0, 1, 1L, Action.OUT);
        ProcessingEvent event01L0S1In = new ProcessingEvent(Instant.parse("2025-01-30T12:00:10.000Z"), 0, 1, 1L, Action.IN);

        testHarness.processElement(event01L0S0In, event01L0S0In.getTimestamp().toEpochMilli());
        testHarness.processElement(event01L0S0Out, event01L0S0Out.getTimestamp().toEpochMilli());
        testHarness.processElement(event01L0S1Out, event01L0S1Out.getTimestamp().toEpochMilli());
        testHarness.processElement(event01L0S1In, event01L0S1In.getTimestamp().toEpochMilli());

        // when
        List<StationProcessingEvent> output = testHarness.extractOutputValues();

        // then
        assertEquals(2, output.size());
        assertEquals(
                new StationProcessingEvent(Instant.parse("2025-01-30T12:00:00.000Z"), Instant.parse("2025-01-30T12:00:05.000Z"), 0, 0, 1L),
                output.get(0)
        );
        assertEquals(
                new StationProcessingEvent(Instant.parse("2025-01-30T12:00:10.000Z"), Instant.parse("2025-01-30T12:00:15.000Z"), 0, 1, 1L),
                output.get(1)
        );

    }

}