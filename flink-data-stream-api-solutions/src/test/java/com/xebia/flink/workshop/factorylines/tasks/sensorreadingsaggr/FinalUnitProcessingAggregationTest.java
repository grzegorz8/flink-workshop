package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr;

import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.EnrichedStationProcessingEvent;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.EnrichedStationProcessingEvent.Statistics;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.StationProcessingEvent;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.UnitSummary;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FinalUnitProcessingAggregationTest {

    private static final int STATION_COUNT = 2;

    private KeyedOneInputStreamOperatorTestHarness<Long, EnrichedStationProcessingEvent, UnitSummary> testHarness;

    @BeforeEach
    public void setupTestHarness() throws Exception {
        FinalUnitProcessingAggregation function = new FinalUnitProcessingAggregation(STATION_COUNT);

        testHarness = new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(function),
                (KeySelector<EnrichedStationProcessingEvent, Long>) e -> e.getStationProcessingEvent().getUnitId(),
                Types.LONG
        );
        testHarness.open();
    }

    @Test
    void shouldAggregateStationProcessingEvents() throws Exception {
        // given
        EnrichedStationProcessingEvent eventL0S0 = new EnrichedStationProcessingEvent(
                new StationProcessingEvent(Instant.parse("2025-01-30T12:00:00.000Z"), Instant.parse("2025-01-30T12:00:05.000Z"), 0, 0, 1L),
                new Statistics(100.0d, 25.0d)
        );
        EnrichedStationProcessingEvent eventL0S1 = new EnrichedStationProcessingEvent(
                new StationProcessingEvent(Instant.parse("2025-01-30T12:00:10.000Z"), Instant.parse("2025-01-30T12:00:15.000Z"), 0, 1, 1L),
                new Statistics(55.0d, 35.0d)
        );

        // when
        testHarness.processElement(eventL0S0, eventL0S0.getStationProcessingEvent().getEndTimestamp().toEpochMilli());
        testHarness.processElement(eventL0S1, eventL0S1.getStationProcessingEvent().getEndTimestamp().toEpochMilli());

        // then
        List<UnitSummary> unitSummaries = testHarness.extractOutputValues();
        assertEquals(1, unitSummaries.size());
        assertEquals(155.0d, unitSummaries.get(0).getEnergyConsumption());
        assertArrayEquals(new Double[]{25.0d, 35.0d}, unitSummaries.get(0).getAvgTemperatures());
    }

}