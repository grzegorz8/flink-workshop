package com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr;

import com.xebia.flink.workshop.factorylines.model.SensorReadings;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.EnrichedStationProcessingEvent;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.EnrichedStationProcessingEvent.Statistics;
import com.xebia.flink.workshop.factorylines.tasks.sensorreadingsaggr.model.StationProcessingEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading.Type.ENERGY_CONSUMPTION;
import static com.xebia.flink.workshop.factorylines.model.SensorReadings.SensorReading.Type.TEMPERATURE;
import static java.util.stream.Collectors.toList;

class EnrichWithSensorReadings
        extends KeyedCoProcessFunction<Tuple2<Integer, Integer>, StationProcessingEvent, SensorReadings, EnrichedStationProcessingEvent> {

    private final long sensorsRetentionPeriodMs;

    private transient MapState<Long, List<StationProcessingEvent>> leftBuffer;
    private transient MapState<Long, SensorReadings> rightBuffer;

    public EnrichWithSensorReadings(long sensorsRetentionPeriodMs) {
        this.sensorsRetentionPeriodMs = sensorsRetentionPeriodMs;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        this.leftBuffer = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("left-buffer", Types.LONG, Types.LIST(Types.POJO(StationProcessingEvent.class))));
        this.rightBuffer = getRuntimeContext()
                .getMapState(new MapStateDescriptor<>("right-buffer", Types.LONG, Types.POJO(SensorReadings.class)));
    }

    @Override
    public void processElement1(StationProcessingEvent value,
                                KeyedCoProcessFunction<Tuple2<Integer, Integer>, StationProcessingEvent, SensorReadings, EnrichedStationProcessingEvent>.Context ctx,
                                Collector<EnrichedStationProcessingEvent> out) throws Exception {
        // 1. Buffer processing event.
        Long timestamp = ctx.timestamp();
        List<StationProcessingEvent> leftEvents = leftBuffer.get(timestamp);
        if (leftEvents == null) {
            leftEvents = new ArrayList<>();
        }
        leftEvents.add(value);
        leftBuffer.put(timestamp, leftEvents);
        // 2. Register timer (the event is ready to be processed).
        ctx.timerService().registerEventTimeTimer(timestamp);
    }

    @Override
    public void processElement2(SensorReadings value,
                                KeyedCoProcessFunction<Tuple2<Integer, Integer>, StationProcessingEvent, SensorReadings, EnrichedStationProcessingEvent>.Context ctx,
                                Collector<EnrichedStationProcessingEvent> out) throws Exception {
        // 1. Buffer sensor readings.
        Long timestamp = ctx.timestamp();
        rightBuffer.put(timestamp, value);
        // 2. Register timer (buffer cleanup).
        ctx.timerService().registerEventTimeTimer(timestamp);
    }

    @Override
    public void onTimer(long timestamp,
                        KeyedCoProcessFunction<Tuple2<Integer, Integer>, StationProcessingEvent, SensorReadings, EnrichedStationProcessingEvent>.OnTimerContext ctx,
                        Collector<EnrichedStationProcessingEvent> out) throws Exception {
        // 1. Extract sensor readings from the state and sort them.
        List<SensorReadings> sensorReadings = getSortedSensorReadings();
        // 2. Find events ready to be processed. Match each event with corresponding sensor readings and calculate relevant statistics.
        List<StationProcessingEvent> eventsToEmit = findEventsToEmit(ctx, timestamp);
        for (StationProcessingEvent event : eventsToEmit) {
            List<SensorReadings> matchingSensorReadings = findMatchingSensorReadings(sensorReadings, event.getStartTimestamp(), event.getEndTimestamp());
            Statistics stats = calculateStatistics(matchingSensorReadings);
            out.collect(new EnrichedStationProcessingEvent(event, stats));
        }

        // 3. Clean both buffers from items that are no longer needed.
        cleanUpLeftBuffer(timestamp);
        cleanUpRightBuffer(timestamp);
    }

    private List<SensorReadings> getSortedSensorReadings() throws Exception {
        List<SensorReadings> result = new ArrayList<>();
        for (SensorReadings sr : rightBuffer.values()) {
            result.add(sr);
        }
        result.sort(Comparator.comparing(SensorReadings::getTimestamp));
        return result;
    }

    private List<StationProcessingEvent> findEventsToEmit(KeyedCoProcessFunction<Tuple2<Integer, Integer>, StationProcessingEvent, SensorReadings, EnrichedStationProcessingEvent>.OnTimerContext ctx,
                                                          long currentTimestamp) throws Exception {
        List<StationProcessingEvent> eventsToEmit = new ArrayList<>();
        Iterator<Map.Entry<Long, List<StationProcessingEvent>>> iterator = leftBuffer.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<StationProcessingEvent>> next = iterator.next();
            Long eventTimestamp = next.getKey();
            List<StationProcessingEvent> events = next.getValue();
            if (eventTimestamp <= currentTimestamp) {
                eventsToEmit.addAll(events);
                iterator.remove();
            }
            // If RocksDB is used, we can break if entry.getKey() > currentTimestamp.
        }
        return eventsToEmit;
    }

    private List<SensorReadings> findMatchingSensorReadings(List<SensorReadings> sensorReadings,
                                                            Instant lowerBound,
                                                            Instant upperBound) {
        return sensorReadings.stream()
                // lowerBound <= sensorReadings.timestamp() < upperBound
                .filter(sr -> !sr.getTimestamp().isBefore(lowerBound) && sr.getTimestamp().isBefore(upperBound))
                .collect(toList());
    }

    private Statistics calculateStatistics(List<SensorReadings> matchingSensorReadings) {
        if (matchingSensorReadings.isEmpty()) {
            return new Statistics(null, null);
        } else {
            double energyConsumption = matchingSensorReadings.get(matchingSensorReadings.size() - 1).getSensorReadingsMap().get(ENERGY_CONSUMPTION).getValue()
                    - matchingSensorReadings.get(0).getSensorReadingsMap().get(ENERGY_CONSUMPTION).getValue();

            double avgTemperature = matchingSensorReadings.stream()
                    .mapToDouble(sr -> sr.getSensorReadingsMap().get(TEMPERATURE).getValue())
                    .average()
                    .getAsDouble();

            Statistics stats = new Statistics();
            stats.setEnergyConsumption(energyConsumption);
            stats.setAvgTemperature(avgTemperature);
            return stats;
        }
    }

    private void cleanUpLeftBuffer(long timestamp) throws Exception {
        // Another approach is to leverage the iterators exposed by Flink's MapState interface. The embedded rocksdb
        // state backend stores your state in SST files -- static sorted tables. The iterators provided on MapState
        // delegate to the native rocksdb iterators, and thus yield results in sorted order -- sorted by the serialized
        // keys. Maybe you can exploit this to good advantage. Note that MapState also offers iterators with the hashmap
        // state backend, but in that case the iterator doesn't yield the entries in any particular order.

        Iterator<Map.Entry<Long, List<StationProcessingEvent>>> iterator = leftBuffer.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<StationProcessingEvent>> entry = iterator.next();
            if (entry.getKey() <= timestamp) {
                iterator.remove();
            }
            // If RocksDB is used, we can break if entry.getKey() > timestamp.
        }
    }

    private void cleanUpRightBuffer(long timestamp) throws Exception {
        Iterator<Map.Entry<Long, SensorReadings>> iterator = rightBuffer.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, SensorReadings> entry = iterator.next();
            if (entry.getKey() < timestamp - sensorsRetentionPeriodMs) {
                iterator.remove();
            }
            // If RocksDB is used, we can break if entry.getKey() > timestamp.
        }
    }

}
