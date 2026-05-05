# Low-level DataStream API

## Introduction to KeyedCoProcessFunction

`KeyedCoProcessFunction` is similar to a `KeyedProcessFunction` but it consumes two data stream inputs.

```java
/**
 * @param <K> Type of the key.
 * @param <IN1> Type of the first input.
 * @param <IN2> Type of the second input.
 * @param <OUT> Output type.
 */
public abstract class KeyedCoProcessFunction<K, IN1, IN2, OUT>
```

```java
public static void main() {
    streamOne
            .keyBy(e -> e.getId())
            .connect(streamTwo.keyBy(e -> e.getId()))
            .process(new MyCoProcessFunction())
            .sinkTo(mySink);
}
```

## Exercise 2: Factory lines - sensor readings aggregation (KeyedCoProcessFunction)

**Goal:** Implement a streaming job that for each unit calculates:

- total energy consumed during assembly,
- the average temperature at each station during unit's assembly.

Possible output message:

```json
{
  "unitId": 1001,
  "line": 1,
  "startTime": "2026-01-30T12:00:00.000Z",
  "endTime": "2026-01-30T12:10:15.000Z",
  "energyConsumption": 120.4,
  "avgTemperature": [
	26.5,
	28.1,
	19.2,
	45.2,
	50.5
  ]
}
```

### Hint 1 - job overview

You can implement the job in the following way:

1. Two sources: `ProcessingEvent`s and `SensorReadings`.
2. Join corresponding `IN` and `OUT` processing events into one `StationProcessingEvent` using `KeyedProcessFunction`.
   Key the stream by `line, station, unitId` first.
3. Join station processing events with sensor readings and calculate required statistics. To this end, first key station
   processing events and sensor readings by `(line, station)`, connect them and apply your custom
   `KeyedCoProcessFunction`. Output: `EnrichedStationProcessingEvent`.
4. Aggregate enriched station processing events to calculate final output. Key the stream by `unitId` and apply custom
   `KeyedProcessFunction`.
5. Print the results on the standard output (`stream.print()`).

```java
public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStream<ProcessingEvent> processingEvents = ...;
    DataStream<SensorReadings> sensorReadings = ...;

    processingEvents
            // join IN and OUT processing events
            .keyBy(e -> Tuple3.of(e.getLine(), e.getStation(), e.getUnitId()), Types.TUPLE(Types.INT, Types.INT, Types.LONG))
            .process(new JoinStartAndFinishEvents())
            // enrich with sensor readings
            .keyBy(e -> Tuple2.of(e.getLine(), e.getStation()), Types.TUPLE(Types.INT, Types.INT))
            .connect(sensorReadings.keyBy(e -> Tuple2.of(e.getLine(), e.getStation()), Types.TUPLE(Types.INT, Types.INT)))
            .process(new EnrichWithSensorReadings(Duration.ofSeconds(60L).toMillis()))
            // calculate final output
            .keyBy(e -> e.getStationProcessingEvent().getUnitId())
            .process(new FinalUnitProcessingAggregation(STATIONS_COUNT))
            .print();

    env.execute();
}
```

### Hint 2 - joining IN and OUT processing events

- For each `unitId` we expect exactly two events. Using `KeyedProcessFunction` you can save the first event in the state
  until the second event arrives. Use `ValueState<ProcessingEvent>`.

### Hint 3 - enrich with sensor readings

- We need to connect two streams, use `KeyedCoProcessFunction`.
- Each unit spends 30-60 seconds on a station. Therefore, we need to keep at least 60 seconds of sensor readings in the
  state. Use `MapState<Long, SensorReadings>` where key is the timestamp of the sensor readings.
- `StationProcessingEvent`s may arrive out of order, so we need to buffer them. Use
  `MapState<Long, List<StationProcessingEvent>>` where key is the timestamp of the station processing event. Note that
  the value is a list - potentially there might be a few events with the same timestamp.
- `processElement1()` and `processElement2()` should only buffer the elements and register timers. The actual join logic
  should be implemented in `onTimer()`.
- When `onTimer()` is called, we do not know if it was scheduled in `processElement1()` or `processElement2()`, so we do
  both actions: emitting results and buffers cleanup.

### Hint 4 - calculate final output

- For each `unitId` we expect exactly `S` `EnrichedStationProcessingEvent`s.
- When enriched station processing event arrives, update state (count, min/max timestamp, energy consumption sum, avg
  temperature measurements).
- When `count == S`, emit final result.

---

### Possible optimisations

#### Timer coalescing

`onTimer()` is called for each registered timestamp. Each `onTimer()` introduces some overhead, especially when it
triggers costly operations such as iterating over `MapState`. For instance, in `EnrichWithSensorReadings` (
`KeyedCoProcessFunction`) we iterate over both sensor readings buffer and station processing events buffer.
Instead of calling `onTimer()` 10 times a second, we can round timer values to full seconds. This way we will have only
one `onTimer()` call instead of 10 times. The overall performance should most likely improve.

```java

@Override
public void processElement1(StationProcessingEvent value,
                            KeyedCoProcessFunction<Tuple2<Integer, Integer>, StationProcessingEvent, SensorReadings, EnrichedStationProcessingEvent>.Context ctx,
                            Collector<EnrichedStationProcessingEvent> out) throws Exception {
    ctx.timerService().registerEventTimeTimer(timestamp);
    // OR coalesce timer
    ctx.timerService().registerEventTimeTimer((timestamp * 1000) / 1000);
}
```
