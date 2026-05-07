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

## Exercise 2: Factory lines - energy consumption aggregation (KeyedCoProcessFunction)

**Goal:** Implement a streaming job that, for each unit, calculates the total energy consumed during assembly.

Possible output message:

```json
{
  "unitId": 1001,
  "line": 1,
  "energyConsumption": 120.4
}
```

### Hint 1 - job overview

You can implement the job in the following way:

1. Two sources: `ProcessingEvents` and `SensorReadings`.
2. Join processing events with sensor readings and calculate required statistics. To this end, station processing events
   and sensor readings should be keyed by `(line, station)`, then connect them and apply your custom
   `KeyedCoProcessFunction`. For each processing event select the closest preceding sensor reading. Output:
   `EnrichedProcessingEvent`.
3. Join corresponding `IN` and `OUT` processing events into one `StationProcessingEvent` using `KeyedProcessFunction`.
   Key the stream by `line, station, unitId` first.
4. Aggregate station processing events to calculate final output. Key the stream by `unitId` and apply custom
   `KeyedProcessFunction`.
5. Print the results on the standard output (`stream.print()`).

### Hint 2 - enrich with sensor readings

- We need to connect two streams, use `KeyedCoProcessFunction`.
- Sensor readings and processing events can come out of order. Therefore, we need to buffer them so that we can then
  join them reliably. In `processElement1()` and `processElement2()`, we only buffer incoming events. The actual join
  logic happens in `onTimer()`.
- Use `MapState<Long, SensorReadings>` to store sensor readings where key is the timestamp of the sensor readings.
- For processing events we can use `MapState<Long, List<ProcessingEvent>>` where the key is the timestamp in
  milliseconds and the value is the list of processing events with that timestamp.
- When `onTimer()` is called, we first match processing events with sensor readings. After that, we clean up buffers by
  removing events older than timer timestamp.

### Hint 3 - joining IN and OUT processing events

- For each `unitId`, we expect exactly two events. Using `KeyedProcessFunction`, you can save the first event in the
  state until the second event arrives. Use `ValueState<ProcessingEvent>`.

### Hint 4 - calculate final output

- For each `unitId`, we expect exactly `S` `EnrichedProcessingEvent`s.
- When station processing event arrives, update state.
- When `count == S`, emit final result.

---

### Late event handling

`ProcessFunction` gives us full control over late event handling. In this case, that means retaining sensor readings in
state for the `allowedLateness` duration and enriching any late processing events directly in `processElement1()`.

```java
@Override
public void processElement1(ProcessingEvent value,
                            KeyedCoProcessFunction<Tuple2<Integer, Integer>, ProcessingEvent, SensorReadings, EnrichedProcessingEvent>.Context ctx,
                            Collector<EnrichedProcessingEvent> out) throws Exception {
    long timestamp = ctx.timestamp();
    long watermark = ctx.timerService().currentWatermark();

    if (timestamp <= watermark) {
        // Late event: emit immediately if within allowed lateness, otherwise drop.
        long latenessThreshold = watermark - allowedLateness.toMillis();
        if (timestamp > latenessThreshold) {
            // Join the event with sensor readings; then emit the result.
        }
        // else: beyond allowed lateness - silently drop.
    } else {
        // On-time event: buffer and wait until the watermark reaches this timestamp.
    }
}
```

---

### Watermark alignment groups

When joining two streams, one source (in our exercise: processing events or sensor readings) may be processed faster
than the other, for example when catching up on a backlog. Because Flink's watermark advances at the pace of the slowest
source, a stateful operator accumulates the events from the faster source. As the state grows, operations like iterating
over MapState become increasingly expensive and overall throughput drops significantly.

Watermark alignment groups address this by capping how far ahead any single source's watermark can run relative to
the others. You enable alignment per source as follows:

```java
WatermarkStrategy
        .<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
        .withWatermarkAlignment("alignment-group-1", Duration.ofSeconds(20), Duration.ofSeconds(1));
```


---

### Possible optimisations

See [State Management optimisations](../../flink-optimisations/04-state-management.md).
