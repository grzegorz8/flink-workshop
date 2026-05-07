# flink-optimisations

## RocksDB - state management

### Lazy deserialization

It is important to understand how the state backend works under the hood. For example, when iterating over `MapState`,
it is crucial to know that:

1) HashMap state is essentially a large Java `HashMap`, and what you get is a reference to an object on the heap.
2) RocksDB keeps data in a binary format. `.entries()` on RocksDB state returns an iterator, where `Map.Entry::getKey()`
   and `Map.Entry::getValue()` **lazily deserialize** the object.

With RocksDB state, there can be a noticeable performance difference depending on where `getValue()` is called.
Compare the two code snippets below. We iterate `rightBuffer` to find the closest object by timestamp.

```java

@Override
public void processElement1(Event event, Context ctx, Collector<JoinedEvent> out) throws Exception {
    Long closestKey = null;
    long minDelta = Long.MAX_VALUE;
    long iterations = 0;

    for (Map.Entry<Long, SensorReading> entry : rightBuffer.entries()) {
        iterations++;
        long delta = Math.abs(event.getTimestamp() - entry.getKey());
        if (delta < minDelta) {
            minDelta = delta;
            closestKey = entry.getKey();
        }
    }

    if (closestKey != null) {
        out.collect(new JoinedEvent(event, rightBuffer.get(closestKey), minDelta, iterations));
    }
}
```

```java

@Override
public void processElement1(Event event, Context ctx, Collector<JoinedEvent> out) throws Exception {
    SensorReading closest = null;
    long minDelta = Long.MAX_VALUE;
    long iterations = 0;

    for (Map.Entry<Long, SensorReading> entry : rightBuffer.entries()) {
        iterations++;
        long delta = Math.abs(event.getTimestamp() - entry.getKey());
        if (delta < minDelta) {
            minDelta = delta;
            closest = entry.getValue();
        }
    }

    if (closest != null) {
        out.collect(new JoinedEvent(event, closest, minDelta, iterations));
    }
}
```

In the first snippet, `getValue()` is called at most once, while in the second snippet it may be called multiple times.

```
Benchmark                                                             Mode  Cnt    Score    Error   Units
IterateMapEntriesBenchmarks.mapEntriesIterationRememberKeyHashmap    thrpt    5  180,643 ± 12,084  ops/ms
IterateMapEntriesBenchmarks.mapEntriesIterationRememberValueHashmap  thrpt    5  176,030 ± 24,263  ops/ms
IterateMapEntriesBenchmarks.mapEntriesIterationRememberKeyRocksDB    thrpt    5   39,388 ±  1,535  ops/ms
IterateMapEntriesBenchmarks.mapEntriesIterationRememberValueRocksDB  thrpt    5   29,123 ±  2,342  ops/ms
```

### Implicit key ordering

Sometimes we want to iterate over `MapState` entries - for example, to remove outdated items (such as entries where
`key < timestamp`) or to find the closest matching object. In general, the `MapState` entries iterator does not
guarantee
any ordering, so we need to iterate over all entries, which can be time-consuming. However, when the RocksDB state
backend is used, entries are ordered by key value (a side effect of RocksDB's implementation - Sorted String Tables).
We can take advantage of this and break the loop as soon as a key exceeds the given timestamp. Compare
[IterateEntriesSortedRememberKeyJoin.java](src/main/java/com/xebia/flink/workshop/optimisations/statemanagement/IterateEntriesSortedRememberKeyJoin.java)
with [IterateEntriesRememberKeyJoin.java](src/main/java/com/xebia/flink/workshop/optimisations/statemanagement/IterateEntriesRememberKeyJoin.java).

```java
private void cleanUpBuffer(long timestamp) throws Exception {
    Iterator<Map.Entry<Long, List<ProcessingEvent>>> iterator = buffer.entries().iterator();
    while (iterator.hasNext()) {
        Map.Entry<Long, List<ProcessingEvent>> entry = iterator.next();
        if (entry.getKey() <= timestamp) {
            iterator.remove();
        } else {
            // If RocksDB is used, we can break if entry.getKey() > timestamp.
            break;
        }
    }
}
```

In this case, the `MapState` iterator delegates to the native RocksDB iterator, so the results are returned in sorted
order.
**Note that this will not work with `HashMap` state, which does not return entries in any particular order.**

`IterateMapEntriesBenchmarks` shows that this technique can bring a noticeable performance improvement (~10% in our
example).

```
Benchmark                                                                   Mode  Cnt    Score    Error   Units
IterateMapEntriesBenchmarks.mapEntriesIterationRememberKeyHashmap          thrpt    5  171,959 ±  8,723  ops/ms
IterateMapEntriesBenchmarks.mapEntriesIterationRememberValueHashmap        thrpt    5  171,109 ± 14,016  ops/ms
IterateMapEntriesBenchmarks.mapEntriesIterationRememberKeyRocksDB          thrpt    5   19,235 ±  0,256  ops/ms
IterateMapEntriesBenchmarks.mapEntriesIterationRememberKeyRocksDBSorted    thrpt    5   21,448 ±  0,368  ops/ms
IterateMapEntriesBenchmarks.mapEntriesIterationRememberValueRocksDB        thrpt    5   13,260 ±  0,497  ops/ms
IterateMapEntriesBenchmarks.mapEntriesIterationRememberValueRocksDBSorted  thrpt    5   14,651 ±  0,686  ops/ms
```
