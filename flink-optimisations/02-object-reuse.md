# flink-optimisations

## Object reuse

### Scenario 02

Below you can find a stacktrace from `BarMapFunction` with object reuse **disabled**. Note that functions within the
same Flink task are chained with `CopyingChainingOutput`.

```
--- BarFunction ---
at com.xebia.flink.workshop.optimisations.objectreuse.ObjectReuse$BarMapFunction.map(ObjectReuse.java:98)
-------------------
at com.xebia.flink.workshop.optimisations.objectreuse.ObjectReuse$BarMapFunction.map(ObjectReuse.java:95)
at org.apache.flink.streaming.api.operators.StreamMap.processElement(StreamMap.java:37)
at org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$1243/0x0000000800779d08.accept(Unknown Source:-1)
at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.pushToOperator(CopyingChainingOutput.java:75)
at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.collect(CopyingChainingOutput.java:50)
at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collectAndCheckIfChained(ChainingOutput.java:91)
at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collectAndCheckIfChained(ChainingOutput.java:41)
at org.apache.flink.streaming.runtime.tasks.BroadcastingOutputCollector.collect(BroadcastingOutputCollector.java:84)
at org.apache.flink.streaming.runtime.tasks.BroadcastingOutputCollector.collect(BroadcastingOutputCollector.java:35)
at org.apache.flink.streaming.api.operators.StreamMap.processElement(StreamMap.java:37)
--- FooFunction ---
at org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$1243/0x0000000800779d08.accept(Unknown Source:-1)
-------------------
at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.pushToOperator(CopyingChainingOutput.java:75)
at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.collect(CopyingChainingOutput.java:50)
at org.apache.flink.streaming.runtime.tasks.CopyingChainingOutput.collect(CopyingChainingOutput.java:29)
at org.apache.flink.streaming.api.operators.TimestampedCollector.collect(TimestampedCollector.java:53)
-------- SomeProcessFunction --------
at com.xebia.flink.workshop.optimisations.objectreuse.ObjectReuse$SomeProcessFunction.processElement(ObjectReuse.java:80)
-------------------------------------
at com.xebia.flink.workshop.optimisations.objectreuse.ObjectReuse$SomeProcessFunction.processElement(ObjectReuse.java:74)
at org.apache.flink.streaming.api.operators.KeyedProcessOperator.processElement(KeyedProcessOperator.java:87)
at org.apache.flink.streaming.runtime.io.RecordProcessorUtils.lambda$getRecordProcessor$0(RecordProcessorUtils.java:64)
at org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$1273/0x000000080078c960.accept(Unknown Source:-1)
at org.apache.flink.streaming.runtime.tasks.OneInputStreamTask$StreamTaskNetworkOutput.emitRecord(OneInputStreamTask.java:245)
at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.processElement(AbstractStreamTaskNetworkInput.java:206)
at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.emitNext(AbstractStreamTaskNetworkInput.java:163)
at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65)
at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:638)
at org.apache.flink.streaming.runtime.tasks.StreamTask$$Lambda$1193/0x0000000800760600.runDefaultAction(Unknown Source:-1)
at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:231)
at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:980)
at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:917)
at org.apache.flink.runtime.taskmanager.Task$$Lambda$1351/0x00000008007bdad8.run(Unknown Source:-1)
at org.apache.flink.runtime.taskmanager.Task.runWithSystemExitMonitoring(Task.java:963)
at org.apache.flink.runtime.taskmanager.Task.restoreAndInvoke(Task.java:942)
at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:756)
at org.apache.flink.runtime.taskmanager.Task.run(Task.java:568)
at java.lang.Thread.run(Thread.java:840)
```

`CopyingChainingOutput` implementation:
```java
@Override
    protected <X> void pushToOperator(StreamRecord<X> record) {
    try {
        StreamRecord<T> castRecord = (StreamRecord<T>) record;

        numRecordsOut.inc();
        numRecordsIn.inc();
        StreamRecord<T> copy = castRecord.copy(serializer.copy(castRecord.getValue()));
        recordProcessor.accept(copy);
    } catch (ClassCastException e) {
        // ...
    }
}
```

---

Below you can find a stacktrace from `BarMapFunction` with object reuse **enabled**. Note that functions within the same
Flink task are chained with `ChainingOutput`.

```
--- BarFunction ---
at com.xebia.flink.workshop.optimisations.objectreuse.ObjectReuse$BarMapFunction.map(ObjectReuse.java:98)
-------------------
at com.xebia.flink.workshop.optimisations.objectreuse.ObjectReuse$BarMapFunction.map(ObjectReuse.java:95)
at org.apache.flink.streaming.api.operators.StreamMap.processElement(StreamMap.java:37)
at org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$1249/0x0000000800776e18.accept(Unknown Source:-1)
at org.apache.flink.streaming.runtime.tasks.ChainingOutput.pushToOperator(ChainingOutput.java:110)
at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collect(ChainingOutput.java:79)
at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collectAndCheckIfChained(ChainingOutput.java:91)
at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collectAndCheckIfChained(ChainingOutput.java:41)
at org.apache.flink.streaming.runtime.tasks.CopyingBroadcastingOutputCollector.collect(CopyingBroadcastingOutputCollector.java:43)
at org.apache.flink.streaming.runtime.tasks.CopyingBroadcastingOutputCollector.collect(CopyingBroadcastingOutputCollector.java:28)
at org.apache.flink.streaming.api.operators.StreamMap.processElement(StreamMap.java:37)
--- FooFunction ---
at org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$1249/0x0000000800776e18.accept(Unknown Source:-1)
-------------------
at org.apache.flink.streaming.runtime.tasks.ChainingOutput.pushToOperator(ChainingOutput.java:110)
at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collect(ChainingOutput.java:79)
at org.apache.flink.streaming.runtime.tasks.ChainingOutput.collect(ChainingOutput.java:41)
at org.apache.flink.streaming.api.operators.TimestampedCollector.collect(TimestampedCollector.java:53)
-------- SomeProcessFunction --------
at com.xebia.flink.workshop.optimisations.objectreuse.ObjectReuse$SomeProcessFunction.processElement(ObjectReuse.java:80)
-------------------------------------
at com.xebia.flink.workshop.optimisations.objectreuse.ObjectReuse$SomeProcessFunction.processElement(ObjectReuse.java:74)
at org.apache.flink.streaming.api.operators.KeyedProcessOperator.processElement(KeyedProcessOperator.java:87)
at org.apache.flink.streaming.runtime.io.RecordProcessorUtils.lambda$getRecordProcessor$0(RecordProcessorUtils.java:64)
at org.apache.flink.streaming.runtime.io.RecordProcessorUtils$$Lambda$1279/0x00000008007892a0.accept(Unknown Source:-1)
at org.apache.flink.streaming.runtime.tasks.OneInputStreamTask$StreamTaskNetworkOutput.emitRecord(OneInputStreamTask.java:245)
at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.processElement(AbstractStreamTaskNetworkInput.java:206)
at org.apache.flink.streaming.runtime.io.AbstractStreamTaskNetworkInput.emitNext(AbstractStreamTaskNetworkInput.java:163)
at org.apache.flink.streaming.runtime.io.StreamOneInputProcessor.processInput(StreamOneInputProcessor.java:65)
at org.apache.flink.streaming.runtime.tasks.StreamTask.processInput(StreamTask.java:638)
at org.apache.flink.streaming.runtime.tasks.StreamTask$$Lambda$1199/0x000000080075f380.runDefaultAction(Unknown Source:-1)
at org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor.runMailboxLoop(MailboxProcessor.java:231)
at org.apache.flink.streaming.runtime.tasks.StreamTask.runMailboxLoop(StreamTask.java:980)
at org.apache.flink.streaming.runtime.tasks.StreamTask.invoke(StreamTask.java:917)
at org.apache.flink.runtime.taskmanager.Task$$Lambda$1355/0x00000008007b3b00.run(Unknown Source:-1)
at org.apache.flink.runtime.taskmanager.Task.runWithSystemExitMonitoring(Task.java:963)
at org.apache.flink.runtime.taskmanager.Task.restoreAndInvoke(Task.java:942)
at org.apache.flink.runtime.taskmanager.Task.doRun(Task.java:756)
at org.apache.flink.runtime.taskmanager.Task.run(Task.java:568)
at java.lang.Thread.run(Thread.java:840)
```

`ChainingOutput` implementation:
```java
protected <X> void pushToOperator(StreamRecord<X> record) {
    try {
        // we know that the given outputTag matches our OutputTag so the record
        // must be of the type that our operator expects.
        StreamRecord<T> castRecord = (StreamRecord<T>) record;

        numRecordsOut.inc();
        numRecordsIn.inc();
        recordProcessor.accept(castRecord);
    } catch (Exception e) {
        throw new ExceptionInChainedOperatorException(e);
    }
}
```


```
Benchmark                                       Mode  Cnt    Score    Error   Units
ObjectReuseBenchmarks.withObjectReuseDisabled  thrpt    5  264,029 ±  5,353  ops/ms
ObjectReuseBenchmarks.withObjectReuseEnabled   thrpt    5  322,082 ± 14,128  ops/ms
```