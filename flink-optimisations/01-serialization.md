# flink-optimisations

## Serialization

```
Benchmark                                        Mode  Cnt     Score     Error   Units
SerializationBenchmarks.serializerNonPojo       thrpt   14  1488,472 ±  82,360  ops/ms
SerializationBenchmarks.serializerKryo          thrpt   14  2100,019 ±  79,787  ops/ms
SerializationBenchmarks.serializerSpecificAvro  thrpt   14  4113,332 ± 113,524  ops/ms
SerializationBenchmarks.serializerPojo          thrpt   14  5479,595 ± 450,590  ops/ms
SerializationBenchmarks.serializerFastAvro      thrpt   14  6427,985 ±  75,054  ops/ms
SerializationBenchmarks.serializerRecord        thrpt   14  6799,428 ± 155,529  ops/ms
SerializationBenchmarks.serializerTuple         thrpt   14  7182,882 ± 125,685  ops/ms
```

Is Java Record serialization really faster than POJO serialization? Under the hood, the same serializer (PojoSerializer)
is used.

When measured in isolation, serialization throughput is comparable.

```
Benchmark                                      Mode  Cnt     Score    Error   Units
PojoSerializationBenchmarks.serializerPojo    thrpt   14  2768,116 ± 19,255  ops/ns
PojoSerializationBenchmarks.serializerRecord  thrpt   14  2757,912 ± 19,464  ops/ns
```

The difference becomes apparent when measuring both serialization and deserialization together. Conclusion: Record
deserialization is may be faster.

```
Benchmark                                      Mode  Cnt     Score    Error   Units
PojoSerializationBenchmarks.serdePojo         thrpt   14   840,484 ± 22,301  ops/ns
PojoSerializationBenchmarks.serdeRecord       thrpt   14  1013,893 ± 18,166  ops/ns
```

Note that Java Records are immutable, unlike POJOs.
