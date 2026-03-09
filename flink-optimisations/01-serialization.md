# flink-optimisations

## Serialization

```
Benchmark                                        Mode  Cnt     Score     Error   Units
SerializationBenchmarks.serializerNonPojo       thrpt   14  1570,226 ± 46,713  ops/ms
SerializationBenchmarks.serializerKryo          thrpt   14  2332,266 ± 190,776  ops/ms
SerializationBenchmarks.serializerSpecificAvro  thrpt   14  6397,093 ± 300,013  ops/ms
SerializationBenchmarks.serializerPojo          thrpt   14  6407,297 ± 144,839  ops/ms
SerializationBenchmarks.serializerRecord        thrpt   14  7151,081 ± 259,583  ops/ms
SerializationBenchmarks.serializerTuple         thrpt   14  8275,443 ± 257,095  ops/ms
```

Java Record serialization is faster than POJO serialization? Under the hood, the same serializer (PojoSerializer) is
used.

Serialization is comparable.

```
Benchmark                                      Mode  Cnt     Score    Error   Units
PojoSerializationBenchmarks.serializerPojo    thrpt   14  2768,116 ± 19,255  ops/ns
PojoSerializationBenchmarks.serializerRecord  thrpt   14  2757,912 ± 19,464  ops/ns
```

The difference is visible when we compare serialization and deserialization. Corollary: record deserialization is
faster.

```
Benchmark                                      Mode  Cnt     Score    Error   Units
PojoSerializationBenchmarks.serdePojo         thrpt   14   840,484 ± 22,301  ops/ns
PojoSerializationBenchmarks.serdeRecord       thrpt   14  1013,893 ± 18,166  ops/ns
```

However, Java Record is immutable in contrast to POJO.
