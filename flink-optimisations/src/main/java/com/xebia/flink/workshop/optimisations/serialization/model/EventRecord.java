package com.xebia.flink.workshop.optimisations.serialization.model;

import java.util.List;

public record EventRecord(Long id,
                          Long longValue1,
                          Long longValue2,
                          String stringValue1,
                          String stringValue2,
                          String stringValue3,
                          List<NestedObject> nestedObjectList,
                          Boolean booleanValue1,
                          Boolean booleanValue2) {
    public record NestedObject(String stringValue1, String stringValue2, Long longValue1) {
    }
}

