package com.xebia.flink.workshop.optimisations.objectreuse.model;

import lombok.Data;

import java.util.List;

@Data
public class Event {

    @Data
    public static class NestedObject {
        private Long longValue1;
        private String stringValue1;
        private String stringValue2;
    }

    private Long id;
    private Long longValue1;
    private Long longValue2;
    private String stringValue1;
    private String stringValue2;
    private String stringValue3;
    private List<NestedObject> nestedObjectList;
    private Boolean booleanValue1;
    private Boolean booleanValue2;
}
