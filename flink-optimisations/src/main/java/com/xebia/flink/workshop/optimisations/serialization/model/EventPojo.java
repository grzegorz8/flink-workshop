package com.xebia.flink.workshop.optimisations.serialization.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventPojo {

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NestedObject {
        private String stringValue1;
        private String stringValue2;
        private Long longValue1;
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
