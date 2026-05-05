package com.xebia.flink.workshop.factorylines.tasks.stationinactivity;

import lombok.Data;

@Data
public class InactivityAlert {
    private long line;
    private long station;
    private Long timestamp;
}
