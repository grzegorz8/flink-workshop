package com.xebia.flink.workshop.stateprocessorapi.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StationCount {
    private int line;
    private int station;
    private long unitCount;
}