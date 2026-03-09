package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
class UnitSummary {
    private long unitId;
    private Double energyConsumption;
}
