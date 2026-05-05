package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
class UnitSummary {
    private long unitId;
    private long line;
    private Double energyConsumption;
}
