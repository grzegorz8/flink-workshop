package com.xebia.flink.workshop.factorylines.tasks.energyconsumption;

import com.xebia.flink.workshop.factorylines.model.ProcessingEvent;
import com.xebia.flink.workshop.factorylines.model.SensorReadings;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
class EnrichedProcessingEvent {
    private ProcessingEvent processingEvent;
    private SensorReadings sensorReadings;
}