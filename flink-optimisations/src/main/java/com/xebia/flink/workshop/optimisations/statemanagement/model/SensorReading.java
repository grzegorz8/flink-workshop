package com.xebia.flink.workshop.optimisations.statemanagement.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {
    private int stationId;
    private int lineId;
    private long timestamp;
    private double temperature;
    private double energyConsumption;
    private double pressure;
    private double humidity;
    private String sensorId;
    private double vibration;
    private double voltage;
    private double current;
    private double rpm;
    private double torque;
    private double flowRate;
    private double co2Level;
    private double noiseLevel;
    private double lightIntensity;
    private double pH;
    private double conductivity;
    private double viscosity;
    private double particleCount;
    private double oilLevel;
    private double coolantTemperature;
    private double exhaustTemperature;
    private double loadFactor;
    private double efficiency;
    private int errorCode;
    private int cycleCount;
    private boolean maintenanceFlag;
}