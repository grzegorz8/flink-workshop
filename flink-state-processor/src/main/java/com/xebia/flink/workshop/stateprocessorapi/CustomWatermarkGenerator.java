package com.xebia.flink.workshop.stateprocessorapi;

import com.xebia.flink.workshop.model.Event;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import java.io.Serializable;
import java.time.Duration;

public class CustomWatermarkGenerator implements WatermarkGenerator<Event>, Serializable {

    private long watermark = Long.MIN_VALUE;

    @Override
    public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
        if (event.getId() % 2 == 0) {
            watermark = Math.max(watermark, event.getTimestamp().toEpochMilli());
        } else {
            watermark = Math.max(watermark, event.getTimestamp().minus(Duration.ofSeconds(10L)).toEpochMilli());
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        if (watermark > Long.MIN_VALUE) {
            output.emitWatermark(new Watermark(watermark));
        }
    }
}

