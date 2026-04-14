# flink-data-stream-api

## Introduction

You work for a manufacturing company that operates L assembly lines, each consisting of S processing stations. All lines
are identical, and each station is responsible for a specific step in the assembly proces. Every unit goes through all
stations on a selected line. Processing a unit at a single station typically takes 30–60 seconds. Each station is
equipped with several sensors that capture metrics such as temperature and energy consumption, with readings collected
every second.

![Task - factory lines overview.png](images/Task%20-%20factory%20lines%20overview.png)

There are two data streams being collected:

- `ProcessingEvents` - a stream of events indicating when a unit enters or leaves a processing station. This means that
  for each unit there are `2S` processing events (`S` IN actions and `S` OUT actions).
- `SensorReadings` - a stream of sensor readings. Typically, there is one event per line per station per second.
