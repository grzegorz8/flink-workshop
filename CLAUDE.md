# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Flink workshop by Xebia — an educational multi-module Maven project teaching Flink stream processing through a
manufacturing factory scenario. Exercises progress through 4 levels: DataStream API → Optimizations → Autoscaler (K8s) →
State Processor API.

## Build & Test Commands

```bash
# Build all modules
mvn clean package

# Build specific module(s)
mvn clean package -pl flink-common,flink-data-stream-api

# Run all tests
mvn test

# Run tests for a specific module
mvn test -pl flink-data-stream-api

# Run a single test class
mvn test -pl flink-data-stream-api -Dtest=SomeTestClass

# Run JMH benchmarks (flink-optimisations)
java -jar flink-optimisations/target/benchmarks.jar
```

**IDE Profile**: Add `-P IDE` to include Flink `provided`-scoped dependencies for IDE support:

```bash
mvn package -P IDE
```

## Architecture

### Module Structure

| Module                            | Purpose                                                                     |
|-----------------------------------|-----------------------------------------------------------------------------|
| `flink-common`                    | Shared data models (`Event`, `SensorReading`), JSON serialization utilities |
| `flink-data-stream-api`           | Level 1 exercises — DataStream API (high & low level)                       |
| `flink-data-stream-api-solutions` | Reference solutions for Level 1 exercises                                   |
| `flink-optimisations`             | Level 2 — JMH benchmarks for serialization, object reuse, reinterpret       |
| `flink-autoscaler`                | Level 3 — Kubernetes Operator autoscaler test job (`BusyJob`)               |
| `flink-datagen`                   | Kafka event generator used with autoscaler (`--records-per-second` flag)    |
| `flink-state-processor`           | Level 4 exercises — Flink State Processor API with RocksDB                  |

### Domain Model

The factory scenario: multiple **assembly lines**, each with **stations**. Two event streams:

- `ProcessingEvent` — units entering/leaving stations
- `SensorReading` — temperature and energy consumption metrics

### Key Patterns

- **Exercise modules** contain `TODO`-marked classes; corresponding solutions live in `flink-data-stream-api-solutions`.
- Fat JARs (via `maven-shade-plugin`) are built for `flink-autoscaler`, `flink-datagen`, and `flink-optimisations` for
  deployment/benchmarking.
- Avro schema compilation is used in `flink-optimisations` via `avro-maven-plugin`.

### Kubernetes Infrastructure (`k8s/`)

Minikube-based setup with Helm-deployed components:

- Flink Kubernetes Operator, MinIO (S3), Kafka, AKHQ (Kafka UI), Prometheus
- Job manifests: `busy-job-deployment.yaml`, `data-generator-deployment.yaml`, session cluster

## Tech Stack

- **Java 17**, **Apache Flink 2.2.0**, Scala binary 2.12
- **Kafka connector** 4.0.1-2.0, **Avro** 1.12.1
- **JUnit 5**, **JMH** 1.37, **Lombok**, **Jackson**

## Exercise Docs

- `flink-data-stream-api/docs/` — exercise instructions with diagrams
- `flink-optimisations/*.md` — optimization exercise guides
