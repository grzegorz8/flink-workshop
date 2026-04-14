# Flink Workshop

This repository contains exercises and code examples for Advanced Stream Processing with Flink workshop.

## Modules

- **flink-autoscaler** — exercises for the Flink Kubernetes
  Operator [Autoscaler](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/).
- **flink-common** — shared classes used across multiple modules.
- **flink-data-stream-api** — exercises for
  the [Flink DataStream API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/overview/).
- **flink-data-stream-api-solutions** — reference solutions for the DataStream API exercises.
- **flink-optimisations** — exercises and benchmarks focused on Flink performance optimisations.
- **flink-state-processor** — exercises for
  the [Flink State Processor API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/libs/state_processor_api/).
- **k8s** — Helm charts and resources for running Flink locally on Minikube.

## Suggested order

1. flink-data-stream-api
2. flink-optimisations
3. flink-autoscaler (depends on `k8s`)
4. flink-state-processor

---

## Pre-workshop checklist

To avoid spending workshop time on setup issues, please verify that all the following prerequisites work beforehand.

### Maven project

The project should build successfully:

```bash
mvn clean package
```

### Minikube

The Flink Kubernetes Operator exercises require Minikube and several supporting services. Navigate to the `k8s`
directory and install them in the following order:

1. `00-k8s`
2. `01-flink-operator`
3. `02-minio`
4. `03-kafka`
5. `04-akhq`
6. `10-prometheus` — Running all the services above alongside several Flink jobs can be resource-intensive, so consider
   uninstalling Prometheus after verification. We will install it again during workshops.

We recommend installing [k9s](https://k9scli.io/) or a similar tool to make it easier to observe Kubernetes resources.
