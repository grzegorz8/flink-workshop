# Flink Workshop

This repository contains exercises and code examples for the Advanced Stream Processing with Flink workshop.

## Modules

- **flink-autoscaler** — exercises for the Flink Kubernetes
  Operator [Autoscaler](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/).
- **flink-common** — shared classes used across multiple modules.
- **flink-data-stream-api** — exercises for
  the [Flink DataStream API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/overview/).
- **flink-data-stream-api-solutions** — reference solutions for the DataStream API exercises.
- **flink-k8s-deployment** — exercises for Kubernetes deployment.
- **flink-optimisations** — exercises and benchmarks focused on Flink performance optimisations.
- **flink-state-processor** — examples of
  the [Flink State Processor API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/libs/state_processor_api/).
- **k8s** — Helm charts and resources for running Flink locally on Minikube.

## Suggested Order

1. flink-data-stream-api
2. flink-k8s-deployment (depends on `k8s/`)
3. flink-optimisations
4. flink-autoscaler (depends on `k8s/`)
5. flink-state-processor

---

## Pre-workshop checklist

To avoid spending workshop time on setup issues, please verify beforehand that all the following prerequisites work.
In case of any issues, please contact `grzegorz.kolakowski@xebia.com`.

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
6. `05-schema-registry`
7. `10-prometheus` - Running all the services alongside several Flink jobs can be resource-intensive, so consider
   uninstalling Prometheus after verification. We will install it again during workshops.

We recommend installing [k9s](https://k9scli.io/) or a similar tool to make it easier to observe Kubernetes resources.
