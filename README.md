# Flink Workshop

**Modules**:

- **flink-autoscaler** - contains exercises related to
  Flink Kubernetes
  Operator's [Autoscaler](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/autoscaler/)
  feature.
- **flink-common** - contains classes shared across multiple modules.
- **flink-data-stream-api** - contains exercises related to
  the [Flink DataStream API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/overview/).
- **flink-data-stream-api-solutions** - contains sample solutions for exercises.
- **flink-datagen** - contains definition of a job that produces events at given, constant rate, used. e.g. in
  autoscaler tests.
- **flink-optimisations** - contains exercises and benchmarks related to flink optimisations.
- **flink-serialization** - TO REMOVE
- **flink-state-processor** - contains exercises related to
  the [Flink State Processor API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/libs/state_processor_api/).
- **k8s** - contains all Helm charts and other resources needed to run Flink locally on Minikube.

**Suggested order**:

1. flink-data-stream-api,
2. flink-optimisations,
3. flink-autoscaler (depends on `k8s`),
4. flink-state-processor.

---

## Minikube

In order to run flink-kubernetes-operator exercises, you need to install minikube and then all relevant services. Go to
`k8s` directory and start installing them in suggested order:

1. `00-k8s`
2. `01-flink-operator`
3. `02-minio`
4. `03-kafka`
5. `04-akhq`
6. `05-flink`
7. `10-prometheus` - It may occur that running all services above and a few flink jobs simultaneously may consume too
   many resources locally, so installing Prometheus is suggested later, when we will focus on monitoring aspects.

We suggest to install `k9s` or similar tool, which makes it easier to observe kubernetes.