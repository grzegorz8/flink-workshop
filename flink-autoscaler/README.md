# flink-autoscaler

In this module we focus on testing and observing flink-kubernetes-operator Autoscaler. To this end, we are going to run
two Flink jobs:

- **EventsGenerator** which produces data at given pace (`com.xebia.flink.workshop.autoscaler.EventsGenerator`),
- **BusyJob** which processes events at constant pace per subtask. The busy-job (
  `com.xebia.flink.workshop.autoscaler.BusyJob`) consists of 4 operators:
   ```
   (Source) -> (some-process-function) -> (another-process-function) -> (Sink)
   ```
  where `some-process-function` processes at most 50 events per second and `another-process-function` up to 100 evens
  per second.

## Scenario

### Base scenario

1. Build `flink-autoscaler` java artifact and deploy it to minio.
   ```bash
   mvn clean deploy -pl flink-common,flink-autoscaler -s .mvn/minio-settings.xml
   ```

2. Run `BusyJob`.
    ```bash
    kubectl apply -f flink-autoscaler/k8s/busy-job.yaml
    ```

3. Start `DataGenerator` job.
   ```bash
   kubectl apply -f flink-autoscaler/k8s/events-generator.yaml
   ```

4. Observe `busy-job` deployment and [Flink UI](`http://localhost:8087`). Monitor flink-operator logs. In [AKHQ](
   `http://localhost:8089`) you can observe the number of events in input and output topic.

5. Modify `records-per-second` parameter in `data-generator` deployment.
    ```yaml
    spec:
      job:
        args:
          - "--records-per-second"
          - "50.0"
    ```

6. Observe flink-operator logs and `busy-job` deployment changes.

### Possible modifications

- Use adaptive scheduler `jobmanager.scheduler: adaptive`. In this scenario, please note that duplicates are written in
  Kafka output topic.
- Decrease or increase catch-up duration `job.autoscaler.catch-up.duration`. The longer the duration, the less
  aggressive scaling up.
