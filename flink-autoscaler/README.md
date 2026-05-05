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

2. Create Kafka topics.
   ```bash
   kubectl apply -f flink-autoscaler/k8s/kafka-topics.yaml
   ```

3. Run `BusyJob`.
    ```bash
    kubectl apply -f flink-autoscaler/k8s/busy-job.yaml
    ```

4. Start `DataGenerator` job.
   ```bash
   kubectl apply -f flink-autoscaler/k8s/events-generator.yaml
   ```

5. Observe `busy-job` deployment and [Flink UI](`http://localhost:8087`). Monitor flink-operator logs. In [AKHQ](
   `http://localhost:8089`) you can observe the number of events in input and output topic.
   You can also check flink-operator logs in order to check autoscaler changes.
    ```text
    2026-04-20 07:08:59,912 o.a.f.a.ScalingMetricCollector [INFO ][flink/busy-job] Metric window is not full until 2026-04-20 07:11:59. 8 samples collected so far
    2026-04-20 07:08:59,932 o.a.f.k.o.r.d.AbstractFlinkResourceReconciler [INFO ][flink/busy-job] Resource fully reconciled, nothing to do...
    2026-04-20 07:10:00,004 o.a.f.a.ScalingMetricCollector [INFO ][flink/busy-job] Metric window is not full until 2026-04-20 07:11:59. 9 samples collected so far
    2026-04-20 07:10:00,041 o.a.f.k.o.r.d.AbstractFlinkResourceReconciler [INFO ][flink/busy-job] Resource fully reconciled, nothing to do...
    2026-04-20 07:11:00,199 o.a.f.a.ScalingMetricCollector [INFO ][flink/busy-job] Metric window is not full until 2026-04-20 07:11:59. 10 samples collected so far
    2026-04-20 07:11:00,224 o.a.f.k.o.r.d.AbstractFlinkResourceReconciler [INFO ][flink/busy-job] Resource fully reconciled, nothing to do...

    2026-04-20 07:12:00,320 o.a.f.k.o.l.AuditUtils         [INFO ][flink/busy-job] >>> Event[Job]       | Info    | SCALINGREPORT   | Scaling execution enabled, begin scaling vertices:{ Vertex ID ea632d67b7d595e5b851708ae9ad79d6 | Parallelism 1 -> 2 | Processing capacity 45.20 -> 65.00 | Target data rate 45.19}{ Vertex ID 0a448493b4782967b150582570326227 | Parallelism 1 -> 2 | Processing capacity 45.22 -> 65.00 | Target data rate 45.22}
    2026-04-20 07:12:00,326 o.a.f.k.o.l.AuditUtils         [INFO ][flink/busy-job] >>> Event[Job]       | Info    | CONFIGURATION RECOMMENDATION | Memory tuning recommends the following configuration (automatic tuning is disabled):
    taskmanager.memory.process.size: 584192801 bytes
    taskmanager.memory.network.max: 2176 kb
    taskmanager.memory.jvm-overhead.fraction: 0.345
    taskmanager.memory.network.min: 2176 kb
    taskmanager.memory.framework.heap.size: 0 bytes
    taskmanager.memory.jvm-metaspace.size: 119287281 bytes
    taskmanager.memory.managed.fraction: 0.0
    Remove the following config entries if present: [taskmanager.memory.flink.size, taskmanager.memory.managed.size, taskmanager.memory.task.heap.size, taskmanager.memory.size]
    2026-04-20 07:12:00,355 o.a.f.k.o.l.AuditUtils         [INFO ][flink/busy-job] >>> Event[Job]       | Info    | SPECCHANGED     | SCALE change(s) detected (Diff: FlinkDeploymentSpec[job.upgradeMode : stateless -> savepoint, flinkConfiguration.pipeline.jobvertex-parallelism-overrides : null -> 9f363b997377bca8297737e982f8f09d:1,0a448493b4782967b150582570326227:2,ea632d67b7d595e5b851708ae9ad79d6:2,bc764cd8ddf7a0cff126f51c16239658:1]), starting reconciliation.
    2026-04-20 07:12:00,355 o.a.f.k.o.r.d.AbstractJobReconciler [INFO ][flink/busy-job] Job is in running state, ready for upgrade with savepoint
    2026-04-20 07:12:00,361 o.a.f.k.o.l.AuditUtils         [INFO ][flink/busy-job] >>> Event[Job]       | Info    | SUSPENDED       | Suspending existing deployment.
    2026-04-20 07:12:00,362 o.a.f.k.o.s.AbstractFlinkService [INFO ][flink/busy-job] Suspending job with savepoint
    ```

6. Modify `records-per-second` parameter in `data-generator` deployment.
    ```yaml
    spec:
      job:
        args:
          - "--records-per-second"
          - "50.0"
    ```

7. Observe flink-operator logs and `busy-job` deployment changes.

### Possible modifications

- Use adaptive scheduler `jobmanager.scheduler: adaptive`. In this scenario, please note that duplicates are written in
  Kafka output topic.
- Decrease or increase catch-up duration `job.autoscaler.catch-up.duration`. The longer the duration, the less
  aggressive scaling up.
