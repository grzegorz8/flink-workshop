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


When adaptive scheduler the Job Manager requests for more Tasks Managers. Once the new TMs are stable, JM initiates job
restart.

```
2026-05-08 11:19:20,057 INFO  org.apache.flink.runtime.resourcemanager.slotmanager.FineGrainedSlotManager [] - Received resource requirements from job c810f8e64e8c58b6674e99d270c8af74: [ResourceRequirement{resourceProfile=ResourceProfile{UNKNOWN}, numberOfRequiredSlots=2}]
2026-05-08 11:19:20,119 INFO  org.apache.flink.runtime.resourcemanager.slotmanager.FineGrainedSlotManager [] - Matching resource requirements against available resources.
Missing resources:
	 Job c810f8e64e8c58b6674e99d270c8af74
		ResourceRequirement{resourceProfile=ResourceProfile{UNKNOWN}, numberOfRequiredSlots=1}
Current resources:
	TaskManager busy-job-taskmanager-1-1
		Available: ResourceProfile{cpuCores=0, taskHeapMemory=0 bytes, taskOffHeapMemory=0 bytes, managedMemory=0 bytes, networkMemory=0 bytes}
		Total:     ResourceProfile{cpuCores=0.4, taskHeapMemory=256.000mb (268435456 bytes), taskOffHeapMemory=0 bytes, managedMemory=0 bytes, networkMemory=64.000mb (67108864 bytes)}
2026-05-08 11:19:20,189 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - need request 1 new workers, current worker number 1, declared worker number 2
2026-05-08 11:19:20,190 INFO  org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils [] - The derived from fraction jvm overhead memory (92.444mb (96935027 bytes)) is less than its min value 192.000mb (201326592 bytes), min value will be used instead
2026-05-08 11:19:20,190 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Requesting new worker with resource spec WorkerResourceSpec {cpuCores=0.4, taskHeapSize=256.000mb (268435456 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemSize=0 bytes, numSlots=1}, current pending count: 1.
2026-05-08 11:19:20,191 INFO  org.apache.flink.runtime.externalresource.ExternalResourceUtils [] - Enabled external resources: []
2026-05-08 11:19:20,192 INFO  org.apache.flink.configuration.Configuration                 [] - Config uses fallback configuration key 'kubernetes.service-account' instead of key 'kubernetes.taskmanager.service-account'
2026-05-08 11:19:20,193 INFO  org.apache.flink.kubernetes.KubernetesResourceManagerDriver  [] - Creating new TaskManager pod with name busy-job-taskmanager-1-2 and resource <1024,0.4>.
2026-05-08 11:19:20,232 INFO  org.apache.flink.kubernetes.KubernetesResourceManagerDriver  [] - Pod busy-job-taskmanager-1-2 is created.
2026-05-08 11:19:20,234 INFO  org.apache.flink.kubernetes.KubernetesResourceManagerDriver  [] - Received new TaskManager pod: busy-job-taskmanager-1-2
2026-05-08 11:19:20,234 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Requested worker busy-job-taskmanager-1-2 with resource spec WorkerResourceSpec {cpuCores=0.4, taskHeapSize=256.000mb (268435456 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemSize=0 bytes, numSlots=1}.
2026-05-08 11:19:39,403 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Registering TaskManager with ResourceID busy-job-taskmanager-1-2 (pekko.tcp://flink@10.244.0.16:6122/user/rpc/taskmanager_0) at ResourceManager
2026-05-08 11:19:39,492 INFO  org.apache.flink.runtime.resourcemanager.slotmanager.FineGrainedSlotManager [] - Registering task executor busy-job-taskmanager-1-2 under 30b3b8665636ba818e8d1fe614cdc0b8 at the slot manager.
2026-05-08 11:19:39,493 INFO  org.apache.flink.runtime.resourcemanager.slotmanager.DefaultSlotStatusSyncer [] - Starting allocation of slot f7b809b00777ea1f1efa12e36fcee4ad from busy-job-taskmanager-1-2 for job c810f8e64e8c58b6674e99d270c8af74 with resource profile ResourceProfile{cpuCores=0.4, taskHeapMemory=256.000mb (268435456 bytes), taskOffHeapMemory=0 bytes, managedMemory=0 bytes, networkMemory=64.000mb (67108864 bytes)}.
2026-05-08 11:19:39,493 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Worker busy-job-taskmanager-1-2 is registered.
2026-05-08 11:19:39,494 INFO  org.apache.flink.runtime.resourcemanager.active.ActiveResourceManager [] - Worker busy-job-taskmanager-1-2 with resource spec WorkerResourceSpec {cpuCores=0.4, taskHeapSize=256.000mb (268435456 bytes), taskOffHeapSize=0 bytes, networkMemSize=64.000mb (67108864 bytes), managedMemSize=0 bytes, numSlots=1} was requested in current attempt. Current pending count after registering: 0.
2026-05-08 11:19:39,785 INFO  org.apache.flink.runtime.scheduler.adaptive.DefaultStateTransitionManager [] - Transitioning from Idling to Stabilizing, job c810f8e64e8c58b6674e99d270c8af74.
2026-05-08 11:20:26,123 INFO  org.apache.flink.runtime.scheduler.adaptive.DefaultStateTransitionManager [] - Desired resources are met, transitioning to the subsequent state, job c810f8e64e8c58b6674e99d270c8af74.
2026-05-08 11:20:26,123 INFO  org.apache.flink.runtime.scheduler.adaptive.DefaultStateTransitionManager [] - Transitioning from Stabilizing to Transitioning, job c810f8e64e8c58b6674e99d270c8af74.
2026-05-08 11:20:26,126 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Job Busy job (c810f8e64e8c58b6674e99d270c8af74) switched from state RUNNING to CANCELLING.
2026-05-08 11:20:26,127 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - Source: events (1/1) (70d1da2bc5593268133b3eddee5870b2_bc764cd8ddf7a0cff126f51c16239658_0_0) switched from RUNNING to CANCELING.
2026-05-08 11:20:26,127 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - KeyedProcess (1/1) (70d1da2bc5593268133b3eddee5870b2_0a448493b4782967b150582570326227_0_0) switched from RUNNING to CANCELING.
2026-05-08 11:20:26,130 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - some-process-function (1/1) (70d1da2bc5593268133b3eddee5870b2_ea632d67b7d595e5b851708ae9ad79d6_0_0) switched from RUNNING to CANCELING.
2026-05-08 11:20:26,130 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph       [] - another-process-function -> Sink: Writer -> Sink: Committer (1/1) (70d1da2bc5593268133b3eddee5870b2_9f363b997377bca8297737e982f8f09d_0_0) switched from RUNNING to CANCELING.
```