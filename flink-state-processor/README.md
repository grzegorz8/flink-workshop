# flink-state-processor-api

In this module we will perform non-backward-compatible upgrade, namely, we will change state type.

1. Run Flink Job `ProcessingEventDatagen` which generates constant stream of `ProcessingEvent`s.
    ```bash
    kubectl apply -f k8s/processing-event-datagen.yaml
    ```
2. Run Flink Job `ProcessingEventJobV1` which generates count of processed units on each station.
    ```bash
    kubectl apply -f k8s/processing-event-job-v1.yaml
    ```
3. Wait a few minutes until the job outputs some initial results. Then stop the job by changing `job.state: suspended`.
4. Download the latest savepoint.
5. Run [PrintSavepointState.java](src/main/java/com/xebia/flink/workshop/stateprocessorapi/PrintSavepointState.java).
6. Run [ProcessingEventStateMigration.java](src/main/java/com/xebia/flink/workshop/stateprocessorapi/ProcessingEventStateMigration.java).
7. Upload generated savepoint.
8. Run [ProcessingEventJobV2.java](src/main/java/com/xebia/flink/workshop/stateprocessorapi/ProcessingEventJobV2.java).