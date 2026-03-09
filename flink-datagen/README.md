# flink-datagen

This module contains a Flink streaming job writing events to a Kafka topic at given pace (`records-per-second`).

## Usage

```bash
kubectl apply -f k8s/05-flink/data-generator-deployment.yaml
```

Modify `records-per-second` parameter in `data-generator` deployment to increase or decrease output rate and then
reapply FlinkDeployment.

```yaml
spec:
  job:
    args:
      - "--records-per-second"
      - "50.0"
```