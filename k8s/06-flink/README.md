# Installing flink

Recreate minio secret in `default` namespace:

```bash
kubectl get secrets minio -n s3 -o json \
    | jq 'del(.metadata["namespace","creationTimestamp","resourceVersion","selfLink","uid","annotations"])' \
    | kubectl apply -n default -f -
```
