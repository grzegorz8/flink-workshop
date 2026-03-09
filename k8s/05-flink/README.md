# Installing flink

Recreate minio secret in `default` namespace:

```bash
kubectl get secrets minio -n s3 -o json \
    | jq 'del(.metadata["namespace","creationTimestamp","resourceVersion","selfLink","uid","annotations"])' \
    | kubectl apply -n default -f -
```

Install Flink operator:
```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.13.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator --set webhook.create=false
```
