# flink-kubernetes-operator

Install flink-kubernetes-operator: https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.13/docs/operations/helm/


```bash
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.13.0/
kubectl create namespace flink
kubectl create namespace flink-operator
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
    -n flink-operator \
    -f 01-flink-operator/values.yaml
```