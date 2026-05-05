# flink-kubernetes-operator

1. Add [flink-kubernetes-operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.13/docs/operations/helm/) Helm repository.
    ```bash
    helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.13.0/
    helm repo update
    ```

2. Create `flink-operator` namespace and install the operator. Create `flink` namespace where Flink jobs will be
   deployed.
    ```bash
    kubectl create namespace flink-operator
    kubectl create namespace flink
    helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
        --version 1.13.0 \
        -n flink-operator \
        -f k8s/01-flink-operator/values.yaml
    kubectl apply -f k8s/01-flink-operator/node-access-rbac.yaml
    ```

3. Verify that the operator is up and running.
    ```bash
    kubectl get pods -n flink-operator
    ```
    ```bash
    NAME                                         READY   STATUS    RESTARTS   AGE
    flink-kubernetes-operator-84c49fdf6f-tppmb   1/1     Running   0          2m
    ```