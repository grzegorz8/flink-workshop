# Generating minio CRDs

1. Install minio helm chart.
    ```bash
    helm repo add bitnami https://charts.bitnami.com/bitnami
    helm repo update
    ```

2. Deploy minio:
    ```bash
    helm install --create-namespace -n s3 minio bitnami/minio --version 17.0.21 --values k8s/02-minio/values.yaml
    ```

3. Check if minio is up and running.
    ```bash
    kubectl get pods -n s3
    ```
    ```bash
    NAME                            READY   STATUS    RESTARTS   AGE
    minio-b997d8bff-59rpz           1/1     Running   0          3m
    minio-console-ccf99ddc4-2zckk   1/1     Running   0          3m
    ```

4. Open `http://localhost:9090/` in your browser to access minio UI. Login using user `admin` and password `adminpass`.
   The `flink` bucket should already exist.
