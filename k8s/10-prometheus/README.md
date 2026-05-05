# Prometheus

## Install

1. Add Prometheus Helm repository.
    ```bash
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    helm repo update
    ```

2. Create `monitoring` namespace.
    ```bash
    kubectl create namespace monitoring
    ```

3. Install Prometheus chart.
    ```bash
    helm upgrade \
        --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
        --version 83.6.0 \
        -f k8s/10-prometheus/values.yaml \
        -n monitoring
    ```

4. Verify that prometheus and grafana pods are running.
    ```bash
    kubectl get pods -n monitoring
    ```
    ```bash
    NAME                                              READY   STATUS    RESTARTS   AGE
    kube-prometheus-stack-grafana-76d8bd8f55-g9lqr    2/2     Running   0          5m36s
    kube-prometheus-stack-operator-76cb798798-c24wj   1/1     Running   0          5m36s
    prometheus-kube-prometheus-stack-prometheus-0     2/2     Running   0          5m31s
   ```

5. Check if you can access [Grafana UI](http://localhost:8088/login). Credentials: `admin`/`admin`.

6. Apply PodMonitor to start collecting Flink metrics.
    ```bash
    kubectl apply -f k8s/10-prometheus/pod-monitor.yaml
    ```

7. Import the Flink dashboard automatically by creating a ConfigMap from the bundled JSON:
    ```bash
    kubectl create configmap flink-monitoring-dashboard \
      --from-file=flink-monitoring.json=k8s/10-prometheus/flink-monitoring.json \
      --namespace monitoring \
      --dry-run=client -o yaml \
    | kubectl label --local -f - grafana_dashboard=1 -o yaml \
    | kubectl apply -f -
    ```
   The Grafana sidecar watches for ConfigMaps labelled `grafana_dashboard=1` and loads them automatically - no manual import needed.

