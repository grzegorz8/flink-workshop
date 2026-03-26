# Prometheus

## Install

```bash
kubectl create namespace monitoring
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack -f 04-prometheus/values.yaml -n monitoring
```

## Collecting Flink jobs' metrics

Apply PodMonitor:

```bash
kubectl apply -f pod-monitor.yaml
```
