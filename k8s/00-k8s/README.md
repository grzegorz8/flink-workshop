# Minikube

Prerequisites:
- Docker is up and running.

## Install minikube

* Install minikube: https://minikube.sigs.k8s.io/docs/start/

```bash
minikube start \
    --cpus 8 \
    --memory 14g \
    --kubernetes-version v1.32.0 \
    --container-runtime=containerd \
    --extra-config=kubelet.authentication-token-webhook=true \
    --extra-config=kubelet.authorization-mode=Webhook \
    --extra-config=scheduler.bind-address=0.0.0.0 \
    --extra-config=controller-manager.bind-address=0.0.0.0 \
    --extra-config=etcd.listen-metrics-urls=http://0.0.0.0:2381
```

```bash
minikube addons enable metrics-server
```

Run the following command to forward service ports:
```bash
minikube tunnel
```
