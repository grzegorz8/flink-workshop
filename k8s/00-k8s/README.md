# Minikube

Prerequisites:

- Docker is up and running.

## Install minikube

1. Install [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl).
2. Install [minikube](https://minikube.sigs.k8s.io/docs/start/).
3. Start `minikube`. Adjust `cpus` and `memory` to your local machine capacity.
    ```bash
    minikube start \
        --cpus 8 \
        --memory 14g \
        --kubernetes-version v1.35.1 \
        --container-runtime=containerd \
        --extra-config=kubelet.authentication-token-webhook=true \
        --extra-config=kubelet.authorization-mode=Webhook \
        --extra-config=scheduler.bind-address=0.0.0.0 \
        --extra-config=controller-manager.bind-address=0.0.0.0
    ```
4. Verify that minikube is up and running.
   ```bash
   minikube status
   kubectl get nodes
   ```
5. Install [k9s](https://k9scli.io/topics/install/) or any other tool to view and manage Kubernetes clusters.
6. Enable `metrics-server` on minikube.
   ```bash
   minikube addons enable metrics-server
   ```
7. Run in a new terminal the following command to forward service ports:
   ```bash
   minikube tunnel
   ```
