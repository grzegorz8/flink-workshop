# Generating kafka CRDs

1. Create `kafka` namespace.
    ```bash
    kubectl create namespace kafka
    ```

2. Add [strimzi](https://strimzi.io/quickstarts/) Helm repository.
    ```bash
    helm repo add strimzi https://strimzi.io/charts/
    helm repo update
    ```

3. Install strimzi operator.
    ```bash
    helm install strimzi-operator strimzi/strimzi-kafka-operator \
    --version 0.51.0 \
    --namespace kafka \
    --create-namespace
    ```

4. Create Kafka cluster.
   ```bash
   kubectl apply -f k8s/03-kafka/kafka-cluster.yaml
    ```

5. Verify that the strimzi operator and Kafka cluster are up and running.
    ```bash
    kubectl get pods -n kafka
    ```
    ```bash
   NAME                                         READY   STATUS    RESTARTS   AGE
   rta-kafka-entity-operator-5fb5475db4-xpsxn   2/2     Running   0          4m
   rta-kafka-rta-kafka-0                        1/1     Running   0          3m
   strimzi-cluster-operator-7d9bbbdf5d-ctbv2    1/1     Running   0          3m
   ```

Kafka is also exposed externally (outside the cluster). The cluster has an `external` listener on port `9094` of type
`LoadBalancer`. With `minikube tunnel` running, Strimzi creates a `LoadBalancer` service that gets an external IP assigned.
