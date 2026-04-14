# Generating kafka CRDs

1. Create `kafka` namespace.
    ```bash
    kubectl create namespace kafka
    ```

2. Install [strimzi](https://strimzi.io/quickstarts/) operator.
    ```bash
    kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
    ```

3. Create Kafka cluster.
   ```bash
   kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
    ```
4. Verify that the strimzi operator and Kafka cluster are up and running.
    ```bash
    kubectl get pods -n kafka
    ```
    ```bash
   NAME                                         READY   STATUS    RESTARTS   AGE
   rta-kafka-entity-operator-5fb5475db4-xpsxn   2/2     Running   0          4m
   rta-kafka-rta-kafka-0                        1/1     Running   0          3m
   strimzi-cluster-operator-7d9bbbdf5d-ctbv2    1/1     Running   0          3m
   ```
