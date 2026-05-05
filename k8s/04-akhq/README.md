# Generating akhq CRDs

1. Add AKHQ Helm repository.
    ```bash
    helm repo add akhq https://akhq.io/
    ```

2. Deploy AKHQ.
    ```bash
    helm upgrade \
        --install akhq akhq/akhq \
        --version 0.27.0 \
        -f k8s/04-akhq/values.yaml \
        -n kafka
    ```

3. Verify that AKHQ is up and running.
    ```bash
    kubectl get pods -n kafka
    ```
    ```bash
    NAME                                         READY   STATUS    RESTARTS   AGE
    akhq-5f6d6775c6-dhs2s                        1/1     Running   0          1m
    rta-kafka-entity-operator-5fb5475db4-xpsxn   2/2     Running   0          5m
    rta-kafka-rta-kafka-0                        1/1     Running   0          4m
    strimzi-cluster-operator-7d9bbbdf5d-ctbv2    1/1     Running   0          4m
   ```
   Open `http://localhost:8089/ui` in your browser to access AKHQ UI.
