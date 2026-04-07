# Schema Registry

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm upgrade --install schema-registry bitnami/schema-registry \
    -f 05-schema-registry/values.yaml -n kafka
```