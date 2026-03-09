# Generating minio CRDs

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm install --create-namespace -n s3 minio bitnami/minio --version 17.0.21 --values 02-minio/values.yaml
```
