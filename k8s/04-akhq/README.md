# Generating akhq CRDs

```bash
helm repo add akhq https://akhq.io/
helm upgrade --install akhq akhq/akhq -f 04-akhq/values.yaml -n kafka
```
