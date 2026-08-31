# Fable Kubernetes deployment

Create the namespace, then create `fable-secret` from `secret.example.yaml` (fill real values locally; never commit it):

```bash
kubectl apply -f namespace.yaml
kubectl create secret generic fable-secret -n fable \
  --from-literal=FABLE_MYSQL_USER=fable \
  --from-literal=FABLE_MYSQL_PASSWORD='...' \
  --from-literal=FABLE_MYSQL_DATABASE=mychat \
  --from-literal=FABLE_REDIS_PASSWORD='...' \
  --from-literal=FABLE_JWT_SECRET='...'
kubectl apply -f infra.yaml -f app.yaml
```
