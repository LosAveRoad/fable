# Fable Kubernetes deployment

MySQL and Redis run on the private infrastructure host; Kafka and Fable run in
Kubernetes. Create the namespace and secret first (fill real values locally;
never commit them):

```bash
kubectl apply -f namespace.yaml
kubectl create secret generic fable-secret -n fable \
  --from-literal=FABLE_MYSQL_HOST=10.0.0.10 \
  --from-literal=FABLE_MYSQL_PORT=3306 \
  --from-literal=FABLE_MYSQL_USER=fable \
  --from-literal=FABLE_MYSQL_PASSWORD='...' \
  --from-literal=FABLE_MYSQL_DATABASE=mychat \
  --from-literal=FABLE_REDIS_HOST=10.0.0.10 \
  --from-literal=FABLE_REDIS_PORT=6379 \
  --from-literal=FABLE_REDIS_PASSWORD='...' \
  --from-literal=FABLE_JWT_SECRET='...'
kubectl -n fable delete deployment kafka --ignore-not-found=true # one-time migration
kubectl -n fable delete deployment fable --ignore-not-found=true # one-time migration
kubectl apply -f infra.yaml
kubectl -n fable wait --for=condition=complete job/kafka-init-topic --timeout=300s
kubectl apply -f app.yaml
```

`infra.yaml` creates Kafka as a StatefulSet and idempotently creates the chat
topic. Fable is also a StatefulSet so each replica keeps a stable Kafka fan-out
consumer identity. Production releases use an immutable commit-SHA image. Do not apply
`secret.example.yaml` directly.
