
#!/bin/bash
if ! kubectl get nodes | grep -q "Ready"; then
  echo "Cluster not ready, restarting..."
  kind delete cluster --name filesure-cluster
  mkdir -p /home/sumon/filesure-data
  kind create cluster --name filesure-cluster --config kubernetes/kind-config.yml
  helm install keda keda/keda --namespace keda --create-namespace --version 2.15.0
  docker build -t filesure-api:latest -f api/Dockerfile .
  docker build -t filesure-worker:latest -f worker/Dockerfile .
  kind load docker-image filesure-api:latest --name filesure-cluster
  kind load docker-image filesure-worker:latest --name filesure-cluster
  kubectl apply -f kubernetes/namespace.yml -f kubernetes/mongodb.yml -f kubernetes/api.yml -f kubernetes/grafana.yml -f kubernetes/prometheus.yml -f prometheus/prometheus.yml -f prometheus/alerts.yml -f kubernetes/mongodb-exporter.yml -f kubernetes/worker-service.yml -f keda/trigger-auth.yaml -f keda/worker-scaledjob.yml -n filesure
  kubectl create secret generic filesure-secrets -n filesure \
    --from-literal=mongo-uri=mongodb://mongodb.filesure.svc.cluster.local:27017 \
    --from-literal=aws-access-key-id=AKIAWODDMODF4TLXKJWO \
    --from-literal=aws-secret-access-key=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 \
    --from-literal=aws-region=ap-south-1 \
    --from-literal=aws-bucket-name=filesure-documents \
    --dry-run=client -o yaml | kubectl apply -f -
  kubectl create secret generic filesure-secrets -n keda \
    --from-literal=mongo-uri=mongodb://mongodb.filesure.svc.cluster.local:27017 \
    --from-literal=aws-access-key-id=AKIAWODDMODF4TLXKJWO \
    --from-literal=aws-secret-access-key=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 \
    --from-literal=aws-region=ap-south-1 \
    --from-literal=aws-bucket-name=filesure-documents \
    --dry-run=client -o yaml | kubectl apply -f -
  ./port-forward.sh &
  sleep 10
  curl -X POST http://localhost:5001/create-job -H "Content-Type: application/json" -d '{"num_jobs": 5}' &
fi
echo "Cluster ready!"
