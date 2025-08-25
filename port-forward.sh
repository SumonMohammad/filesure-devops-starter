#!/bin/bash
kubectl port-forward svc/worker 9100:9100 -n filesure &
kubectl port-forward svc/prometheus 9090:9090 -n filesure &
kubectl port-forward svc/grafana 3000:3000 -n filesure &  # Adjust if needed
kubectl port-forward svc/mongodb-exporter-prometheus-mongodb-exporter 9216:9216 -n filesure &
kubectl port-forward svc/api 5001:5001 -n filesure &      # Adjust if needed
echo "Port forwards started in background. Press Ctrl+C to stop."
wait