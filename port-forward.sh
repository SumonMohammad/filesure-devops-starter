#!/bin/bash
kubectl port-forward svc/api 5001:5001 -n filesure &
kubectl port-forward svc/prometheus 9090:9090 -n filesure &
kubectl port-forward svc/grafana 3000:3000 -n filesure &
kubectl port-forward svc/mongodb 27017:27017 -n filesure &

echo "Port forwards started in background. Press Ctrl+C to stop."
wait