#!/usr/bin/env bash
set -euo pipefail
kubectl delete namespace airflow-demo --ignore-not-found
# Optional: remove images from minikube docker
if [ "${1:-}" = "--images" ]; then eval "$(minikube docker-env)"; docker rmi graphify-airflow:local airflow-demo:local || true; fi
