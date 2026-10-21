#!/usr/bin/env bash
# Build images directly into minikube's Docker daemon (no registry needed).
set -euo pipefail
cd "$(dirname "$0")/.."
eval "$(minikube docker-env)"
docker build -t graphify-airflow:local graphify/
docker build -t airflow-demo:local airflow/
docker images | grep -E 'graphify-airflow|airflow-demo'
