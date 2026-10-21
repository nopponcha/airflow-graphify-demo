#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
NS=airflow-demo
kubectl apply -f k8s/00-namespace.yaml -f k8s/10-secrets-config.yaml -f k8s/20-postgres.yaml
kubectl -n $NS rollout status statefulset/postgres --timeout=300s
kubectl -n $NS delete job airflow-init --ignore-not-found
kubectl apply -f k8s/30-airflow-init-job.yaml
kubectl -n $NS wait --for=condition=complete job/airflow-init --timeout=600s
kubectl apply -f k8s/40-airflow.yaml -f k8s/50-graphify.yaml
for d in airflow-api-server airflow-scheduler airflow-dag-processor airflow-triggerer graphify; do
  kubectl -n $NS rollout status deploy/$d --timeout=600s
done
kubectl -n $NS get pods,svc
echo "Airflow UI : $(minikube service -n $NS airflow-api-server --url 2>/dev/null || echo 'kubectl -n airflow-demo port-forward svc/airflow-api-server 8080:8080')"
echo "Graphify   : $(minikube service -n $NS graphify --url 2>/dev/null || echo 'kubectl -n airflow-demo port-forward svc/graphify 8081:8080')"
