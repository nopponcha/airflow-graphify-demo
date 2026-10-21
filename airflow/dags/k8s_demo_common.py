"""Shared KubernetesPodOperator defaults for the minikube demo."""
from __future__ import annotations

from kubernetes.client import models as k8s

DEMO_NAMESPACE = "airflow-demo"
DEMO_SERVICE_ACCOUNT = "airflow"

KPO_DEMO_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"},
    limits={"memory": "256Mi"},
)

KPO_DEMO_KWARGS = {
    "namespace": DEMO_NAMESPACE,
    "in_cluster": True,
    "get_logs": True,
    "on_finish_action": "delete_pod",
    "service_account_name": DEMO_SERVICE_ACCOUNT,
    "container_resources": KPO_DEMO_RESOURCES,
}

GRAPHIFY_ENV_FROM = [
    k8s.V1EnvFromSource(
        config_map_ref=k8s.V1ConfigMapEnvSource(name="airflow-config"),
    ),
]
