"""Optional: run Graphify analysis as a one-off Kubernetes pod (local image, no registry)."""
from __future__ import annotations

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

from k8s_demo_common import DEMO_NAMESPACE, DEMO_SERVICE_ACCOUNT

_ANALYZE_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "250m", "memory": "512Mi"},
    limits={"memory": "2Gi"},
)

with DAG(
    dag_id="graphify_analyze_job",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["demo", "graphify", "kubernetes"],
    doc_md="Manual trigger: clones Airflow and runs Graphify AST analysis inside a pod.",
) as dag:
    KubernetesPodOperator(
        task_id="run_graphify_analysis",
        name="graphify-analyze-once",
        namespace=DEMO_NAMESPACE,
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_pod",
        service_account_name=DEMO_SERVICE_ACCOUNT,
        image="graphify-airflow:local",
        image_pull_policy="Never",
        cmds=["/usr/local/bin/analyze.sh"],
        container_resources=_ANALYZE_RESOURCES,
    )
