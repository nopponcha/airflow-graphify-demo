"""Pull Graphify stats from the in-cluster service via KubernetesPodOperator tasks."""
from __future__ import annotations

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

from k8s_demo_common import GRAPHIFY_ENV_FROM, KPO_DEMO_KWARGS

_REPORT_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "100m", "memory": "128Mi"},
    limits={"memory": "512Mi"},
)

with DAG(
    dag_id="graphify_code_report",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["demo", "graphify", "kubernetes"],
) as dag:
    fetch_stats = KubernetesPodOperator(
        task_id="fetch_stats",
        name="graphify-fetch-stats",
        image="python:3.12-slim",
        env_from=GRAPHIFY_ENV_FROM,
        cmds=["python3", "-c"],
        arguments=[
            """
import json, os, urllib.request
base = os.environ["GRAPHIFY_URL"].rstrip("/")
with urllib.request.urlopen(f"{base}/stats.json", timeout=30) as r:
    stats = json.loads(r.read().decode())
with open("/airflow/xcom/return.json", "w", encoding="utf-8") as f:
    json.dump(stats, f)
""".strip(),
        ],
        do_xcom_push=True,
        **KPO_DEMO_KWARGS,
    )

    fetch_insights = KubernetesPodOperator(
        task_id="fetch_insights",
        name="graphify-fetch-insights",
        image="python:3.12-slim",
        env_from=GRAPHIFY_ENV_FROM,
        cmds=["python3", "-c"],
        arguments=[
            r"""
import json, os, re, urllib.request
base = os.environ["GRAPHIFY_URL"].rstrip("/")
with urllib.request.urlopen(f"{base}/GRAPH_REPORT.md", timeout=30) as r:
    report = r.read().decode()
god_sec = report.split("## God Nodes", 1)[-1].split("\n## ", 1)[0]
gods = re.findall(r"^\d+\. `([^`]+)` - (\d+) edges", god_sec, flags=re.M)[:10]
cycle_sec = report.split("## Import Cycles", 1)[-1].split("\n## ", 1)[0]
cycles = sum(1 for line in cycle_sec.splitlines() if "cycle:" in line)
payload = {"gods": gods, "cycles": cycles}
with open("/airflow/xcom/return.json", "w", encoding="utf-8") as f:
    json.dump(payload, f)
""".strip(),
        ],
        do_xcom_push=True,
        **KPO_DEMO_KWARGS,
    )

    report = KubernetesPodOperator(
        task_id="report",
        name="graphify-report",
        image="python:3.12-slim",
        cmds=["python3", "-c"],
        arguments=[
            """
import json, os
stats = json.loads(os.environ["STATS_JSON"])
insights = json.loads(os.environ["INSIGHTS_JSON"])
print("=== Graphify x Apache Airflow ===")
print(f"Commit: {stats.get('commit')}  built: {stats.get('built_at')}")
print(f"Nodes: {stats.get('nodes'):,}  Edges: {stats.get('edges'):,}")
print(f"Import cycles found: {insights.get('cycles')}")
for i, (name, n) in enumerate(insights.get("gods", []), 1):
    print(f"{i:2}. {name} ({n} edges)")
""".strip(),
        ],
        env_vars={
            "STATS_JSON": "{{ ti.xcom_pull(task_ids='fetch_stats') | tojson }}",
            "INSIGHTS_JSON": "{{ ti.xcom_pull(task_ids='fetch_insights') | tojson }}",
        },
        container_resources=_REPORT_RESOURCES,
        **{k: v for k, v in KPO_DEMO_KWARGS.items() if k != "container_resources"},
    )

    [fetch_stats, fetch_insights] >> report
