"""Simple ETL demo: extract -> transform -> load as Kubernetes pods (KubernetesPodOperator)."""
from __future__ import annotations

import pendulum
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG

from k8s_demo_common import KPO_DEMO_KWARGS

with DAG(
    dag_id="demo_simple_etl",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["demo", "etl", "kubernetes"],
) as dag:
    extract = KubernetesPodOperator(
        task_id="extract",
        name="demo-etl-extract",
        image="python:3.12-slim",
        cmds=["python3", "-c"],
        arguments=[
            """
import json
rows = [
    {"region": "BKK", "sales": 1200},
    {"region": "CNX", "sales": 640},
    {"region": "HKT", "sales": 910},
]
with open("/airflow/xcom/return.json", "w", encoding="utf-8") as f:
    json.dump(rows, f)
""".strip(),
        ],
        do_xcom_push=True,
        **KPO_DEMO_KWARGS,
    )

    transform = KubernetesPodOperator(
        task_id="transform",
        name="demo-etl-transform",
        image="python:3.12-slim",
        cmds=["python3", "-c"],
        arguments=[
            """
import json, os
rows = json.loads(os.environ["ROWS_JSON"])
total = sum(r["sales"] for r in rows)
top = max(rows, key=lambda r: r["sales"])["region"]
summary = {"total_sales": total, "top_region": top, "regions": len(rows)}
with open("/airflow/xcom/return.json", "w", encoding="utf-8") as f:
    json.dump(summary, f)
""".strip(),
        ],
        env_vars={"ROWS_JSON": "{{ ti.xcom_pull(task_ids='extract') | tojson }}"},
        do_xcom_push=True,
        **KPO_DEMO_KWARGS,
    )

    load = KubernetesPodOperator(
        task_id="load",
        name="demo-etl-load",
        image="busybox:1.36",
        cmds=["sh", "-c"],
        arguments=[
            'echo "Loaded summary to warehouse (demo): $SUMMARY_JSON"',
        ],
        env_vars={"SUMMARY_JSON": "{{ ti.xcom_pull(task_ids='transform') | tojson }}"},
        **KPO_DEMO_KWARGS,
    )

    extract >> transform >> load
