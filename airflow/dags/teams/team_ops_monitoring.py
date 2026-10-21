"""Team Ops — ExternalTaskSensor on sales + shared clean_data for log metrics."""
from __future__ import annotations

import pendulum
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task

from common.pipeline import clean_data


@dag(
    dag_id="team_ops_monitoring",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["team:ops", "impact-demo"],
)
def team_ops_monitoring():
    wait_sales_load = ExternalTaskSensor(
        task_id="wait_sales_daily_load",
        external_dag_id="team_sales_daily",
        external_task_id="load_warehouse",
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
    )

    @task
    def ingest_metrics() -> list[dict]:
        return [{"id": "m-1", "amount": 1, "segment": "latency"}]

    @task
    def normalize(rows: list[dict]) -> list[dict]:
        return clean_data(rows)

    @task
    def emit_health_check(rows: list[dict]) -> dict:
        return {"healthy": True, "signals": len(rows)}

    metrics = ingest_metrics()
    normalized = normalize(metrics)
    report = emit_health_check(normalized)
    wait_sales_load >> metrics


team_ops_monitoring()
