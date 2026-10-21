"""Simple ETL demo: extract -> transform -> load (no external services)."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="demo_simple_etl",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["demo", "etl"],
)
def demo_simple_etl():
    @task
    def extract() -> list[dict]:
        return [
            {"region": "BKK", "sales": 1200},
            {"region": "CNX", "sales": 640},
            {"region": "HKT", "sales": 910},
        ]

    @task
    def transform(rows: list[dict]) -> dict:
        total = sum(r["sales"] for r in rows)
        top = max(rows, key=lambda r: r["sales"])["region"]
        return {"total_sales": total, "top_region": top, "regions": len(rows)}

    @task
    def load(summary: dict) -> None:
        print(f"Loaded summary to warehouse (demo): {summary}")

    load(transform(extract()))


demo_simple_etl()
