"""Team Finance — asset-triggered report; imports clean_data + enrich_customer."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from common.pipeline import clean_data, enrich_customer
from teams.assets_demo import SALES_DAILY_ASSET


@dag(
    dag_id="team_finance_report",
    schedule=[SALES_DAILY_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["team:finance", "impact-demo"],
)
def team_finance_report():
    @task
    def read_sales_snapshot() -> list[dict]:
        return [{"id": "snap-1", "amount": 15000, "segment": "enterprise"}]

    @task
    def clean(rows: list[dict]) -> list[dict]:
        return clean_data(rows)

    @task
    def enrich(rows: list[dict]) -> list[dict]:
        return enrich_customer(rows)

    @task
    def build_finance_report(rows: list[dict]) -> dict:
        total = sum(r.get("amount", 0) for r in rows)
        return {"total_revenue": total, "rows": len(rows)}

    build_finance_report(enrich(clean(read_sales_snapshot())))


team_finance_report()
