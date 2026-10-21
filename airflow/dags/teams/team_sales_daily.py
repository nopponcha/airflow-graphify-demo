"""Team Sales — daily load; publishes SALES_DAILY_ASSET for downstream finance."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from common.pipeline import clean_data, enrich_customer, load_to_warehouse
from teams.assets_demo import SALES_DAILY_ASSET


@dag(
    dag_id="team_sales_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["team:sales", "impact-demo"],
)
def team_sales_daily():
    @task
    def extract_sales() -> list[dict]:
        return [
            {"id": "o-1", "amount": 15000, "segment": "enterprise"},
            {"id": "o-2", "amount": 400, "segment": "smb"},
        ]

    @task
    def clean(rows: list[dict]) -> list[dict]:
        return clean_data(rows)

    @task
    def enrich(rows: list[dict]) -> list[dict]:
        return enrich_customer(rows)

    @task(outlets=[SALES_DAILY_ASSET])
    def load_warehouse(rows: list[dict]) -> dict:
        return load_to_warehouse("sales_daily", rows)

    load_warehouse(enrich(clean(extract_sales())))


team_sales_daily()
