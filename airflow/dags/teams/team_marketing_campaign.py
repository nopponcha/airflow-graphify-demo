"""Team Marketing — uses clean_data + validate_records; publishes marketing asset."""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from common.pipeline import clean_data, load_to_warehouse, validate_records
from teams.assets_demo import MARKETING_READY_ASSET


@dag(
    dag_id="team_marketing_campaign",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["team:marketing", "impact-demo"],
)
def team_marketing_campaign():
    @task
    def extract_leads() -> list[dict]:
        return [
            {"id": "l-10", "amount": 0, "segment": "inbound"},
            {"id": "l-11", "amount": 250, "segment": "partner"},
        ]

    @task
    def clean(rows: list[dict]) -> list[dict]:
        return clean_data(rows)

    @task
    def validate(rows: list[dict]) -> list[dict]:
        return validate_records(rows)

    @task(outlets=[MARKETING_READY_ASSET])
    def load(rows: list[dict]) -> dict:
        return load_to_warehouse("marketing_campaign", rows)

    load(validate(clean(extract_leads())))


team_marketing_campaign()
