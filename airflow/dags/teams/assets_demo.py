"""Shared Asset URI for cross-DAG scheduling (Airflow 3)."""
from airflow.sdk import Asset

SALES_DAILY_ASSET = Asset("file://demo/warehouse/sales_daily")
MARKETING_READY_ASSET = Asset("file://demo/warehouse/marketing_campaign")
