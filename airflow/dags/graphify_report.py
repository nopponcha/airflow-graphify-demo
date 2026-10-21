"""Pull Graphify analysis stats from the in-cluster graphify service and report them."""
from __future__ import annotations

import json
import os
import re
import urllib.request

import pendulum
from airflow.sdk import dag, task

GRAPHIFY_URL = os.environ.get("GRAPHIFY_URL", "http://graphify.airflow-demo.svc.cluster.local:8080")


def _get(path: str) -> str:
    with urllib.request.urlopen(f"{GRAPHIFY_URL}/{path}", timeout=30) as r:
        return r.read().decode("utf-8")


@dag(
    dag_id="graphify_code_report",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Bangkok"),
    catchup=False,
    tags=["demo", "graphify"],
)
def graphify_code_report():
    @task
    def fetch_stats() -> dict:
        return json.loads(_get("stats.json"))

    @task
    def fetch_god_nodes() -> list[str]:
        report = _get("GRAPH_REPORT.md")
        sec = report.split("## God Nodes", 1)[-1].split("\n## ", 1)[0]
        return re.findall(r"^\d+\. `([^`]+)` - (\d+) edges", sec, flags=re.M)[:10]

    @task
    def fetch_cycles() -> int:
        report = _get("GRAPH_REPORT.md")
        sec = report.split("## Import Cycles", 1)[-1].split("\n## ", 1)[0]
        return sum(1 for line in sec.splitlines() if "cycle:" in line)

    @task
    def report(stats: dict, gods: list, cycles: int) -> None:
        print("=== Graphify x Apache Airflow ===")
        print(f"Commit: {stats.get('commit')}  built: {stats.get('built_at')}")
        print(f"Nodes: {stats.get('nodes'):,}  Edges: {stats.get('edges'):,}")
        print(f"Import cycles found: {cycles}")
        for i, (name, n) in enumerate(gods, 1):
            print(f"{i:2}. {name} ({n} edges)")

    report(fetch_stats(), fetch_god_nodes(), fetch_cycles())


graphify_code_report()
