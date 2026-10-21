#!/bin/sh
# Clone Airflow (shallow) and build the Graphify knowledge graph (AST only, no LLM).
set -eu
REF="${AIRFLOW_REF:-main}"
rm -rf /work/airflow
git clone --depth 1 --branch "$REF" https://github.com/apache/airflow /work/airflow
# IMPORTANT: run from the PARENT dir. Running inside airflow/ breaks because
# airflow/logging/ shadows Python's stdlib "logging" module.
cd /work/airflow/airflow-core/src
graphify update airflow --no-cluster
graphify cluster-only airflow --no-label
OUT=airflow/graphify-out
mkdir -p /srv/site
cp "$OUT/graph.html" "$OUT/graph.json" "$OUT/GRAPH_REPORT.md" /srv/site/
git -C /work/airflow rev-parse HEAD > /srv/site/COMMIT.txt
date -u +%Y-%m-%dT%H:%M:%SZ > /srv/site/BUILT_AT.txt
python3 - <<'PY'
import json
g=json.load(open("/srv/site/graph.json"))
n=len(g.get("nodes",[])); e=len(g.get("links",g.get("edges",[])))
json.dump({"nodes":n,"edges":e,"commit":open("/srv/site/COMMIT.txt").read().strip(),
           "built_at":open("/srv/site/BUILT_AT.txt").read().strip()},open("/srv/site/stats.json","w"))
PY
cat > /srv/site/index.html <<'H'
<!doctype html><meta charset="utf-8"><title>Graphify x Apache Airflow</title>
<body style="font-family:sans-serif;max-width:720px;margin:40px auto">
<h1>Graphify: Apache Airflow Knowledge Graph</h1>
<ul><li><a href="graph.html">Interactive graph (graph.html)</a></li>
<li><a href="GRAPH_REPORT.md">Report (GRAPH_REPORT.md)</a></li>
<li><a href="stats.json">stats.json</a></li><li><a href="graph.json">graph.json (full, ~24MB)</a></li></ul>
H
rm -rf /work/airflow
echo "analysis done"
