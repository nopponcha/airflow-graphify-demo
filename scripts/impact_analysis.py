#!/usr/bin/env python3
"""DAG impact analysis: git diff + Graphify reverse deps + AST runtime links."""
from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None  # type: ignore


@dataclass
class ImpactHit:
    dag_id: str
    task_id: str | None
    link_type: str  # code import | runtime
    detail: str = ""


@dataclass
class ImpactRow:
    changed: str
    hits: list[ImpactHit] = field(default_factory=list)


def run(cmd: list[str], cwd: Path | None = None) -> str:
    return subprocess.check_output(cmd, cwd=cwd, text=True, stderr=subprocess.STDOUT)


def git_changed_files(base: str, head: str, repo: Path, prefix: str) -> list[Path]:
    out = run(["git", "diff", "--name-only", f"{base}..{head}"], cwd=repo)
    files: list[Path] = []
    for line in out.splitlines():
        line = line.strip()
        if not line.startswith(prefix):
            continue
        files.append(repo / line)
    return files


def git_changed_symbols(base: str, head: str, repo: Path, path: Path) -> set[str]:
    """Best-effort: function/class names touched in diff hunks."""
    rel = path.relative_to(repo).as_posix()
    try:
        diff = run(["git", "diff", f"{base}..{head}", "-U0", "--", rel], cwd=repo)
    except subprocess.CalledProcessError:
        return set()
    symbols: set[str] = set()
    for line in diff.splitlines():
        if line.startswith(("+", "-")) and not line.startswith(("+++", "---")):
            stripped = line[1:].strip()
            if stripped.startswith("def ") or stripped.startswith("async def "):
                symbols.add(stripped.split("(")[0].replace("def ", "").replace("async ", "").strip())
            elif stripped.startswith("class "):
                symbols.add(stripped.split("(")[0].split(":")[0].replace("class ", "").strip())
    if not symbols and path.suffix == ".py":
        # File-level change: all top-level defs in the new version
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    symbols.add(node.name)
                elif isinstance(node, ast.ClassDef):
                    symbols.add(node.name)
        except OSError:
            pass
    return symbols


def run_graphify(dags_dir: Path) -> Path:
    graphify = "graphify"
    airflow_root = dags_dir.parent
    out_dir = dags_dir / "graphify-out"
    if out_dir.exists():
        import shutil

        shutil.rmtree(out_dir)
    subprocess.check_call(
        [graphify, "update", "dags", "--no-cluster"],
        cwd=airflow_root,
    )
    graph_path = dags_dir / "graphify-out" / "graph.json"
    if not graph_path.is_file():
        raise FileNotFoundError(f"Graphify did not produce {graph_path}")
    return graph_path


def load_graph(path: Path) -> tuple[dict[str, dict], list[dict]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    nodes = {n["id"]: n for n in data.get("nodes", [])}
    links = data.get("links", data.get("edges", []))
    return nodes, links


def reverse_reach(nodes: dict[str, dict], links: list[dict], seeds: set[str]) -> set[str]:
    """Nodes that transitively depend on seeds (edge source -> target means source uses target)."""
    dependents: dict[str, set[str]] = defaultdict(set)
    for link in links:
        dependents[link["target"]].add(link["source"])
    seen = set(seeds)
    queue = list(seeds)
    while queue:
        cur = queue.pop()
        for upstream in dependents.get(cur, ()):
            if upstream not in seen:
                seen.add(upstream)
                queue.append(upstream)
    return seen


def norm_path(p: str) -> str:
    return p.replace("\\", "/").lstrip("./")


def nodes_for_changed_files(nodes: dict[str, dict], changed: Iterable[Path], dags_dir: Path) -> set[str]:
    rel_paths = {norm_path(str(f.relative_to(dags_dir.parent))) for f in changed}
    # graphify stores paths like dags/common/pipeline.py
    hits: set[str] = set()
    for nid, node in nodes.items():
        sf = norm_path(node.get("source_file", ""))
        if sf in rel_paths or any(sf.endswith(rp) for rp in rel_paths):
            hits.add(nid)
        label = node.get("label", "")
        for rp in rel_paths:
            if rp.endswith(".py") and label == Path(rp).name:
                hits.add(nid)
    return hits


def nodes_for_symbols(nodes: dict[str, dict], module_path: str, symbols: set[str]) -> set[str]:
    seeds: set[str] = set()
    mod_slug = module_path.replace("/", "_").replace(".py", "").replace("-", "_")
    for sym in symbols:
        for nid, node in nodes.items():
            if sym and sym in node.get("label", ""):
                if module_path in node.get("source_file", ""):
                    seeds.add(nid)
            # graphify ids like common_pipeline_clean_data
            guess = f"{mod_slug}_{sym}"
            if nid == guess or nid.endswith(f"_{sym}"):
                seeds.add(nid)
    return seeds


def parse_dag_files(dags_dir: Path) -> dict[str, dict[str, Any]]:
    """AST metadata per DAG file: dag_id, imports, runtime links."""
    meta: dict[str, dict[str, Any]] = {}
    asset_uri_by_name: dict[str, str] = {}
    assets_file = dags_dir / "teams" / "assets_demo.py"
    if assets_file.is_file():
        for line in assets_file.read_text(encoding="utf-8").splitlines():
            if "Asset(" in line and "=" in line:
                name = line.split("=")[0].strip()
                uri = line.split("Asset(", 1)[1].split(")", 1)[0].strip('"\'')
                asset_uri_by_name[name] = uri

    for py in sorted(dags_dir.rglob("*.py")):
        if py.name == "__init__.py" or "graphify-out" in py.as_posix():
            continue
        try:
            tree = ast.parse(py.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        rel = py.relative_to(dags_dir.parent).as_posix()
        info: dict[str, Any] = {
            "dag_ids": set(),
            "imported_symbols": set(),
            "external_waits": [],  # (external_dag_id, external_task_id)
            "triggers": [],
            "asset_outlets": set(),
            "asset_inlets": set(),
            "tasks": set(),
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                name = ""
                if isinstance(func, ast.Name):
                    name = func.id
                elif isinstance(func, ast.Attribute):
                    name = func.attr
                if name in {"dag", "DAG"}:
                    for kw in node.keywords:
                        if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                            info["dag_ids"].add(str(kw.value.value))
                if name == "ExternalTaskSensor":
                    ext_dag = ext_task = None
                    for kw in node.keywords:
                        if kw.arg == "external_dag_id" and isinstance(kw.value, ast.Constant):
                            ext_dag = str(kw.value.value)
                        if kw.arg == "external_task_id" and isinstance(kw.value, ast.Constant):
                            ext_task = str(kw.value.value)
                    if ext_dag:
                        info["external_waits"].append((ext_dag, ext_task))
                if name == "TriggerDagRunOperator":
                    for kw in node.keywords:
                        if kw.arg == "trigger_dag_id" and isinstance(kw.value, ast.Constant):
                            info["triggers"].append(str(kw.value.value))
                if name == "task":
                    for kw in node.keywords:
                        if kw.arg == "outlets" and isinstance(kw.value, (ast.List, ast.Tuple)):
                            for elt in kw.value.elts:
                                if isinstance(elt, ast.Name) and elt.id in asset_uri_by_name:
                                    info["asset_outlets"].add(asset_uri_by_name[elt.id])
            if isinstance(node, ast.FunctionDef):
                for dec in node.decorator_list:
                    if isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "task":
                        info["tasks"].add(node.name)
                        for kw in dec.keywords:
                            if kw.arg == "outlets" and isinstance(kw.value, (ast.List, ast.Tuple)):
                                for elt in kw.value.elts:
                                    if isinstance(elt, ast.Name) and elt.id in asset_uri_by_name:
                                        info["asset_outlets"].add(asset_uri_by_name[elt.id])
            if isinstance(node, ast.ImportFrom) and node.module:
                mod = node.module
                for alias in node.names:
                    info["imported_symbols"].add(f"{mod}.{alias.name}")
            if isinstance(node, ast.Import):
                for alias in node.names:
                    info["imported_symbols"].add(alias.name)
        # schedule=[Asset(...)] on @dag
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "dag":
                for kw in node.keywords:
                    if kw.arg == "schedule" and isinstance(kw.value, (ast.List, ast.Tuple)):
                        for elt in kw.value.elts:
                            if isinstance(elt, ast.Name) and elt.id in asset_uri_by_name:
                                info["asset_inlets"].add(asset_uri_by_name[elt.id])
        if info["dag_ids"]:
            meta[rel] = info
    return meta


def dag_id_from_graph_node(node: dict[str, Any], dag_meta: dict[str, dict]) -> tuple[str | None, str | None]:
    sf = norm_path(node.get("source_file", ""))
    task_label = node.get("label", "")
    task_id = None
    if task_label.endswith("()"):
        task_id = task_label[:-2]
    for path, info in dag_meta.items():
        if sf.endswith(path) or path in sf:
            dag_ids = info.get("dag_ids") or set()
            if dag_ids:
                dag_id = sorted(dag_ids)[0]
                tasks = info.get("tasks") or set()
                if task_id in tasks:
                    return dag_id, task_id
                # graphify task ids embed dag name
                for t in tasks:
                    if t in node.get("id", ""):
                        return dag_id, t
                return dag_id, task_id
    return None, None


def load_owners(path: Path) -> tuple[dict[str, str], dict[str, dict]]:
    if yaml is None:
        raise RuntimeError("PyYAML is required (pip install pyyaml)")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    dag_owners = raw.get("dag_owners", {})
    teams = raw.get("teams", {})
    return dag_owners, teams


def owner_for_dag(dag_id: str, dag_owners: dict[str, str], teams: dict[str, dict]) -> str:
    team_key = dag_owners.get(dag_id, "platform")
    team = teams.get(team_key, {})
    gh = team.get("github", "@nopponcha")
    display = team.get("display", team_key)
    return f"{display} ({gh})"


def build_impact_rows(
    base: str,
    head: str,
    repo: Path,
    dags_rel: str = "airflow/dags",
) -> tuple[list[ImpactRow], Path]:
    dags_dir = repo / dags_rel
    changed = git_changed_files(base, head, repo, prefix=dags_rel + "/")
    if not changed:
        return [], dags_dir

    graph_path = run_graphify(dags_dir)
    nodes, links = load_graph(graph_path)
    dag_meta = parse_dag_files(dags_dir)

    rows: list[ImpactRow] = []

    # Group changes by file
    for path in changed:
        if not path.suffix == ".py":
            sym_label = path.relative_to(repo).as_posix()
            rows.append(ImpactRow(changed=sym_label))
            continue
        symbols = git_changed_symbols(base, head, repo, path)
        rel_mod = path.relative_to(dags_dir.parent).as_posix()
        seeds = nodes_for_changed_files(nodes, [path], dags_dir)
        seeds |= nodes_for_symbols(nodes, rel_mod, symbols)

        reachable = reverse_reach(nodes, links, seeds) if seeds else set()
        hits: list[ImpactHit] = []
        seen_hit: set[tuple[str, str | None, str]] = set()

        for nid in reachable:
            node = nodes.get(nid)
            if not node:
                continue
            dag_id, task_id = dag_id_from_graph_node(node, dag_meta)
            if not dag_id:
                continue
            key = (dag_id, task_id, "code import")
            if key not in seen_hit:
                seen_hit.add(key)
                hits.append(ImpactHit(dag_id=dag_id, task_id=task_id, link_type="code import"))

        # AST: direct imports of changed module symbols
        module = path.relative_to(dags_dir).as_posix().replace(".py", "").replace("/", ".")
        for dag_path, info in dag_meta.items():
            for imp in info["imported_symbols"]:
                if module in imp or any(s in imp for s in symbols):
                    for dag_id in info["dag_ids"]:
                        key = (dag_id, None, "code import")
                        if key not in seen_hit:
                            seen_hit.add(key)
                            hits.append(
                                ImpactHit(
                                    dag_id=dag_id,
                                    task_id="(import)",
                                    link_type="code import",
                                    detail=imp,
                                )
                            )

        label = rel_mod
        if symbols:
            label = ", ".join(sorted(symbols)) + f" ({rel_mod})"
        rows.append(ImpactRow(changed=label, hits=hits))

    # Runtime cross-DAG links triggered when upstream DAG files change
    changed_dag_ids: set[str] = set()
    for path in changed:
        rel = path.relative_to(dags_dir.parent).as_posix()
        for dp, info in dag_meta.items():
            if dp in rel and info["dag_ids"]:
                changed_dag_ids |= info["dag_ids"]

    if changed_dag_ids:
        runtime_row = ImpactRow(changed="(runtime cross-DAG)")
        seen_rt: set[tuple[str, str | None, str]] = set()
        for dag_path, info in dag_meta.items():
            for ext_dag, ext_task in info["external_waits"]:
                if ext_dag in changed_dag_ids:
                    for dag_id in info["dag_ids"]:
                        key = (dag_id, ext_task or "wait", "runtime")
                        if key not in seen_rt:
                            seen_rt.add(key)
                            runtime_row.hits.append(
                                ImpactHit(
                                    dag_id=dag_id,
                                    task_id=f"wait_{ext_dag}" if not ext_task else f"wait_{ext_dag}.{ext_task}",
                                    link_type="runtime",
                                    detail=f"ExternalTaskSensor → {ext_dag}.{ext_task}",
                                )
                            )
        # Asset edges
        outlet_by_dag: dict[str, set[str]] = defaultdict(set)
        inlet_by_dag: dict[str, set[str]] = defaultdict(set)
        for info in dag_meta.values():
            for dag_id in info["dag_ids"]:
                outlet_by_dag[dag_id] |= info["asset_outlets"]
                inlet_by_dag[dag_id] |= info["asset_inlets"]
        for producer in changed_dag_ids:
            for uri in outlet_by_dag.get(producer, ()):
                for consumer, inlets in inlet_by_dag.items():
                    if uri in inlets:
                        key = (consumer, None, "runtime")
                        if key not in seen_rt:
                            seen_rt.add(key)
                            runtime_row.hits.append(
                                ImpactHit(
                                    dag_id=consumer,
                                    task_id="(asset schedule)",
                                    link_type="runtime",
                                    detail=f"Asset {uri} from {producer}",
                                )
                            )
        if runtime_row.hits:
            rows.append(runtime_row)

    return rows, graph_path


def format_markdown(rows: list[ImpactRow], dag_owners: dict[str, str], teams: dict[str, dict]) -> str:
    if not rows:
        return "## DAG impact analysis\n\nNo changes under `airflow/dags/` in this diff.\n"

    lines = [
        "## DAG impact analysis",
        "",
        "| Changed symbol / file | Affected DAG | Task | Owner | Link type |",
        "| --- | --- | --- | --- | --- |",
    ]
    owners_mentioned: set[str] = set()
    for row in rows:
        if not row.hits:
            lines.append(f"| `{row.changed}` | — | — | — | — |")
            continue
        for hit in row.hits:
            owner = owner_for_dag(hit.dag_id, dag_owners, teams)
            owners_mentioned.add(owner)
            task = hit.task_id or "—"
            detail = f" ({hit.detail})" if hit.detail else ""
            lines.append(
                f"| `{row.changed}` | `{hit.dag_id}` | `{task}`{detail} | {owner} | {hit.link_type} |"
            )
    lines.extend(
        [
            "",
            "### Owners to notify",
            "",
        ]
    )
    for o in sorted(owners_mentioned):
        lines.append(f"- {o}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute DAG impact for a git diff range.")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--base", required=True, help="Base git ref (exclusive..inclusive range with --head)")
    parser.add_argument("--head", default="HEAD", help="Head git ref")
    parser.add_argument("--owners", type=Path, default=None, help="OWNERS.yaml path")
    parser.add_argument("--output-md", type=Path, default=None, help="Write markdown report here")
    parser.add_argument("--output-json", type=Path, default=None, help="Write JSON summary")
    args = parser.parse_args()

    repo = args.repo_root
    owners_path = args.owners or (repo / "airflow" / "dags" / "OWNERS.yaml")
    dag_owners, teams = load_owners(owners_path)

    rows, graph_path = build_impact_rows(args.base, args.head, repo)
    md = format_markdown(rows, dag_owners, teams)

    if args.output_md:
        args.output_md.parent.mkdir(parents=True, exist_ok=True)
        args.output_md.write_text(md, encoding="utf-8")
    else:
        print(md)

    if args.output_json:
        payload = {
            "graph": str(graph_path.relative_to(repo)),
            "rows": [
                {
                    "changed": r.changed,
                    "hits": [
                        {
                            "dag_id": h.dag_id,
                            "task_id": h.task_id,
                            "link_type": h.link_type,
                            "detail": h.detail,
                        }
                        for h in r.hits
                    ],
                }
                for r in rows
            ],
        }
        args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return 0


if __name__ == "__main__":
    sys.exit(main())
