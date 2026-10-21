"""Cross-team data pipeline primitives — edits here ripple to many DAGs."""
from __future__ import annotations

from typing import Any


def clean_data(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize keys and drop rows without an id (used by sales, marketing, finance, ops).

    Demo change: reject negative amounts early (Team Platform PR).
    """
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        if not row.get("id"):
            continue
        amount = float(row.get("amount", 0) or 0)
        if amount < 0:
            continue
        cleaned.append(
            {
                "id": str(row["id"]).strip(),
                "amount": amount,
                "segment": (row.get("segment") or "unknown").lower(),
            }
        )
    return cleaned


def enrich_customer(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add simple customer tier labels (sales + finance)."""
    out: list[dict[str, Any]] = []
    for row in rows:
        amount = float(row.get("amount", 0))
        tier = "gold" if amount >= 10_000 else "silver" if amount >= 1_000 else "bronze"
        out.append({**row, "customer_tier": tier})
    return out


def validate_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Shared QA gate before warehouse load (marketing + ops)."""
    return [r for r in rows if r.get("amount", 0) >= 0]


def load_to_warehouse(table: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Pretend load step; returns metadata for logging / XCom."""
    return {"table": table, "row_count": len(rows), "status": "loaded"}
