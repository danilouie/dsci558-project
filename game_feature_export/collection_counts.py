"""Aggregate User–Game collection edge counts from neo4j/import CSVs (see neo4j/SCHEMA.md)."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

# Basenames under neo4j/import (kg_etl/export_csvs.py).
_COLLECTION_FILES: tuple[tuple[str, str], ...] = (
    ("user_game_owns.csv", "owns"),
    ("user_game_wants.csv", "wants"),
    ("user_game_wants_to_buy.csv", "wtb"),
    ("user_game_wants_to_trade.csv", "wtt"),
)


def _strip(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def resolve_csv_paths(neo4j_import: Path, basename: str) -> list[Path]:
    """
    Prefer a single non-empty flat CSV; otherwise use sorted chunks under bgg_rel_chunks/.
    Never return both (chunks are splits of the flat file).
    """
    neo4j_import = neo4j_import.resolve()
    flat = neo4j_import / basename
    stem = basename[:-4] if basename.endswith(".csv") else basename
    if flat.is_file() and flat.stat().st_size > 0:
        return [flat]
    chunk_dir = neo4j_import / "bgg_rel_chunks"
    return sorted(chunk_dir.glob(f"{stem}_*.csv"))


def collection_shares(o: int, w: int, wb: int, wt: int) -> tuple[float, float, float, float]:
    """Each count divided by (o + w + wb + wt); all NaN if denominator is 0."""
    den = o + w + wb + wt
    if den <= 0:
        nan = float("nan")
        return (nan, nan, nan, nan)
    return (o / den, w / den, wb / den, wt / den)


def load_collection_counts_by_bgg_id(
    neo4j_import: Path,
) -> tuple[dict[str, tuple[int, int, int, int]], dict[str, Any]]:
    """
    Returns (counts_by_bgg_id, meta).

    Each value is (owns, wants, wants_to_buy, wants_to_trade) edge counts per game.
    """
    neo4j_import = neo4j_import.resolve()
    meta: dict[str, Any] = {
        "paths": {},
        "rows_scanned": {},
    }
    owns_m: dict[str, int] = {}
    wants_m: dict[str, int] = {}
    wtb_m: dict[str, int] = {}
    wtt_m: dict[str, int] = {}

    for basename, key in _COLLECTION_FILES:
        paths = resolve_csv_paths(neo4j_import, basename)
        meta["paths"][key] = [str(x) for x in paths]
        rows = 0
        target: dict[str, int]
        if key == "owns":
            target = owns_m
        elif key == "wants":
            target = wants_m
        elif key == "wtb":
            target = wtb_m
        else:
            target = wtt_m
        for p in paths:
            with p.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames or "bgg_id" not in reader.fieldnames:
                    continue
                for row in reader:
                    rows += 1
                    bid = _strip(row.get("bgg_id"))
                    if not bid:
                        continue
                    target[bid] = target.get(bid, 0) + 1
        meta["rows_scanned"][key] = rows

    all_ids = owns_m.keys() | wants_m.keys() | wtb_m.keys() | wtt_m.keys()
    merged: dict[str, tuple[int, int, int, int]] = {}
    for bid in all_ids:
        merged[bid] = (
            owns_m.get(bid, 0),
            wants_m.get(bid, 0),
            wtb_m.get(bid, 0),
            wtt_m.get(bid, 0),
        )
    return merged, meta
