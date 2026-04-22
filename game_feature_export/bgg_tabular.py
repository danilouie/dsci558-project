"""Load BGG game/rank CSVs from neo4j/import and merge into per-bgg_id feature dicts."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def _strip(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def _parse_float(raw: Any) -> float | None:
    s = _strip(raw)
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_int_optional(raw: Any) -> float | None:
    """Numeric columns that may be empty; use float for parquet/scaler consistency."""
    v = _parse_float(raw)
    return v


def _parse_bool_expansion(raw: Any) -> float:
    s = _strip(raw).lower()
    if s in ("true", "1", "yes"):
        return 1.0
    return 0.0


def _primary_category(categories: str) -> str:
    s = _strip(categories)
    if not s:
        return ""
    return s.split("|")[0].strip()


def load_games_by_bgg_id(games_csv: Path) -> dict[str, dict[str, Any]]:
    """Parse games.csv keyed by bgg_id string."""
    out: dict[str, dict[str, Any]] = {}
    if not games_csv.is_file():
        return out
    with games_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bid = _strip(row.get("bgg_id"))
            if not bid:
                continue
            out[bid] = {
                "game_name": _strip(row.get("name")),
                "year": _parse_float(row.get("year")),
                "bgg_chart_rank": _parse_float(row.get("rank")),
                "geek_rating": _parse_float(row.get("geek_rating")),
                "avg_rating": _parse_float(row.get("avg_rating")),
                "num_voters": _parse_float(row.get("num_voters")),
                "is_expansion": _parse_bool_expansion(row.get("is_expansion")),
                "min_players": _parse_float(row.get("min_players")),
                "max_players": _parse_float(row.get("max_players")),
                "best_min_players": _parse_float(row.get("best_min_players")),
                "best_max_players": _parse_float(row.get("best_max_players")),
                "min_playtime": _parse_float(row.get("min_playtime")),
                "max_playtime": _parse_float(row.get("max_playtime")),
                "min_age": _parse_float(row.get("min_age")),
                "complexity": _parse_float(row.get("complexity")),
                "categories": _strip(row.get("categories")),
                "mechanisms": _strip(row.get("mechanisms")),
                "abstracts_rank": _parse_int_optional(row.get("abstracts_rank")),
                "cgs_rank": _parse_int_optional(row.get("cgs_rank")),
                "childrensgames_rank": _parse_int_optional(row.get("childrensgames_rank")),
                "familygames_rank": _parse_int_optional(row.get("familygames_rank")),
                "partygames_rank": _parse_int_optional(row.get("partygames_rank")),
                "strategygames_rank": _parse_int_optional(row.get("strategygames_rank")),
                "thematic_rank": _parse_int_optional(row.get("thematic_rank")),
                "wargames_rank": _parse_int_optional(row.get("wargames_rank")),
            }
            out[bid]["primary_category"] = _primary_category(out[bid]["categories"])
    return out


def load_ranks_by_bgg_id(ranks_csv: Path) -> dict[str, dict[str, Any]]:
    """Parse ranks.csv keyed by bgg_id."""
    out: dict[str, dict[str, Any]] = {}
    if not ranks_csv.is_file():
        return out
    with ranks_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bid = _strip(row.get("bgg_id"))
            if not bid:
                continue
            out[bid] = {
                "rank_value": _parse_float(row.get("rank_value")),
                "bayes_average": _parse_float(row.get("bayesaverage")),
                "ranks_average": _parse_float(row.get("average")),
                "usersrated": _parse_float(row.get("usersrated")),
            }
    return out


def merge_tabular_for_bgg_id(
    bgg_id: str,
    games: dict[str, dict[str, Any]],
    ranks: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Left-join style dict for one game; missing CSV rows -> NaN-like placeholders."""
    nan = float("nan")
    g = games.get(bgg_id)
    r = ranks.get(bgg_id)

    def num(g_or_r: dict[str, Any] | None, key: str) -> float:
        if not g_or_r:
            return nan
        v = g_or_r.get(key)
        if v is None:
            return nan
        return float(v)

    cats = (_strip(g["categories"]) if g else "") or ""
    mech = (_strip(g["mechanisms"]) if g else "") or ""

    merged: dict[str, Any] = {
        "game_name": _strip(g["game_name"]) if g else "",
        "year": num(g, "year"),
        "bgg_chart_rank": num(g, "bgg_chart_rank"),
        "rank_value": num(r, "rank_value"),
        "geek_rating": num(g, "geek_rating"),
        "avg_rating": num(g, "avg_rating"),
        "bayes_average": num(r, "bayes_average"),
        "num_voters": num(g, "num_voters"),
        "usersrated_ranks": num(r, "usersrated"),
        "is_expansion": float(g["is_expansion"]) if g else 0.0,
        "min_players": num(g, "min_players"),
        "max_players": num(g, "max_players"),
        "best_min_players": num(g, "best_min_players"),
        "best_max_players": num(g, "best_max_players"),
        "min_playtime": num(g, "min_playtime"),
        "max_playtime": num(g, "max_playtime"),
        "min_age": num(g, "min_age"),
        "complexity": num(g, "complexity"),
        "abstracts_rank": num(g, "abstracts_rank"),
        "cgs_rank": num(g, "cgs_rank"),
        "childrensgames_rank": num(g, "childrensgames_rank"),
        "familygames_rank": num(g, "familygames_rank"),
        "partygames_rank": num(g, "partygames_rank"),
        "strategygames_rank": num(g, "strategygames_rank"),
        "thematic_rank": num(g, "thematic_rank"),
        "wargames_rank": num(g, "wargames_rank"),
        "categories": cats,
        "mechanisms": mech,
        "primary_category": _primary_category(cats),
        "has_games_csv": bool(g),
        "has_ranks_csv": bool(r),
    }
    return merged
