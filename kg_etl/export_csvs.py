from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

from rapidfuzz import fuzz, process

from .paths import ProjectPaths
from .util import (
    csv_iter,
    ensure_dir,
    jsonl_iter,
    parse_bool,
    parse_date_yyyy_mm_dd_from_iso,
    parse_datetime_iso,
    parse_float,
    parse_int,
    tsv_iter,
    write_csv,
)


@dataclass(frozen=True)
class ExportConfig:
    out_dir: Path
    # For dev/smoke runs. Set to None for full export.
    limit_games: Optional[int] = None
    limit_price_files: Optional[int] = None
    limit_reviews: Optional[int] = None
    limit_ranks: Optional[int] = None

    # Review linking strategy
    enable_review_fuzzy_match: bool = True
    review_match_min_score: int = 92  # 0..100; 92 is conservative to avoid bad links


def _pipe_list(v: Any) -> str:
    """
    Serialize a list to a pipe-delimited string for `split()` in Cypher.

    We intentionally avoid JSON parsing in Neo4j (APOC dependency).
    """
    if v is None:
        return ""
    if isinstance(v, list):
        items = [str(x).replace("|", "/") for x in v if x is not None and str(x).strip() != ""]
        return "|".join(items)
    s = str(v).strip()
    return s.replace("|", "/") if s else ""


def export_games(paths: ProjectPaths, cfg: ExportConfig) -> tuple[int, dict[str, str]]:
    """
    Returns (count, bgg_id->name) for later matching.
    """
    out_path = cfg.out_dir / "games.csv"
    fieldnames = [
        "bgg_id",
        "name",
        "year",
        "rank",
        "geek_rating",
        "avg_rating",
        "num_voters",
        "is_expansion",
        "description",
        "min_players",
        "max_players",
        "best_min_players",
        "best_max_players",
        "min_playtime",
        "max_playtime",
        "min_age",
        "complexity",
        "categories",
        "mechanisms",
        "abstracts_rank",
        "cgs_rank",
        "childrensgames_rank",
        "familygames_rank",
        "partygames_rank",
        "strategygames_rank",
        "thematic_rank",
        "wargames_rank",
    ]

    name_by_bgg_id: dict[str, str] = {}

    def rows() -> Iterable[dict[str, Any]]:
        i = 0
        for obj in jsonl_iter(paths.games_jsonl):
            i += 1
            if cfg.limit_games is not None and i > cfg.limit_games:
                break

            bgg_id = str(obj.get("bgg_id", "")).strip()
            if not bgg_id:
                continue
            name = str(obj.get("name", "")).strip()
            if name:
                name_by_bgg_id[bgg_id] = name

            yield {
                "bgg_id": bgg_id,
                "name": name,
                "year": parse_int(obj.get("year")),
                "rank": parse_int(obj.get("rank")),
                "geek_rating": parse_float(obj.get("geek_rating")),
                "avg_rating": parse_float(obj.get("avg_rating")),
                "num_voters": parse_int(obj.get("num_voters")),
                "is_expansion": parse_bool(obj.get("is_expansion")),
                "description": obj.get("description", ""),
                "min_players": parse_int(obj.get("min_players")),
                "max_players": parse_int(obj.get("max_players")),
                "best_min_players": parse_int(obj.get("best_min_players")),
                "best_max_players": parse_int(obj.get("best_max_players")),
                "min_playtime": parse_int(obj.get("min_playtime")),
                "max_playtime": parse_int(obj.get("max_playtime")),
                "min_age": parse_int(obj.get("min_age")),
                "complexity": parse_float(obj.get("complexity")),
                "categories": _pipe_list(obj.get("categories")),
                "mechanisms": _pipe_list(obj.get("mechanisms")),
                "abstracts_rank": parse_int(obj.get("abstracts_rank")),
                "cgs_rank": parse_int(obj.get("cgs_rank")),
                "childrensgames_rank": parse_int(obj.get("childrensgames_rank")),
                "familygames_rank": parse_int(obj.get("familygames_rank")),
                "partygames_rank": parse_int(obj.get("partygames_rank")),
                "strategygames_rank": parse_int(obj.get("strategygames_rank")),
                "thematic_rank": parse_int(obj.get("thematic_rank")),
                "wargames_rank": parse_int(obj.get("wargames_rank")),
            }

    count = write_csv(out_path, fieldnames, rows())
    return count, name_by_bgg_id


def export_mapping(paths: ProjectPaths, cfg: ExportConfig) -> int:
    out_path = cfg.out_dir / "bgo_keys.csv"
    fieldnames = ["key", "slug", "title", "bgg_id", "bgg_url", "detail_url"]

    def rows() -> Iterable[dict[str, Any]]:
        # 1) Read the "official" mapping file (may be a different key set)
        mapping_by_key: dict[str, dict[str, str]] = {}
        for row in tsv_iter(paths.bgo_map_tsv):
            key = row.get("key", "").strip()
            if not key:
                continue
            mapping_by_key[key] = row

        # 2) Read the actual key set we have price histories for
        if paths.price_key_name_tsv.exists():
            # Build game-name -> bgg_id from games.jsonl for name linking.
            # Keep this O(N) and deterministic; avoid fuzzy here (too expensive at scale).
            def normalize_name(s: str) -> str:
                s = s.lower().strip()
                s = (
                    s.replace("–", "-")
                    .replace("—", "-")
                    .replace("’", "'")
                    .replace("“", '"')
                    .replace("”", '"')
                )
                out = []
                for ch in s:
                    if ch.isalnum() or ch in {" ", ":", "-", "'"}:
                        out.append(ch)
                    else:
                        out.append(" ")
                s = "".join(out)
                s = " ".join(s.split())
                return s

            bgg_id_by_lower_name: dict[str, str] = {}
            bgg_id_by_norm_name: dict[str, str] = {}
            for obj in jsonl_iter(paths.games_jsonl):
                bgg_id = str(obj.get("bgg_id", "")).strip()
                name = str(obj.get("name", "")).strip()
                if not bgg_id or not name:
                    continue
                ln = name.lower()
                if ln and ln not in bgg_id_by_lower_name:
                    bgg_id_by_lower_name[ln] = bgg_id
                nn = normalize_name(name)
                if nn and nn not in bgg_id_by_norm_name:
                    bgg_id_by_norm_name[nn] = bgg_id

            def resolve_bgg_id_from_name(title: str) -> str:
                ln = title.strip().lower()
                if not ln:
                    return ""
                if ln in bgg_id_by_lower_name:
                    return bgg_id_by_lower_name[ln]
                nn = normalize_name(title)
                return bgg_id_by_norm_name.get(nn, "")

            for kn in tsv_iter(paths.price_key_name_tsv):
                key = kn.get("key", "").strip()
                title = kn.get("name", "").strip()
                if not key:
                    continue

                extra = mapping_by_key.get(key, {})
                bgg_id = extra.get("bgg_id", "").strip() or resolve_bgg_id_from_name(title)

                yield {
                    "key": key,
                    "slug": extra.get("slug", ""),
                    "title": extra.get("title", "") or title,
                    "bgg_id": bgg_id,
                    "bgg_url": extra.get("bgg_url", ""),
                    "detail_url": extra.get("detail_url", ""),
                }
            return

        # Fallback: use the mapping TSV as-is
        for row in mapping_by_key.values():
            key = row.get("key", "").strip()
            if not key:
                continue
            yield {
                "key": key,
                "slug": row.get("slug", ""),
                "title": row.get("title", ""),
                "bgg_id": row.get("bgg_id", "").strip(),
                "bgg_url": row.get("bgg_url", ""),
                "detail_url": row.get("detail_url", ""),
            }

    return write_csv(out_path, fieldnames, rows())


def export_ranks(paths: ProjectPaths, cfg: ExportConfig) -> int:
    out_path = cfg.out_dir / "ranks.csv"
    # Keep compatibility columns (used by Cypher import) and also store every
    # column from `boardgames_ranks.csv`.
    fieldnames = [
        "rank_id",
        "bgg_id",
        "rank_value",
        "bayesaverage",
        "average",
        "usersrated",
        "is_expansion",
        "abstracts_rank",
        "cgs_rank",
        "childrensgames_rank",
        "familygames_rank",
        "partygames_rank",
        "strategygames_rank",
        "thematic_rank",
        "wargames_rank",
        # additional columns from input
        "id",
        "name",
        "yearpublished",
        "rank",
    ]

    def rows() -> Iterable[dict[str, Any]]:
        i = 0
        for row in csv_iter(paths.ranks_csv):
            i += 1
            if cfg.limit_ranks is not None and i > cfg.limit_ranks:
                break
            bgg_id = row.get("id", "").strip()
            if not bgg_id:
                continue
            yield {
                "rank_id": bgg_id,
                "bgg_id": bgg_id,
                "rank_value": parse_int(row.get("rank")),
                # keep original numeric/string values too
                "bayesaverage": parse_float(row.get("bayesaverage")),
                "average": parse_float(row.get("average")),
                "usersrated": parse_int(row.get("usersrated")),
                "is_expansion": parse_bool(row.get("is_expansion")),
                "abstracts_rank": parse_int(row.get("abstracts_rank")),
                "cgs_rank": parse_int(row.get("cgs_rank")),
                "childrensgames_rank": parse_int(row.get("childrensgames_rank")),
                "familygames_rank": parse_int(row.get("familygames_rank")),
                "partygames_rank": parse_int(row.get("partygames_rank")),
                "strategygames_rank": parse_int(row.get("strategygames_rank")),
                "thematic_rank": parse_int(row.get("thematic_rank")),
                "wargames_rank": parse_int(row.get("wargames_rank")),
                "id": row.get("id", ""),
                "name": row.get("name", ""),
                "yearpublished": row.get("yearpublished", ""),
                "rank": row.get("rank", ""),
            }

    return write_csv(out_path, fieldnames, rows())


def export_price_points(paths: ProjectPaths, cfg: ExportConfig, bgg_id_by_bgo_key: dict[str, str]) -> int:
    """
    Produces `price_points.csv` only.
    Rules:
    - Skip rows where all of min/mean/max are missing.
    - Skip rows that cannot be mapped to a BGG game.
    """
    points_path = cfg.out_dir / "price_points.csv"
    # Store all fields present in `price_history[].result.data[]` in addition to
    # the derived/normalized properties used for querying.
    point_fields = [
        "price_point_id",
        "bgg_id",
        "date",
        "source",
        # derived + pt object fields
        "pt_id",
        "dt",
        "min",
        "mean",
        "max",
        "min_st",
        # normalized numeric fields (preferred)
        "min_price",
        "mean_price",
        "max_price",
    ]
    ensure_dir(cfg.out_dir)

    count_points = 0
    with points_path.open("w", encoding="utf-8", newline="") as pf:
        import csv

        p_writer = csv.DictWriter(pf, fieldnames=point_fields, extrasaction="ignore")
        p_writer.writeheader()

        files = sorted(paths.price_histories_dir.glob("*.json"))
        if cfg.limit_price_files is not None:
            files = files[: cfg.limit_price_files]

        for fpath in files:
            try:
                obj = json.loads(fpath.read_text(encoding="utf-8"))
            except Exception:
                continue

            bgo_key = str(obj.get("key", "")).strip()
            if not bgo_key:
                continue
            bgg_id = bgg_id_by_bgo_key.get(bgo_key, "")
            if not bgg_id:
                continue

            # price_history is a list; each element has result.data[]
            price_history = obj.get("price_history") or []
            for chunk in price_history:
                data = ((chunk or {}).get("result") or {}).get("data") or []
                for pt in data:
                    dt_raw = str((pt or {}).get("dt", "")).strip()
                    d = parse_date_yyyy_mm_dd_from_iso(dt_raw)
                    if d is None:
                        continue
                    min_v = parse_float((pt or {}).get("min"))
                    mean_v = parse_float((pt or {}).get("mean"))
                    max_v = parse_float((pt or {}).get("max"))
                    # user requirement: ignore points with no price values
                    if min_v is None and mean_v is None and max_v is None:
                        continue
                    date_str = d.isoformat()
                    price_point_id = f"{bgg_id}::{date_str}"

                    p_writer.writerow(
                        {
                            "price_point_id": price_point_id,
                            "bgg_id": bgg_id,
                            "date": date_str,
                            "source": "BGO",
                            "pt_id": str((pt or {}).get("id", "")).strip(),
                            "dt": dt_raw,
                            "min": min_v,
                            "mean": mean_v,
                            "max": max_v,
                            "min_st": parse_float((pt or {}).get("min_st")),
                            "min_price": min_v,
                            "mean_price": mean_v,
                            "max_price": max_v,
                        }
                    )
                    count_points += 1

    return count_points


def export_reviews(
    paths: ProjectPaths,
    cfg: ExportConfig,
    name_by_bgg_id: dict[str, str],
) -> tuple[int, int]:
    """
    Returns (reviews_count, review_edges_count).

    Linking:
    - Try exact match of `game_name` to `Game.name` (case-insensitive)
    - Fallback to fuzzy match against `Game.name` if enabled.
    """
    out_reviews = cfg.out_dir / "reviews.csv"
    out_edges = cfg.out_dir / "game_review_edges.csv"

    # Store every key present in `bgq_reviews.jsonl` records.
    # In this dataset the first record reveals all keys and we keep them stable.
    first_review = None
    for obj in jsonl_iter(paths.bgq_reviews_jsonl):
        first_review = obj
        break
    if first_review is None:
        first_review = {}

    bgq_keys = list(first_review.keys())
    # Keep our compatibility/computed columns first.
    review_fields = [
        "review_id",
        "bgg_id",
        "url",
        "title",
        "author",
        "category",
        "published_at",
        "score",
        "game_name_raw",
        # plus the rest of BGQ keys (full record storage)
    ] + [k for k in bgq_keys if k not in {"url", "title", "author", "category", "score"}]
    edge_fields = ["bgg_id", "review_id"]

    # Prepare match indexes
    by_lower_name: dict[str, str] = {}
    names: list[str] = []
    bgg_ids_for_names: list[str] = []
    for bgg_id, name in name_by_bgg_id.items():
        ln = name.strip().lower()
        if ln and ln not in by_lower_name:
            by_lower_name[ln] = bgg_id
            names.append(name)
            bgg_ids_for_names.append(bgg_id)

    def resolve_bgg_id(game_name: str) -> Optional[str]:
        ln = game_name.strip().lower()
        if not ln:
            return None
        if ln in by_lower_name:
            return by_lower_name[ln]
        if not cfg.enable_review_fuzzy_match or not names:
            return None
        match = process.extractOne(
            game_name,
            names,
            scorer=fuzz.WRatio,
        )
        if not match:
            return None
        choice, score, idx = match[0], match[1], match[2]
        if score < cfg.review_match_min_score:
            return None
        return bgg_ids_for_names[idx]

    import csv

    ensure_dir(cfg.out_dir)
    review_count = 0
    edge_count = 0
    with out_reviews.open("w", encoding="utf-8", newline="") as rf, out_edges.open(
        "w", encoding="utf-8", newline=""
    ) as ef:
        r_writer = csv.DictWriter(rf, fieldnames=review_fields, extrasaction="ignore")
        e_writer = csv.DictWriter(ef, fieldnames=edge_fields, extrasaction="ignore")
        r_writer.writeheader()
        e_writer.writeheader()

        i = 0
        for obj in jsonl_iter(paths.bgq_reviews_jsonl):
            i += 1
            if cfg.limit_reviews is not None and i > cfg.limit_reviews:
                break

            url = str(obj.get("url", "")).strip()
            if not url:
                continue

            game_name = str(obj.get("game_name", "")).strip()
            bgg_id = resolve_bgg_id(game_name) if game_name else None
            if not bgg_id:
                continue

            published = parse_datetime_iso(str(obj.get("published_date", "")).strip())
            published_at = published.isoformat() if published else ""

            # Prepare row with all keys from the BGQ record.
            row_out: dict[str, Any] = {
                "review_id": url,
                "bgg_id": bgg_id,
                "url": url,
                "title": obj.get("title", ""),
                "author": obj.get("author", ""),
                "category": obj.get("category", ""),
                "published_at": published_at,
                # store numeric score for querying
                "score": parse_float(obj.get("score")),
                "game_name_raw": game_name,
            }

            # Add every original BGQ key (body, hits, misses, etc.)
            for k in bgq_keys:
                if k in {"url", "title", "author", "category", "score"}:
                    # these are already normalized/stored above
                    continue
                v = obj.get(k)
                if k in {"hits", "misses"} and isinstance(v, list):
                    row_out[k] = _pipe_list(v)
                else:
                    row_out[k] = v if v is not None else ""

            r_writer.writerow(row_out)
            e_writer.writerow({"bgg_id": bgg_id, "review_id": url})
            review_count += 1
            edge_count += 1

    return review_count, edge_count


def export_all(paths: ProjectPaths, cfg: ExportConfig) -> dict[str, int]:
    ensure_dir(cfg.out_dir)

    games_count, name_by_bgg_id = export_games(paths, cfg)
    mapping_count = export_mapping(paths, cfg)

    # Build bgo_key -> bgg_id map for price history linking from the exported CSV
    bgg_id_by_bgo_key: dict[str, str] = {}
    for row in csv_iter(cfg.out_dir / "bgo_keys.csv"):
        key = row.get("key", "").strip()
        bgg_id = row.get("bgg_id", "").strip()
        if key and bgg_id:
            bgg_id_by_bgo_key[key] = bgg_id

    ranks_count = export_ranks(paths, cfg)
    price_points_count = export_price_points(paths, cfg, bgg_id_by_bgo_key=bgg_id_by_bgo_key)
    reviews_count, review_edges_count = export_reviews(paths, cfg, name_by_bgg_id=name_by_bgg_id)

    return {
        "games": games_count,
        "bgo_keys": mapping_count,
        "ranks": ranks_count,
        "price_points": price_points_count,
        "reviews": reviews_count,
        "review_edges": review_edges_count,
    }

