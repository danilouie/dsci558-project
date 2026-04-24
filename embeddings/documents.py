from __future__ import annotations

import csv
import hashlib
import re
import heapq
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Iterable, Iterator

from kg_etl.util import tsv_iter

# Matches neo4j/SCHEMA.md + embedding plan doc_kinds.
DOC_BGQ_REVIEW = "bgq_review"
DOC_BGG_REVIEW = "bgg_review"
DOC_GAME_DESCRIPTION = "game_description"
DOC_GAME_ID_NAME = "game_id_name"

_DOC_KIND_ORDER = {
    DOC_GAME_DESCRIPTION: 0,
    DOC_BGQ_REVIEW: 1,
    DOC_BGG_REVIEW: 2,
    DOC_GAME_ID_NAME: 3,
}


@dataclass(frozen=True)
class EmbeddingDocument:
    """One row to be embedded; sorted order is the FAISS row order."""

    doc_kind: str
    sort_key: str  # deterministic string for ordering within kind
    review_id: str | None  # BGQ (url)
    bgg_review_id: str | None
    bgg_id: str | None  # required for game_description; optional denorm for reviews
    text: str

    def text_sha256(self) -> str:
        return hashlib.sha256(self.text.encode("utf-8")).hexdigest()


def _sort_tuple(d: EmbeddingDocument) -> tuple[int, str]:
    order = _DOC_KIND_ORDER.get(d.doc_kind, len(_DOC_KIND_ORDER))
    return (order, d.sort_key)


def sort_documents(rows: list[EmbeddingDocument]) -> list[EmbeddingDocument]:
    return sorted(rows, key=_sort_tuple)


def _merge_streams(
    streams: list[Iterable[EmbeddingDocument]],
    *,
    limit: int | None,
) -> list[EmbeddingDocument]:
    """K-way merge by SCHEMA ordering; each stream must be sorted by sort_key within its doc_kind."""
    keyed = (
        ((_sort_tuple(d), d) for d in stream) for stream in streams
    )
    merged = heapq.merge(*keyed)
    if limit is None:
        return [pair[1] for pair in merged]
    return [pair[1] for pair in islice(merged, limit)]


def bgq_embed_text(row: dict[str, str]) -> str:
    """Concatenate BGQ long-form fields (title + body + overview + hits/misses)."""
    parts: list[str] = []
    for k in ("title", "body", "gameplay_overview", "game_experience"):
        v = (row.get(k) or "").strip()
        if v:
            parts.append(v)
    for k in ("hits", "misses"):
        raw = row.get(k, "")
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            continue
        if isinstance(raw, str) and "|" in raw:
            parts.append(f"{k}: {raw}")
        else:
            parts.append(f"{k}: {raw}")
    return "\n\n".join(parts)


def iter_games_descriptions(games_csv: Path) -> Iterator[EmbeddingDocument]:
    if not games_csv.is_file():
        return
    with games_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            bgg_id = (row.get("bgg_id") or "").strip()
            desc = (row.get("description") or "").strip()
            if not bgg_id or not desc:
                continue
            yield EmbeddingDocument(
                doc_kind=DOC_GAME_DESCRIPTION,
                sort_key=bgg_id,
                review_id=None,
                bgg_review_id=None,
                bgg_id=bgg_id,
                text=desc,
            )


def game_id_name_embed_text(name: str, bgg_id: str) -> str:
    """Concatenate ``Game.name`` and ``Game.bgg_id`` for semantic embedding."""
    n = (name or "").strip()
    bid = (bgg_id or "").strip()
    if not bid or not n:
        return ""
    return f"{n}\n[bgg_id: {bid}]"


def iter_games_id_name(games_csv: Path) -> Iterator[EmbeddingDocument]:
    """One row per game: embed ``name`` + ``bgg_id`` string; ``id_map`` still keys ``bgg_id``."""
    if not games_csv.is_file():
        yield from ()
        return
    with games_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            bgg_id = (row.get("bgg_id") or "").strip()
            name = (row.get("name") or "").strip()
            if not bgg_id or not name:
                continue
            text = game_id_name_embed_text(name, bgg_id)
            if not text:
                continue
            yield EmbeddingDocument(
                doc_kind=DOC_GAME_ID_NAME,
                sort_key=bgg_id,
                review_id=None,
                bgg_review_id=None,
                bgg_id=bgg_id,
                text=text,
            )


def iter_bgq_reviews(reviews_csv: Path) -> Iterator[EmbeddingDocument]:
    if not reviews_csv.is_file():
        return
    with reviews_csv.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            # row values are strings from csv
            srow = {k: (v if v is not None else "") for k, v in row.items()}
            rid = (srow.get("review_id") or "").strip()
            if not rid:
                continue
            text = bgq_embed_text(srow).strip()
            if not text:
                continue
            bgg_id = (srow.get("bgg_id") or "").strip() or None
            yield EmbeddingDocument(
                doc_kind=DOC_BGQ_REVIEW,
                sort_key=rid,
                review_id=rid,
                bgg_review_id=None,
                bgg_id=bgg_id,
                text=text,
            )


def iter_bgg_reviews(
    tsv_paths: Iterable[Path],
    bgg_id_by_review: dict[str, str] | None = None,
) -> Iterator[EmbeddingDocument]:
    lookup = bgg_id_by_review or {}
    for p in tsv_paths:
        if not p.is_file():
            continue
        for row in tsv_iter(p):
            bid = (row.get("bgg_review_id") or "").strip()
            comment = (row.get("comment_text") or "").strip()
            if not bid or not comment:
                continue
            denorm = lookup.get(bid)
            yield EmbeddingDocument(
                doc_kind=DOC_BGG_REVIEW,
                sort_key=bid,
                review_id=None,
                bgg_review_id=bid,
                bgg_id=denorm,
                text=comment,
            )


def load_game_bgg_review_edges(neo4j_import: Path) -> dict[str, str]:
    """Map bgg_review_id -> bgg_id from game_bgg_review_edges.csv (optional denormalization)."""
    p = neo4j_import / "game_bgg_review_edges.csv"
    if not p.is_file():
        return {}
    import csv

    out: dict[str, str] = {}
    with p.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            br = (row.get("bgg_review_id") or "").strip()
            bgg = (row.get("bgg_id") or "").strip()
            if br and bgg:
                out[br] = bgg
    return out


def resolve_bgg_review_tsv_files(neo4j_import: Path) -> list[Path]:
    """
    Prefer unified bgg_reviews.tsv; fall back to chunked bgg_reviews_*.tsv (any depth,
    e.g. neo4j/import/bgg_review_chunks/ after chunk_bgg_reviews_tsv.py).
    """
    single = neo4j_import / "bgg_reviews.tsv"
    if single.is_file():
        return [single]
    return sorted(neo4j_import.glob("**/bgg_reviews_*.tsv"))


def _filter_by_bgg_ids(
    docs: Iterable[EmbeddingDocument], allowed: frozenset[str]
) -> Iterator[EmbeddingDocument]:
    """Keep rows whose ``bgg_id`` is in ``allowed`` (drops rows with missing id)."""
    for d in docs:
        bid = (d.bgg_id or "").strip()
        if bid and bid in allowed:
            yield d


def collect_all_documents(
    neo4j_import: Path,
    *,
    limit: int | None = None,
    include_games: bool = True,
    include_bgq: bool = True,
    include_bgg_reviews: bool = True,
    only_bgg_ids: frozenset[str] | None = None,
) -> list[EmbeddingDocument]:
    """
    Returns documents in global deterministic order (game descriptions, then BGQ, then BGG).

    For a small ``limit`` without ``include_bgg_reviews``, avoids scanning millions of BGG rows.
    A full export with BGG reviews still loads all BGG comments into memory for sorting.

    If ``only_bgg_ids`` is set, only rows whose ``bgg_id`` is in that set are kept (after load).
    """
    edges_map = load_game_bgg_review_edges(neo4j_import)
    streams: list[Iterable[EmbeddingDocument]] = []
    if include_games:
        g = iter_games_descriptions(neo4j_import / "games.csv")
        if only_bgg_ids is not None:
            g = _filter_by_bgg_ids(g, only_bgg_ids)
        streams.append(sorted(g, key=lambda d: d.sort_key))
    if include_bgq:
        g = iter_bgq_reviews(neo4j_import / "reviews.csv")
        if only_bgg_ids is not None:
            g = _filter_by_bgg_ids(g, only_bgg_ids)
        streams.append(sorted(g, key=lambda d: d.sort_key))
    if include_bgg_reviews:
        br_paths = resolve_bgg_review_tsv_files(neo4j_import)
        g = iter_bgg_reviews(br_paths, edges_map)
        if only_bgg_ids is not None:
            g = _filter_by_bgg_ids(g, only_bgg_ids)
        streams.append(sorted(g, key=lambda d: d.sort_key))
    if not streams:
        raise ValueError(
            "collect_all_documents: at least one of include_games/include_bgq/include_bgg_reviews must be True"
        )
    return _merge_streams(streams, limit=limit)


_ws_re = re.compile(r"\s+")


def normalize_query_text(s: str) -> str:
    return _ws_re.sub(" ", (s or "").strip())
