from __future__ import annotations

import csv
import hashlib
import re
from pathlib import Path
from typing import Iterator

from embeddings.documents import DOC_GAME_DESCRIPTION, EmbeddingDocument

_UNCAT_KEY = "__uncategorized__"


def parse_pipe_categories(raw: str | None) -> list[str]:
    """Split BGG pipe-delimited categories field (matches kg_etl export)."""
    if raw is None or not str(raw).strip():
        return []
    return [p.strip() for p in str(raw).split("|") if p.strip()]


_slug_safe = re.compile(r"[^a-z0-9]+")


def category_slug(label: str, *, collision_suffix: str | None = None) -> str:
    """Filesystem-safe slug for a category label."""
    base = _slug_safe.sub("_", label.strip().lower()).strip("_")
    if not base:
        base = "category"
    if collision_suffix:
        return f"{base}_{collision_suffix}"
    return base


def stable_slug_suffix(label: str) -> str:
    """Short disambiguator when two category labels slug to the same string."""
    return hashlib.sha256(label.encode("utf-8")).hexdigest()[:8]


def iter_game_rows(games_csv: Path) -> Iterator[dict[str, str]]:
    if not games_csv.is_file():
        yield from ()
        return
    with games_csv.open("r", encoding="utf-8", newline="") as f:
        yield from csv.DictReader(f)


def build_documents_by_category(
    games_csv: Path,
    *,
    uncategorized: bool = True,
) -> dict[str, list[EmbeddingDocument]]:
    """
    Map category label -> ``EmbeddingDocument`` rows (game_description only).

    Games with multiple categories appear under each category bucket. Within a bucket,
    ``bgg_id`` is unique (first row wins).

    When ``uncategorized`` is True (default), games with no categories go under ``__uncategorized__``.
    Set ``uncategorized=False`` to omit those games from every category index.
    """
    buckets: dict[str, dict[str, EmbeddingDocument]] = {}

    def add_doc(cat_label: str, bid: str, desc: str) -> None:
        if cat_label not in buckets:
            buckets[cat_label] = {}
        if bid in buckets[cat_label]:
            return
        buckets[cat_label][bid] = EmbeddingDocument(
            doc_kind=DOC_GAME_DESCRIPTION,
            sort_key=bid,
            review_id=None,
            bgg_review_id=None,
            bgg_id=bid,
            text=desc,
        )

    for row in iter_game_rows(games_csv):
        bid = (row.get("bgg_id") or "").strip()
        desc = (row.get("description") or "").strip()
        if not bid or not desc:
            continue
        cats = parse_pipe_categories(row.get("categories"))
        if not cats:
            if uncategorized:
                add_doc(_UNCAT_KEY, bid, desc)
            continue
        for cat in cats:
            add_doc(cat, bid, desc)

    # Row order is arbitrary; ``sort_documents`` in the pipeline fixes order.
    return {k: list(v.values()) for k, v in buckets.items()}


def uncategorized_registry_key() -> str:
    return _UNCAT_KEY
