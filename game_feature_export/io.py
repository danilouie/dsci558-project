"""Load embedding artifacts, reviewer maps, and sentiment sidecar."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from embeddings.documents import DOC_BGQ_REVIEW, DOC_BGG_REVIEW, DOC_GAME_DESCRIPTION
from embeddings.layout import ArtifactPaths, EmbeddingMeta

from game_feature_export.schema import DOC_KINDS_REVIEW


def read_embedding_meta(artifact_root: Path) -> EmbeddingMeta:
    paths = ArtifactPaths(root=artifact_root.resolve())
    return ArtifactPaths.read_meta(paths.meta_json)


def load_id_map_table(artifact_root: Path) -> pa.Table:
    paths = ArtifactPaths(root=artifact_root.resolve())
    if not paths.id_map_parquet.is_file():
        raise FileNotFoundError(paths.id_map_parquet)
    return pq.read_table(paths.id_map_parquet)


def filter_review_rows_with_bgg_id(tbl: pa.Table) -> pa.Table:
    """Keep only review doc kinds with non-empty bgg_id."""
    kind_mask = pc.is_in(tbl["doc_kind"], value_set=pa.array(list(DOC_KINDS_REVIEW)))
    bgg = tbl["bgg_id"]
    valid_bgg = pc.and_(pc.is_valid(bgg), pc.not_equal(pc.cast(bgg, pa.large_string()), ""))
    return tbl.filter(pc.and_(kind_mask, valid_bgg))


def load_game_description_faiss_map(artifact_root: Path) -> dict[str, int]:
    """
    bgg_id -> faiss_id for ``game_description`` rows (one row per game; last wins if duplicates).
    """
    tbl = load_id_map_table(artifact_root)
    kind_ok = pc.equal(tbl["doc_kind"], pa.scalar(DOC_GAME_DESCRIPTION))
    bgg = tbl["bgg_id"]
    valid_bgg = pc.and_(pc.is_valid(bgg), pc.not_equal(pc.cast(bgg, pa.large_string()), ""))
    sub = tbl.filter(pc.and_(kind_ok, valid_bgg))
    faiss_py = sub.column("faiss_id").to_pylist()
    bgg_py = sub.column("bgg_id").to_pylist()
    out: dict[str, int] = {}
    for fid, bid in zip(faiss_py, bgg_py, strict=True):
        bs = str(bid).strip() if bid is not None else ""
        if bs:
            out[bs] = int(fid)
    return out


def load_sentiment_by_faiss_id(path: Path) -> pa.Table:
    """Parquet with at least faiss_id (int64), sentiment_score (float).

    Accepts a single ``.parquet`` file or a directory of fragment ``*.parquet`` files
    (e.g. an interrupted sentiment run before merge).
    """
    path = path.resolve()
    if path.is_dir():
        parts = sorted(path.glob("*.parquet"))
        if not parts:
            raise FileNotFoundError(f"No *.parquet files in {path}")
        if len(parts) == 1:
            t = pq.read_table(parts[0])
        else:
            t = pa.concat_tables([pq.read_table(p) for p in parts])
    elif path.is_file():
        t = pq.read_table(path)
    else:
        raise FileNotFoundError(path)
    for col in ("faiss_id", "sentiment_score"):
        if col not in t.column_names:
            raise ValueError(f"Sentiment parquet missing required column {col!r}: {t.column_names}")
    return t


def join_sentiment(id_tbl: pa.Table, sentiment_tbl: pa.Table) -> pa.Table:
    """Inner-join sentiment on faiss_id (drops reviews without a score)."""
    right = sentiment_tbl.select(["faiss_id", "sentiment_score"])
    return id_tbl.join(right, keys=["faiss_id"], join_type="inner")


def _strip(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def load_bgq_review_author_map(reviews_csv: Path) -> dict[str, str]:
    """review_id -> author (BGQ)."""
    out: dict[str, str] = {}
    if not reviews_csv.is_file():
        return out
    with reviews_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rid = _strip(row.get("review_id"))
            if not rid:
                continue
            out[rid] = _strip(row.get("author"))
    return out


def load_bgg_username_map(bgg_reviews_tsv: Path) -> dict[str, str]:
    """bgg_review_id -> username."""
    out: dict[str, str] = {}
    if not bgg_reviews_tsv.is_file():
        return out
    from kg_etl.util import tsv_iter

    for row in tsv_iter(bgg_reviews_tsv):
        bid = _strip(row.get("bgg_review_id"))
        if not bid:
            continue
        out[bid] = _strip(row.get("username"))
    return out


def resolve_bgg_review_paths(neo4j_import: Path) -> list[Path]:
    single = neo4j_import / "bgg_reviews.tsv"
    if single.is_file():
        return [single]
    return sorted(neo4j_import.glob("**/bgg_reviews_*.tsv"))


def load_all_bgg_username_maps(neo4j_import: Path) -> dict[str, str]:
    merged: dict[str, str] = {}
    for p in resolve_bgg_review_paths(neo4j_import):
        merged.update(load_bgg_username_map(p))
    return merged


def add_reviewer_column(
    tbl: pa.Table,
    *,
    bgq_map: dict[str, str],
    bgg_map: dict[str, str],
) -> pa.Table:
    """Add reviewer_id string column from doc_kind + review_id / bgg_review_id."""

    def reviewer_for_row(
        doc_kind: str | None,
        review_id: str | None,
        bgg_review_id: str | None,
    ) -> str:
        dk = _strip(doc_kind)
        if dk == DOC_BGQ_REVIEW:
            rid = _strip(review_id)
            return bgq_map.get(rid, "")
        if dk == DOC_BGG_REVIEW:
            bid = _strip(bgg_review_id)
            return bgg_map.get(bid, "")
        return ""

    n = tbl.num_rows
    kinds = tbl.column("doc_kind").to_pylist()
    rids = tbl.column("review_id").to_pylist()
    brids = tbl.column("bgg_review_id").to_pylist()
    reviewers = [
        reviewer_for_row(k, r, b) for k, r, b in zip(kinds, rids, brids, strict=True)
    ]
    return tbl.append_column("reviewer_id", pa.array(reviewers, type=pa.large_string()))


def open_vectors_mmap(artifact_root: Path) -> tuple[np.ndarray | None, Any | None]:
    """
    Returns (vectors mmap ndarray, or None) and (faiss index or None).
    Prefer vectors.npy mmap; else load FAISS IndexFlat* and reconstruct rows.

    Non-flat indexes (e.g. IVF) do not support reliable per-row reconstruction without
    ``vectors.npy`` or a direct map; require ``vectors.npy`` in that case.
    """
    paths = ArtifactPaths(root=artifact_root.resolve())
    if paths.vectors_npy.is_file():
        mm = np.load(paths.vectors_npy, mmap_mode="r")
        return mm, None
    meta = ArtifactPaths.read_meta(paths.meta_json)
    if "Flat" not in meta.faiss_index_type:
        raise FileNotFoundError(
            f"{paths.vectors_npy} is required for index type {meta.faiss_index_type!r}. "
            "Reconstruct-by-faiss-id is only supported for IndexFlat* when vectors.npy is absent."
        )
    import faiss  # noqa: PLC0415

    if not paths.index_faiss.is_file():
        raise FileNotFoundError(
            f"Need {paths.vectors_npy} or {paths.index_faiss} under {paths.root}"
        )
    index = faiss.read_index(str(paths.index_faiss))
    return None, index


def fetch_vectors(
    faiss_ids: np.ndarray,
    *,
    vectors_mm: np.ndarray | None,
    faiss_index: Any | None,
    dim: int,
) -> np.ndarray:
    """Shape (len(faiss_ids), dim) float32."""
    faiss_ids = np.asarray(faiss_ids, dtype=np.int64)
    if vectors_mm is not None:
        return np.ascontiguousarray(vectors_mm[faiss_ids].astype(np.float32, copy=False))
    if faiss_index is None:
        raise ValueError("Neither vectors_mm nor faiss_index is available")
    out = np.empty((len(faiss_ids), dim), dtype=np.float32)
    for i, fid in enumerate(faiss_ids):
        out[i] = faiss_index.reconstruct(int(fid))
    return out


def load_concept_vectors(good_path: Path | None, bad_path: Path | None) -> tuple[np.ndarray | None, np.ndarray | None]:
    if good_path is None or bad_path is None:
        return None, None
    if not good_path.is_file() or not bad_path.is_file():
        return None, None
    g = np.load(good_path).astype(np.float32).reshape(-1)
    b = np.load(bad_path).astype(np.float32).reshape(-1)
    return g, b
