"""
Stream embedding shards, score review rows with a transformers sentiment model,
write ``review_sentiment.parquet`` (faiss_id, sentiment_score, sentiment_model, doc_kind).

Interrupted runs can continue with ``--resume`` (part files + checkpoint next to the output).
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from embeddings.layout import ArtifactPaths

from game_feature_export.schema import DOC_KINDS_REVIEW

from game_feature_export.review_sentiment.scoring import batch_scores

CHECKPOINT_VERSION = 1

_SENTIMENT_SCHEMA = pa.schema(
    [
        ("faiss_id", pa.int64()),
        ("sentiment_score", pa.float32()),
        ("sentiment_model", pa.large_string()),
        ("doc_kind", pa.large_string()),
    ]
)


def _parts_dir(out_parquet: Path) -> Path:
    return out_parquet.with_name(out_parquet.stem + "_parts")


def _checkpoint_path(out_parquet: Path) -> Path:
    return out_parquet.with_name(out_parquet.stem + "_checkpoint.json")


def _sorted_shard_paths(shards_dir: Path) -> list[Path]:
    paths = sorted(shards_dir.glob("shard_*.parquet"))
    if not paths:
        raise FileNotFoundError(f"No shard_*.parquet under {shards_dir}")
    return paths


def _shard_faiss_id_offsets(shard_paths: list[Path]) -> list[int]:
    out: list[int] = []
    cum = 0
    for p in shard_paths:
        out.append(cum)
        cum += pq.read_metadata(p).num_rows
    return out


def _precount_review_candidate_rows(
    shard_paths: list[Path],
    *,
    cap: int | None,
) -> int:
    """Rows whose ``doc_kind`` is a review kind (upper bound on scored rows if some texts empty)."""
    total = 0
    for sp in shard_paths:
        tbl = pq.read_table(sp, columns=["doc_kind"])
        for k in tbl.column("doc_kind").to_pylist():
            if (k or "") in DOC_KINDS_REVIEW:
                total += 1
                if cap is not None and total >= cap:
                    return total
    return total


def _require_shard_text_column(shard_paths: list[Path]) -> None:
    """Shards must include ``text`` (embeddings pipeline ``--store-text``). Fail fast before loading HF models."""
    schema = pq.read_schema(shard_paths[0])
    if "text" in schema.names:
        return
    raise ValueError(
        "Embedding shards have no 'text' column (your index was built without storing full text). "
        "Rebuild embeddings with the same inputs and add --store-text "
        "(e.g. python scripts/build_embeddings_faiss.py ... --store-text; see embeddings/README.md). "
        "Sentiment scoring needs review body text in shards/shard_*.parquet."
    )


def _device_key(device: int | str) -> str:
    return str(device)


def _validate_checkpoint(
    ckpt: dict[str, Any],
    *,
    embedding_root: Path,
    sentiment_model: str,
    batch_size: int,
    max_rows: int | None,
    device: int | str,
) -> None:
    if int(ckpt.get("version", 0)) != CHECKPOINT_VERSION:
        raise ValueError(f"Unsupported checkpoint version {ckpt.get('version')!r}")
    if ckpt.get("embedding_root") != str(embedding_root.resolve()):
        raise ValueError(
            "Checkpoint embedding_root does not match; use the same --embedding-root as the original run."
        )
    if ckpt.get("sentiment_model") != sentiment_model:
        raise ValueError("Checkpoint sentiment_model does not match; use the same --sentiment-model as the original run.")
    if int(ckpt.get("batch_size", -1)) != batch_size:
        raise ValueError("Checkpoint batch_size does not match; use the same --batch-size as the original run.")
    ck_max = ckpt.get("max_rows")
    ck_max_i = int(ck_max) if ck_max is not None else None
    if ck_max_i != max_rows:
        raise ValueError("Checkpoint max_rows does not match; use the same --max-rows as the original run.")
    if ckpt.get("device") != _device_key(device):
        raise ValueError("Checkpoint device does not match; use the same --device as the original run.")


def _write_checkpoint(
    path: Path,
    *,
    embedding_root: Path,
    sentiment_model: str,
    batch_size: int,
    max_rows: int | None,
    device: int | str,
    next_shard_idx: int,
    next_row: int,
    written: int,
    next_part_seq: int,
) -> None:
    path.write_text(
        json.dumps(
            {
                "version": CHECKPOINT_VERSION,
                "embedding_root": str(embedding_root.resolve()),
                "sentiment_model": sentiment_model,
                "batch_size": batch_size,
                "max_rows": max_rows,
                "device": _device_key(device),
                "next_shard_idx": next_shard_idx,
                "next_row": next_row,
                "written": written,
                "next_part_seq": next_part_seq,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _merge_parts_to_output(parts_dir: Path, out_parquet: Path) -> None:
    parts = sorted(parts_dir.glob("part_*.parquet"))
    if not parts:
        raise ValueError(f"No part files under {parts_dir} to merge.")
    with pq.ParquetWriter(out_parquet, schema=_SENTIMENT_SCHEMA, compression="zstd") as writer:
        for p in parts:
            writer.write_table(pq.read_table(p))


def run_review_sentiment(
    *,
    embedding_root: Path,
    out_parquet: Path,
    sentiment_model: str,
    batch_size: int = 32,
    device: int | str = "mps",
    max_rows: int | None = None,
    show_progress: bool = True,
    resume: bool = False,
    overwrite: bool = False,
) -> Path:
    """
    Read ``shards/shard_*.parquet`` in faiss order; require a ``text`` column.
    Only rows with ``doc_kind`` in ``bgq_review`` / ``bgg_review`` are scored and written.

    Writes incremental ``part_*.parquet`` files under ``<out_stem>_parts/`` and a checkpoint
    file; on success merges to ``out_parquet`` and removes parts + checkpoint.
    Use ``--resume`` to continue after an interruption (same CLI flags as the original run).
    """
    embedding_root = embedding_root.resolve()
    out_parquet = out_parquet.resolve()
    parts_dir = _parts_dir(out_parquet)
    ckpt_path = _checkpoint_path(out_parquet)

    paths = ArtifactPaths(root=embedding_root)
    shards_dir = paths.shards_dir
    if not shards_dir.is_dir():
        raise FileNotFoundError(f"Missing shards directory: {shards_dir}")

    shard_paths = _sorted_shard_paths(shards_dir)
    offsets = _shard_faiss_id_offsets(shard_paths)
    _require_shard_text_column(shard_paths)

    if overwrite:
        shutil.rmtree(parts_dir, ignore_errors=True)
        ckpt_path.unlink(missing_ok=True)
        out_parquet.unlink(missing_ok=True)

    start_shard_idx = 0
    start_row = 0
    written = 0
    part_seq = 0

    partial = ckpt_path.is_file() or parts_dir.is_dir()

    if resume:
        if not ckpt_path.is_file():
            raise FileNotFoundError(
                f"Cannot --resume: missing {ckpt_path}. "
                "Use a partial run that wrote checkpoint + parts, or start without --resume."
            )
        if not parts_dir.is_dir():
            raise FileNotFoundError(f"Cannot --resume: missing parts directory {parts_dir}")
        ckpt = json.loads(ckpt_path.read_text(encoding="utf-8"))
        _validate_checkpoint(
            ckpt,
            embedding_root=embedding_root,
            sentiment_model=sentiment_model,
            batch_size=batch_size,
            max_rows=max_rows,
            device=device,
        )
        start_shard_idx = int(ckpt["next_shard_idx"])
        start_row = int(ckpt["next_row"])
        written = int(ckpt["written"])
        part_seq = int(ckpt["next_part_seq"])
    elif partial:
        raise FileExistsError(
            f"Interrupted sentiment run found ({parts_dir} or {ckpt_path}). "
            "Pass --resume with the same flags, or --overwrite to discard progress."
        )
    else:
        out_parquet.unlink(missing_ok=True)
        shutil.rmtree(parts_dir, ignore_errors=True)
        parts_dir.mkdir(parents=True, exist_ok=True)

    from transformers import pipeline  # noqa: PLC0415

    clf = pipeline(
        "sentiment-analysis",
        model=sentiment_model,
        device=device,
        truncation=True,
        max_length=512,
    )

    pbar = None
    if show_progress:
        try:
            from tqdm import tqdm  # noqa: PLC0415
        except ImportError:  # pragma: no cover
            tqdm = None  # type: ignore[assignment,misc]
        else:
            pre = _precount_review_candidate_rows(shard_paths, cap=max_rows)
            if pre > 0:
                pbar = tqdm(
                    total=pre,
                    initial=min(written, pre),
                    desc="Scoring reviews",
                    unit="review",
                    smoothing=0.05,
                )

    def save_ckpt(next_shard_idx: int, next_row: int) -> None:
        _write_checkpoint(
            ckpt_path,
            embedding_root=embedding_root,
            sentiment_model=sentiment_model,
            batch_size=batch_size,
            max_rows=max_rows,
            device=device,
            next_shard_idx=next_shard_idx,
            next_row=next_row,
            written=written,
            next_part_seq=part_seq,
        )

    fids_batch: list[int] = []
    kinds_batch: list[str] = []
    texts_batch: list[str] = []

    def flush() -> None:
        nonlocal written, fids_batch, kinds_batch, texts_batch, part_seq
        if not texts_batch:
            return
        n_batch = len(texts_batch)
        raw = clf(texts_batch)
        if isinstance(raw, dict):
            raw = [raw]
        scores = batch_scores(raw)
        batch_tbl = pa.table(
            {
                "faiss_id": pa.array(fids_batch, type=pa.int64()),
                "sentiment_score": pa.array(scores, type=pa.float32()),
                "sentiment_model": pa.array([sentiment_model] * n_batch, type=pa.large_string()),
                "doc_kind": pa.array(kinds_batch, type=pa.large_string()),
            }
        )
        part_path = parts_dir / f"part_{part_seq:06d}.parquet"
        pq.write_table(batch_tbl, part_path)
        part_seq += 1
        written += n_batch
        if pbar is not None:
            pbar.update(n_batch)
        fids_batch, kinds_batch, texts_batch = [], [], []

    completed = False
    loop_exc: BaseException | None = None
    try:
        for shard_idx, sp in enumerate(shard_paths):
            if shard_idx < start_shard_idx:
                continue
            tbl = pq.read_table(sp, columns=["doc_kind", "text"])
            base = offsets[shard_idx]
            n = tbl.num_rows
            kinds = tbl.column("doc_kind").to_pylist()
            texts = tbl.column("text").to_pylist()

            row_start = start_row if shard_idx == start_shard_idx else 0
            for i in range(row_start, n):
                if max_rows is not None and written >= max_rows:
                    break
                dk = kinds[i] or ""
                if dk not in DOC_KINDS_REVIEW:
                    continue
                txt = (texts[i] or "").strip()
                if not txt:
                    continue
                fids_batch.append(base + i)
                kinds_batch.append(dk)
                texts_batch.append(txt)
                if len(texts_batch) >= batch_size:
                    flush()
                    save_ckpt(shard_idx, i + 1)

            flush()
            save_ckpt(shard_idx + 1, 0)

            if max_rows is not None and written >= max_rows:
                break
        completed = True
    except BaseException as e:
        loop_exc = e
    finally:
        if pbar is not None:
            pbar.close()

    if loop_exc is not None:
        raise loop_exc
    assert completed

    if written == 0:
        pq.write_table(
            pa.table(
                {
                    "faiss_id": pa.array([], type=pa.int64()),
                    "sentiment_score": pa.array([], type=pa.float32()),
                    "sentiment_model": pa.array([], type=pa.large_string()),
                    "doc_kind": pa.array([], type=pa.large_string()),
                }
            ),
            out_parquet,
            compression="zstd",
        )
    else:
        _merge_parts_to_output(parts_dir, out_parquet)

    shutil.rmtree(parts_dir, ignore_errors=True)
    ckpt_path.unlink(missing_ok=True)

    meta_sidecar = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "embedding_root": str(embedding_root),
        "sentiment_model": sentiment_model,
        "batch_size": batch_size,
        "device": device,
        "review_doc_kinds": sorted(DOC_KINDS_REVIEW),
        "rows_written": written,
        "resumed": resume,
    }
    (out_parquet.parent / "review_sentiment_run_meta.json").write_text(
        json.dumps(meta_sidecar, indent=2) + "\n", encoding="utf-8"
    )
    return out_parquet
