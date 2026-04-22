"""Stage A (28k) and Stage C (BGQ) train/val/test column assignment."""

from __future__ import annotations

import csv
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np

def _strip(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def load_bgq_scores_max_by_bgg_id(reviews_csv: Path) -> dict[str, float]:
    """Per bgg_id, max BGQ review score (float) for stratified splits."""
    out: dict[str, list[float]] = {}
    if not reviews_csv.is_file():
        return {}
    with reviews_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bid = _strip(row.get("bgg_id"))
            if not bid:
                continue
            raw = row.get("score")
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                continue
            try:
                sc = float(raw)
            except ValueError:
                continue
            out.setdefault(bid, []).append(sc)
    return {k: max(v) for k, v in out.items()}


def bgq_review_bgg_ids_set(reviews_csv: Path) -> set[str]:
    """Distinct bgg_id that appear in BGQ reviews (labeled pool)."""
    return set(load_bgq_scores_max_by_bgg_id(reviews_csv).keys())


def stratified_train_val_test(
    bgg_ids: Sequence[str],
    *,
    strat_labels: Sequence[float],
    seed: int,
    train_frac: float = 560 / 700,
    val_frac: float = 70 / 700,
    test_frac: float = 70 / 700,
) -> dict[str, str]:
    """Return bgg_id -> train|val|test."""
    ids = list(bgg_ids)
    labels = np.asarray(strat_labels, dtype=np.float64)
    if len(ids) != len(labels):
        raise ValueError("bgg_ids and strat_labels length mismatch")

    try:
        from sklearn.model_selection import train_test_split  # noqa: PLC0415
    except ImportError as e:
        raise ImportError("splits.stratified_train_val_test requires scikit-learn") from e

    n = len(ids)
    if n == 0:
        return {}

    relative_test = val_frac + test_frac
    if relative_test <= 0 or relative_test >= 1:
        raise ValueError("val_frac + test_frac must be in (0, 1)")

    strat = None
    if labels.size >= 4 and np.unique(labels).size >= 2:
        qs = np.quantile(labels, [0.25, 0.5, 0.75])
        strat = np.searchsorted(qs, labels, side="right")
        _, counts = np.unique(strat, return_counts=True)
        if np.min(counts) < 2:
            strat = None

    idx = np.arange(n)
    idx_train, idx_temp = train_test_split(
        idx,
        test_size=float(relative_test),
        random_state=seed,
        stratify=strat,
    )

    strat_temp = strat[idx_temp] if strat is not None else None
    if strat_temp is not None:
        _, c2 = np.unique(strat_temp, return_counts=True)
        if np.min(c2) < 2:
            strat_temp = None
    abs_temp_test = float(test_frac / relative_test)
    if len(idx_temp) < 2:
        idx_val, idx_test = idx_temp.astype(int), np.array([], dtype=int)
    else:
        idx_val, idx_test = train_test_split(
            idx_temp,
            test_size=abs_temp_test,
            random_state=seed + 1,
            stratify=strat_temp,
        )

    assign: dict[str, str] = {}
    for i in idx_train:
        assign[str(ids[int(i)])] = "train"
    for i in idx_val:
        assign[str(ids[int(i)])] = "val"
    for i in idx_test:
        assign[str(ids[int(i)])] = "test"
    return assign


def build_stage_c_splits(
    reviews_csv: Path,
    *,
    seed: int,
    splits_json_out: Path | None = None,
) -> tuple[dict[str, str], dict[str, Any]]:
    """
    BGQ cohort = all bgg_id with parseable scores in reviews.csv.
    Stratified split ~ 560/70/70 when N≈700; scales proportionally otherwise.
    """
    scores = load_bgq_scores_max_by_bgg_id(reviews_csv)
    cohort = sorted(scores.keys())
    meta: dict[str, Any] = {
        "cohort_source": str(reviews_csv.resolve()),
        "n_cohort": len(cohort),
        "seed": seed,
        "score_policy": "max(score) per bgg_id",
    }

    if not cohort:
        meta["train_ids"] = []
        meta["val_ids"] = []
        meta["test_ids"] = []
        if splits_json_out:
            splits_json_out.parent.mkdir(parents=True, exist_ok=True)
            splits_json_out.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
        return {}, meta

    labels = [scores[b] for b in cohort]
    mapping = stratified_train_val_test(
        cohort,
        strat_labels=labels,
        seed=seed,
        train_frac=560 / 700,
        val_frac=70 / 700,
        test_frac=70 / 700,
    )

    train_ids = sorted([b for b, s in mapping.items() if s == "train"])
    val_ids = sorted([b for b, s in mapping.items() if s == "val"])
    test_ids = sorted([b for b, s in mapping.items() if s == "test"])

    meta["train_ids"] = train_ids
    meta["val_ids"] = val_ids
    meta["test_ids"] = test_ids

    if splits_json_out:
        splits_json_out.parent.mkdir(parents=True, exist_ok=True)
        splits_json_out.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    return mapping, meta


def assign_stage_a_split(
    export_bgg_ids: Sequence[str],
    *,
    bgq_review_bgg_ids: set[str],
    seed: int,
    extra_test_fraction: float,
) -> dict[str, str]:
    """
    BGQ-reviewed games (`bgq_review` in id_map passed as bgq_review_bgg_ids) → ``test``.
    Optionally hold out ``extra_test_fraction`` of remaining BGG ids as ``test``.
    """
    rng = np.random.default_rng(seed)
    export_set = list(dict.fromkeys(export_bgg_ids))
    bgq_local = set(export_set) & bgq_review_bgg_ids
    rest = [b for b in export_set if b not in bgq_review_bgg_ids]

    assign: dict[str, str] = {}
    for b in bgq_local:
        assign[b] = "test"

    # Random subset of non-BGQ games → test
    mask = rng.uniform(size=len(rest)) < float(extra_test_fraction)
    for i, b in enumerate(rest):
        assign[b] = "test" if mask[i] else "train"

    # Any id not touched (shouldn't happen)
    for b in export_set:
        assign.setdefault(b, "train")

    return assign


def split_dict_from_splits_json(path: Path) -> dict[str, str]:
    """Load splits.json train_ids/val_ids/test_ids → bgg_id -> split."""
    if not path.is_file():
        return {}
    meta = json.loads(path.read_text(encoding="utf-8"))
    assign: dict[str, str] = {}
    for b in meta.get("train_ids") or []:
        assign[str(b)] = "train"
    for b in meta.get("val_ids") or []:
        assign[str(b)] = "val"
    for b in meta.get("test_ids") or []:
        assign[str(b)] = "test"
    return assign


def load_splits_json_optional(path: Path | None) -> dict[str, str]:
    if path is None:
        return {}
    return split_dict_from_splits_json(path)


def compute_bgq_review_bgg_ids_from_id_map(id_map_parquet: Path) -> set[str]:
    """Distinct bgg_id with at least one bgq_review row (embedding index)."""
    import pyarrow.parquet as pq  # noqa: PLC0415

    from embeddings.documents import DOC_BGQ_REVIEW

    tbl = pq.read_table(id_map_parquet, columns=["doc_kind", "bgg_id"])
    kinds = tbl.column("doc_kind").to_pylist()
    bggs = tbl.column("bgg_id").to_pylist()
    out: set[str] = set()
    for k, b in zip(kinds, bggs, strict=True):
        if (k or "").strip() != DOC_BGQ_REVIEW:
            continue
        bs = str(b).strip() if b is not None else ""
        if bs:
            out.add(bs)
    return out
