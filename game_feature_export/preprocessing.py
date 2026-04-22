"""Train-only scaling for ``features_per_game.parquet`` (tabular + optional L2 on ``mean_embedding``)."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from embeddings.documents import DOC_BGQ_REVIEW

ScalerKind = Literal["robust", "standard"]
EmbeddingPolicy = Literal["l2", "raw"]

# NN defaults: clip symmetric bounds toward train distribution (quantile) with a tight cap.
DEFAULT_NN_TABULAR_CLIP_CAP = 4.0  # max symmetric clip bound when using default --nn-safe quantile path
DEFAULT_NN_TABULAR_CLIP_QUANTILE = 0.99  # quantile of |scaled FIT values| before cap

# Alias for callers that pinned the old fixed clip constant.
DEFAULT_NN_TABULAR_CLIP = DEFAULT_NN_TABULAR_CLIP_CAP


def _load_sklearn():
    try:
        from sklearn.preprocessing import RobustScaler, StandardScaler  # noqa: PLC0415
    except ImportError as e:
        raise ImportError(
            "preprocessing requires scikit-learn: pip install scikit-learn"
        ) from e
    return RobustScaler, StandardScaler


def discover_scalar_columns(
    tbl: pa.Table,
    *,
    embedding_col: str = "mean_embedding",
    skip_embedding_cols: tuple[str, ...] = ("description_embedding",),
) -> list[str]:
    """Use every numeric column except ``bgg_id`` and list embedding columns."""
    skip = {"bgg_id", embedding_col, *skip_embedding_cols}
    out: list[str] = []
    for name in tbl.column_names:
        if name in skip:
            continue
        col = tbl.column(name).type
        if pa.types.is_fixed_size_list(col) or pa.types.is_list(col):
            continue
        if pa.types.is_floating(col) or pa.types.is_integer(col):
            out.append(name)
    return out


def table_to_arrays(
    tbl: pa.Table,
    *,
    scalar_cols: list[str],
    embedding_col: str = "mean_embedding",
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Returns (X_tab (n, k) float64, X_emb (n, d) float32, bgg_ids (n,) object).
    """
    n = tbl.num_rows
    if n == 0:
        raise ValueError("Empty table")
    bgg_ids = np.array(tbl.column("bgg_id").to_pylist(), dtype=object)

    rows_tab: list[list[float]] = []
    for name in scalar_cols:
        rows_tab.append([float(x) if x is not None else np.nan for x in tbl.column(name).to_pylist()])
    X_tab = np.asarray(rows_tab, dtype=np.float64).T

    emb_py = tbl.column(embedding_col).to_pylist()
    if not emb_py:
        raise ValueError(f"No {embedding_col!r} column")
    lens = {len(row) for row in emb_py if row is not None}
    if len(lens) != 1:
        raise ValueError(f"Inconsistent {embedding_col} lengths: {lens}")
    d = lens.pop()
    X_emb = np.asarray(emb_py, dtype=np.float32)
    if X_emb.shape != (n, d):
        raise ValueError(f"{embedding_col} shape {X_emb.shape} vs n_rows={n}, dim={d}")

    return X_tab, X_emb, bgg_ids


def train_mask_from_ids(bgg_ids: np.ndarray, train_ids: set[str]) -> np.ndarray:
    return np.array([(str(x) if x is not None else "") in train_ids for x in bgg_ids], dtype=bool)


def split_train_fraction(
    bgg_ids: np.ndarray,
    *,
    fraction: float,
    seed: int,
) -> np.ndarray:
    """Deterministic pseudo-random train mask (~fraction of rows)."""
    if not 0.0 < fraction <= 1.0:
        raise ValueError("fraction must be in (0, 1]")
    rng = np.random.default_rng(seed)
    u = rng.uniform(size=len(bgg_ids))
    return u < fraction


def bgq_review_bgg_ids_from_id_map(id_map_parquet: Path) -> set[str]:
    """Distinct ``bgg_id`` values that have at least one ``bgq_review`` row in ``id_map``."""
    if not id_map_parquet.is_file():
        raise FileNotFoundError(id_map_parquet)
    tbl = pq.read_table(id_map_parquet, columns=["doc_kind", "bgg_id"])
    kinds = tbl.column("doc_kind").to_pylist()
    bggs = tbl.column("bgg_id").to_pylist()
    out: set[str] = set()
    for k, b in zip(kinds, bggs, strict=True):
        if (k or "").strip() != DOC_BGQ_REVIEW:
            continue
        bid = str(b).strip() if b is not None else ""
        if bid:
            out.add(bid)
    return out


def eligible_rows_bgq_games(bgg_ids: np.ndarray, bgq_bgg_ids: set[str]) -> np.ndarray:
    """Boolean mask: row i is eligible iff ``bgg_ids[i]`` is in ``bgq_bgg_ids``."""
    return np.array(
        [str(x).strip() in bgq_bgg_ids if x is not None else False for x in bgg_ids],
        dtype=bool,
    )


def eligible_rows_non_bgq_games(bgg_ids: np.ndarray, bgq_bgg_ids: set[str]) -> np.ndarray:
    """Boolean mask: row i is eligible iff ``bgg_ids[i]`` is **not** in ``bgq_bgg_ids``."""
    return ~eligible_rows_bgq_games(bgg_ids, bgq_bgg_ids)


def split_train_fraction_eligible(
    bgg_ids: np.ndarray,
    eligible_mask: np.ndarray,
    *,
    fraction: float,
    seed: int,
) -> np.ndarray:
    """~``fraction`` of **eligible** rows get train=True; ineligible rows are never train."""
    if not 0.0 < fraction <= 1.0:
        raise ValueError("fraction must be in (0, 1]")
    train_mask = np.zeros(len(bgg_ids), dtype=bool)
    idx = np.flatnonzero(eligible_mask)
    if idx.size == 0:
        return train_mask
    rng = np.random.default_rng(seed)
    u = rng.uniform(size=idx.size)
    train_mask[idx[u < fraction]] = True
    return train_mask


def split_pipe_multi_labels(raw: Any) -> list[str]:
    """Split BGG pipe-separated lists (categories, mechanisms) into trimmed tokens."""
    if raw is None:
        return []
    s = str(raw).strip()
    if not s:
        return []
    return [p.strip() for p in s.split("|") if p.strip()]


def fit_ohe_vocab_from_strings(
    strings: Sequence[Any],
    *,
    max_tokens: int | None,
    min_count: int = 1,
) -> list[str]:
    """
    FIT-split token vocabulary: frequency-descending, then alphabetical for ties.
    Multi-label strings count each token independently.
    """
    ctr: Counter[str] = Counter()
    for raw in strings:
        for t in split_pipe_multi_labels(raw):
            ctr[t] += 1
    pairs = sorted(ctr.items(), key=lambda kv: (-kv[1], kv[0]))
    out = [t for t, c in pairs if c >= min_count]
    if max_tokens is not None:
        out = out[:max_tokens]
    return out


def multi_hot_matrix(strings: Sequence[Any], vocab: Sequence[str]) -> np.ndarray:
    """Shape (len(strings), len(vocab)), float32 in {0, 1}; unknown tokens omitted."""
    if not vocab:
        return np.zeros((len(strings), 0), dtype=np.float32)
    token_to_idx = {t: i for i, t in enumerate(vocab)}
    n = len(strings)
    k = len(vocab)
    out = np.zeros((n, k), dtype=np.float32)
    for i, raw in enumerate(strings):
        for t in split_pipe_multi_labels(raw):
            j = token_to_idx.get(t)
            if j is not None:
                out[i, j] = 1.0
    return out


def parquet_safe_ohe_names(prefix: str, tokens: Sequence[str]) -> tuple[str, ...]:
    """Unique parquet column names: ``prefix__slug`` with collision avoidance."""
    used: set[str] = set()
    names: list[str] = []
    for tok in tokens:
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", tok.strip())
        slug = slug.strip("_")[:120] or "x"
        base = f"{prefix}__{slug}"
        cand = base
        u = 2
        while cand in used:
            cand = f"{base}_{u}"
            u += 1
        used.add(cand)
        names.append(cand)
    return tuple(names)


def median_l2_embedding_scale(Xt: np.ndarray, Xe: np.ndarray) -> float:
    """Scale factor so median row L2 norms of ``Xe`` match those of ``Xt`` (after tabular scaling / clip)."""
    nt = np.linalg.norm(np.asarray(Xt, dtype=np.float64), axis=1)
    ne = np.linalg.norm(np.asarray(Xe, dtype=np.float64), axis=1)
    nt_f = nt[np.isfinite(nt)]
    ne_f = ne[np.isfinite(ne)]
    if nt_f.size == 0 or ne_f.size == 0:
        return 1.0
    mt = float(np.median(nt_f))
    me = float(np.median(ne_f))
    if not math.isfinite(mt) or not math.isfinite(me) or me <= 1e-12:
        return 1.0
    return mt / me


def apply_embedding_policy(X_emb: np.ndarray, policy: EmbeddingPolicy) -> np.ndarray:
    x = np.asarray(X_emb, dtype=np.float64)
    if policy == "raw":
        return x.astype(np.float32, copy=False)
    norms = np.linalg.norm(x, axis=1, keepdims=True)
    norms = np.maximum(norms, 1e-12)
    out = (x / norms).astype(np.float32, copy=False)
    return out


def build_tabular_scaler(kind: ScalerKind):
    RobustScaler, StandardScaler = _load_sklearn()
    if kind == "robust":
        return RobustScaler(with_centering=True, with_scaling=True, quantile_range=(25.0, 75.0))
    return StandardScaler()


@dataclass(frozen=True)
class PreprocessBundle:
    """Serialized artifact for inference (joblib)."""

    tabular_scaler: Any
    scalar_columns: tuple[str, ...]
    scaler_kind: ScalerKind
    embedding_policy: EmbeddingPolicy
    embedding_dim: int
    train_bgg_id_sha256: str
    meta: dict[str, Any]
    tabular_clip: float | None = None
    embedding_block_scale: float = 1.0
    pre_row_divide: tuple[float, ...] | None = None
    # Multi-label one-hot for ``categories`` / ``mechanisms`` (FIT vocab; unknown token → zeros).
    category_vocab: tuple[str, ...] = ()
    mechanism_vocab: tuple[str, ...] = ()
    category_ohe_columns: tuple[str, ...] = ()
    mechanism_ohe_columns: tuple[str, ...] = ()

    def transform(self, X_tab: np.ndarray, X_emb: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Apply fitted tabular scaler, optional symmetric clip, embedding policy, and block scale."""
        Xt_raw = np.asarray(X_tab, dtype=np.float64).copy()
        div = self.pre_row_divide
        if div is not None and len(div) == Xt_raw.shape[1]:
            for j in range(Xt_raw.shape[1]):
                d = float(div[j])
                if d > 1e-18:
                    Xt_raw[:, j] /= d
        Xt = self.tabular_scaler.transform(Xt_raw)
        tc = self.tabular_clip
        if tc is not None and math.isfinite(float(tc)):
            c = float(tc)
            Xt = np.clip(Xt, -c, c)
        Xe = apply_embedding_policy(np.asarray(X_emb, dtype=np.float32), self.embedding_policy)
        s = float(self.embedding_block_scale)
        if not math.isfinite(s):
            s = 1.0
        if s != 1.0:
            Xe = (Xe * s).astype(np.float32, copy=False)
        return Xt.astype(np.float32, copy=False), Xe


def fit_preprocess_bundle(
    *,
    X_tab_train: np.ndarray,
    scalar_columns: list[str],
    scaler_kind: ScalerKind,
    embedding_policy: EmbeddingPolicy,
    embedding_dim: int,
    train_bgg_ids: list[str],
    tabular_clip: float | None = None,
    tabular_clip_quantile: float | None = None,
    tabular_clip_cap: float | None = None,
    X_emb_train: np.ndarray | None = None,
    balance_embedding_scale: bool = False,
    divide_columns_by_mean: tuple[str, ...] | None = None,
) -> PreprocessBundle:
    if tabular_clip is not None and tabular_clip <= 0:
        raise ValueError("tabular_clip must be positive when set")
    if tabular_clip is not None and tabular_clip_quantile is not None:
        raise ValueError("Specify at most one of tabular_clip and tabular_clip_quantile")
    if tabular_clip_quantile is not None and not 0.0 < tabular_clip_quantile < 1.0:
        raise ValueError("tabular_clip_quantile must be in (0, 1)")
    if balance_embedding_scale and X_emb_train is None:
        raise ValueError("balance_embedding_scale requires X_emb_train")
    X_fit_in = np.asarray(X_tab_train, dtype=np.float64).copy()
    divide_set = set(divide_columns_by_mean or ())
    pre_div: tuple[float, ...] | None = None
    if divide_set:
        pre_div_list = [1.0] * X_fit_in.shape[1]
        for j, name in enumerate(scalar_columns):
            if name in divide_set:
                col = X_fit_in[:, j]
                m = float(np.nanmean(col))
                pre_div_list[j] = m if m > 1e-18 else 1.0
                denom = pre_div_list[j]
                if denom > 1e-18:
                    X_fit_in[:, j] /= denom
        pre_div = tuple(pre_div_list)

    scaler = build_tabular_scaler(scaler_kind)
    scaler.fit(X_fit_in)
    Xt_fit = scaler.transform(np.asarray(X_fit_in, dtype=np.float64))

    resolved_clip: float | None = None
    if tabular_clip_quantile is not None:
        abs_flat = np.abs(np.asarray(Xt_fit, dtype=np.float64)).ravel()
        finite = abs_flat[np.isfinite(abs_flat)]
        if finite.size > 0:
            resolved_clip = float(np.quantile(finite, tabular_clip_quantile))
        if resolved_clip is not None and tabular_clip_cap is not None:
            cap = float(tabular_clip_cap)
            if cap <= 0:
                raise ValueError("tabular_clip_cap must be positive")
            resolved_clip = min(resolved_clip, cap)
        if resolved_clip is not None and not math.isfinite(resolved_clip):
            resolved_clip = None
    elif tabular_clip is not None:
        resolved_clip = float(tabular_clip)
        if not math.isfinite(resolved_clip):
            resolved_clip = None

    embedding_block_scale = 1.0
    if balance_embedding_scale and X_emb_train is not None:
        Xt_tr = np.asarray(Xt_fit, dtype=np.float64, copy=True)
        if resolved_clip is not None and math.isfinite(float(resolved_clip)):
            c = float(resolved_clip)
            Xt_tr = np.clip(Xt_tr, -c, c)
        Xe_tr = apply_embedding_policy(np.asarray(X_emb_train, dtype=np.float32), embedding_policy)
        embedding_block_scale = median_l2_embedding_scale(Xt_tr, Xe_tr)
        if not math.isfinite(embedding_block_scale):
            embedding_block_scale = 1.0
    lines = "\n".join(sorted(train_bgg_ids))
    h = hashlib.sha256(lines.encode("utf-8")).hexdigest()
    meta = {
        "scaler_kind": scaler_kind,
        "embedding_policy": embedding_policy,
        "n_scalar_features": len(scalar_columns),
        "embedding_dim": embedding_dim,
        "n_train_rows_used_for_fit": int(X_tab_train.shape[0]),
        "tabular_clip": resolved_clip,
        "tabular_clip_quantile_fit": tabular_clip_quantile,
        "tabular_clip_cap": tabular_clip_cap,
        "embedding_block_scale": embedding_block_scale,
        "balance_embedding_scale": balance_embedding_scale,
        "divide_columns_by_mean": list(divide_columns_by_mean or ()),
    }
    return PreprocessBundle(
        tabular_scaler=scaler,
        scalar_columns=tuple(scalar_columns),
        scaler_kind=scaler_kind,
        embedding_policy=embedding_policy,
        embedding_dim=embedding_dim,
        train_bgg_id_sha256=h,
        meta=meta,
        tabular_clip=resolved_clip,
        embedding_block_scale=embedding_block_scale,
        pre_row_divide=pre_div,
    )


def standardized_table(
    tbl: pa.Table,
    bundle: PreprocessBundle,
    *,
    scalar_cols: list[str],
    embedding_col: str = "mean_embedding",
    extra_embedding_cols: tuple[str, ...] = ("description_embedding",),
    categories_col: str = "categories",
    mechanisms_col: str = "mechanisms",
) -> pa.Table:
    """Transform full table; preserve ``bgg_id`` and non-feature columns not in scalar_cols unchanged."""
    X_tab, X_emb, bgg_ids = table_to_arrays(tbl, scalar_cols=scalar_cols, embedding_col=embedding_col)
    if X_tab.shape[1] != len(bundle.scalar_columns):
        raise ValueError("scalar column list mismatch bundle")
    Xt, Xe = bundle.transform(X_tab, X_emb)

    extra_emb_out: dict[str, np.ndarray] = {}
    for enc in extra_embedding_cols:
        if enc not in tbl.column_names or enc == embedding_col:
            continue
        emb_py = tbl.column(enc).to_pylist()
        if not emb_py:
            continue
        Xex = np.asarray(emb_py, dtype=np.float32)
        extra_emb_out[enc] = apply_embedding_policy(Xex, bundle.embedding_policy)

    ohe_additions: dict[str, np.ndarray] = {}
    if bundle.category_vocab:
        if categories_col not in tbl.column_names:
            raise ValueError(
                f"Bundle has category one-hot vocab but column {categories_col!r} is missing from table"
            )
        cats = tbl.column(categories_col).to_pylist()
        Mcat = multi_hot_matrix(cats, bundle.category_vocab)
        for idx, name in enumerate(bundle.category_ohe_columns):
            if name in tbl.column_names:
                raise ValueError(f"One-hot column name collides with existing column: {name!r}")
            ohe_additions[name] = Mcat[:, idx].astype(np.float64, copy=False)
    if bundle.mechanism_vocab:
        if mechanisms_col not in tbl.column_names:
            raise ValueError(
                f"Bundle has mechanism one-hot vocab but column {mechanisms_col!r} is missing from table"
            )
        mechs = tbl.column(mechanisms_col).to_pylist()
        Mmech = multi_hot_matrix(mechs, bundle.mechanism_vocab)
        for idx, name in enumerate(bundle.mechanism_ohe_columns):
            if name in tbl.column_names or name in ohe_additions:
                raise ValueError(f"One-hot column name collides: {name!r}")
            ohe_additions[name] = Mmech[:, idx].astype(np.float64, copy=False)

    cols: dict[str, Any] = {}
    for name in tbl.column_names:
        if name == "bgg_id":
            cols[name] = tbl.column(name)
        elif name == embedding_col:
            cols[name] = [row.tolist() for row in Xe]
        elif name in extra_emb_out:
            arr = extra_emb_out[name]
            cols[name] = [arr[i].tolist() for i in range(arr.shape[0])]
        elif name in scalar_cols:
            j = scalar_cols.index(name)
            cols[name] = Xt[:, j].astype(np.float64)
        else:
            cols[name] = tbl.column(name)

    # Append one-hot columns after existing schema (multi-label binary features; not RobustScaled).
    for name in bundle.category_ohe_columns:
        cols[name] = ohe_additions[name]
    for name in bundle.mechanism_ohe_columns:
        cols[name] = ohe_additions[name]

    return pa.table(cols)


def write_bundle(path: Path, bundle: PreprocessBundle) -> None:
    import joblib  # noqa: PLC0415

    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, path)


def load_bundle(path: Path) -> PreprocessBundle:
    import joblib  # noqa: PLC0415

    obj = joblib.load(path)
    if not isinstance(obj, PreprocessBundle):
        raise TypeError(f"Expected PreprocessBundle, got {type(obj)}")
    # Bundles saved before ``tabular_clip`` existed may omit the attribute after unpickle.
    if not hasattr(obj, "tabular_clip"):
        object.__setattr__(obj, "tabular_clip", None)
    if not hasattr(obj, "embedding_block_scale"):
        object.__setattr__(obj, "embedding_block_scale", 1.0)
    if not hasattr(obj, "pre_row_divide"):
        object.__setattr__(obj, "pre_row_divide", None)
    if not hasattr(obj, "category_vocab"):
        object.__setattr__(obj, "category_vocab", ())
    if not hasattr(obj, "mechanism_vocab"):
        object.__setattr__(obj, "mechanism_vocab", ())
    if not hasattr(obj, "category_ohe_columns"):
        object.__setattr__(obj, "category_ohe_columns", ())
    if not hasattr(obj, "mechanism_ohe_columns"):
        object.__setattr__(obj, "mechanism_ohe_columns", ())
    tc = getattr(obj, "tabular_clip", None)
    if tc is not None and not math.isfinite(float(tc)):
        object.__setattr__(obj, "tabular_clip", None)
    ebs = getattr(obj, "embedding_block_scale", 1.0)
    if not math.isfinite(float(ebs)):
        object.__setattr__(obj, "embedding_block_scale", 1.0)
    return obj


def write_preprocess_meta(output_dir: Path, bundle: PreprocessBundle, extra: dict[str, Any]) -> None:
    payload = {
        **bundle.meta,
        "scalar_columns": list(bundle.scalar_columns),
        "train_bgg_id_sha256": bundle.train_bgg_id_sha256,
        **extra,
    }
    (output_dir / "preprocess_meta.json").write_text(
        json.dumps(payload, indent=2) + "\n", encoding="utf-8"
    )
