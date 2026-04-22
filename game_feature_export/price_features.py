"""BGO price history JSON → scalar features (Stage B core + extended)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

import numpy as np

from kg_etl.util import tsv_iter

PriceFeatureMode = Literal["core", "extended"]


@dataclass(frozen=True)
class BgoMappingResult:
    """bgg_id → chosen BGO key; duplicates resolved by lexicographically smallest key."""

    bgg_to_key: dict[str, str]
    duplicate_bgg_ids: dict[str, list[str]]


def load_bgo_bgg_to_key(tsv_path: Path) -> BgoMappingResult:
    """Load mapping TSV: key slug title bgg_id ... — one winning key per bgg_id."""
    raw: dict[str, list[str]] = {}
    if not tsv_path.is_file():
        return BgoMappingResult(bgg_to_key={}, duplicate_bgg_ids={})

    for row in tsv_iter(tsv_path):
        key = (row.get("key") or "").strip()
        bgg = (row.get("bgg_id") or "").strip()
        if not key or not bgg:
            continue
        raw.setdefault(bgg, []).append(key)

    out: dict[str, str] = {}
    dups: dict[str, list[str]] = {}
    for bgg, keys in raw.items():
        uk = sorted(set(keys))
        if len(uk) > 1:
            dups[bgg] = uk
        out[bgg] = uk[0]
    return BgoMappingResult(bgg_to_key=out, duplicate_bgg_ids=dups)


def _parse_dt(raw: Any) -> datetime | None:
    if raw is None:
        return None
    s = str(raw).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _iter_week_rows(obj: Any) -> list[dict[str, Any]]:
    """Flatten price_history[].result.data[] rows."""
    rows: list[dict[str, Any]] = []
    if not isinstance(obj, dict):
        return rows
    blocks = obj.get("price_history") or []
    if not isinstance(blocks, list):
        return rows
    for block in blocks:
        if not isinstance(block, dict):
            continue
        res = block.get("result") or {}
        if not isinstance(res, dict):
            continue
        data = res.get("data") or []
        if not isinstance(data, list):
            continue
        for row in data:
            if isinstance(row, dict):
                rows.append(row)
    return rows


def load_price_series(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return _iter_week_rows(obj)


def _dedupe_sort_by_dt(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort by dt ascending; keep last row per duplicate dt."""
    by_dt: dict[datetime, dict[str, Any]] = {}
    for row in rows:
        dt = _parse_dt(row.get("dt"))
        if dt is None:
            continue
        by_dt[dt] = row
    return [by_dt[k] for k in sorted(by_dt.keys())]


def _mean_float(x: Any) -> float | None:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _ols_slope(y: np.ndarray) -> float:
    """y vs 0..n-1; ddof slope."""
    n = len(y)
    if n < 2:
        return 0.0
    x = np.arange(n, dtype=np.float64)
    x_mean = float(np.mean(x))
    y_mean = float(np.mean(y))
    denom = float(np.sum((x - x_mean) ** 2))
    if denom <= 1e-18:
        return 0.0
    return float(np.sum((x - x_mean) * (y - y_mean)) / denom)


def _max_drawdown_relative(series: np.ndarray) -> float:
    """Max (peak - value) / peak on running peak of weekly means."""
    if series.size == 0:
        return 0.0
    peak = float(series[0])
    max_dd = 0.0
    for v in series:
        fv = float(v)
        if fv > peak:
            peak = fv
        if peak > 1e-12:
            dd = (peak - fv) / peak
            if dd > max_dd:
                max_dd = dd
    return float(max_dd)


def compute_price_features_for_series(
    rows: list[dict[str, Any]],
    *,
    as_of: datetime | None,
    mode: PriceFeatureMode,
) -> dict[str, float]:
    """
    Compute price scalars from raw weekly rows (unsorted OK).
    ``as_of`` UTC: keep rows with dt <= as_of; if None, use last observed dt in series.
    """
    nan = float("nan")
    rows = _dedupe_sort_by_dt(rows)
    if not rows:
        return _empty_price_features(mode)

    last_dt_all = _parse_dt(rows[-1].get("dt"))
    cutoff = as_of
    if cutoff is None:
        cutoff = last_dt_all or datetime.now(timezone.utc)
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    cutoff = cutoff.astimezone(timezone.utc)

    filtered = []
    for row in rows:
        dt = _parse_dt(row.get("dt"))
        if dt is None:
            continue
        if dt <= cutoff:
            filtered.append((dt, row))

    if not filtered:
        return _empty_price_features(mode)

    dts = [t for t, _ in filtered]
    series_rows = [r for _, r in filtered]
    first_dt, last_dt = dts[0], dts[-1]

    means: list[float | None] = [_mean_float(r.get("mean")) for r in series_rows]
    mins: list[float | None] = [_mean_float(r.get("min")) for r in series_rows]
    maxs: list[float | None] = [_mean_float(r.get("max")) for r in series_rows]

    valid_idx = [i for i, m in enumerate(means) if m is not None]
    n_weeks = len(valid_idx)
    if n_weeks == 0:
        base = _empty_price_features(mode)
        base["weeks_since_last_obs"] = float(
            max(0.0, (cutoff - last_dt).total_seconds() / (7 * 24 * 3600))
        )
        base["calendar_span_weeks"] = 0.0
        return base

    last_i = valid_idx[-1]
    last_mean = float(means[last_i])
    last_min = mins[last_i]
    last_max = maxs[last_i]

    arr = np.array([means[i] for i in valid_idx], dtype=np.float64)
    mean_hist = float(np.mean(arr))
    median_weekly = float(np.median(arr))
    vol = float(np.std(arr, ddof=1)) if n_weeks >= 2 else 0.0
    cv = float(vol / mean_hist) if mean_hist > 1e-12 else 0.0
    p25, p75 = (
        float(np.percentile(arr, 25.0)),
        float(np.percentile(arr, 75.0)),
    )
    iqr = float(p75 - p25)

    first_mean = float(arr[0])
    pct_change = (
        float((last_mean - first_mean) / first_mean) if abs(first_mean) > 1e-12 else 0.0
    )

    def window_slope(k: int) -> float:
        idxs = valid_idx[-k:] if len(valid_idx) >= k else valid_idx
        if len(idxs) < 2:
            return 0.0
        y = np.array([means[i] for i in idxs], dtype=np.float64)
        return _ols_slope(y)

    slope_4w = window_slope(4)
    slope_12w = window_slope(12)
    slope_full = _ols_slope(arr)

    # trailing 12 calendar weeks null fraction
    week_start = cutoff - timedelta(weeks=12)
    slots = 12
    filled = 0
    for w in range(slots):
        ws = week_start + timedelta(weeks=w)
        we = ws + timedelta(weeks=1)
        any_ok = False
        for i, dt in enumerate(dts):
            if ws <= dt < we and means[i] is not None:
                any_ok = True
                break
        if any_ok:
            filled += 1
    trailing_null_frac_12w = float((slots - filled) / slots) if slots else 0.0

    weeks_since_last = float(max(0.0, (cutoff - last_dt).total_seconds() / (7 * 24 * 3600)))
    span_sec = max(0.0, (last_dt - first_dt).total_seconds())
    calendar_span_weeks = float(span_sec / (7 * 24 * 3600))

    mean_vs_last_ratio = (
        float(mean_hist / last_mean - 1.0) if last_mean > 1e-12 else 0.0
    )

    log1p_last_mean = float(np.log1p(max(0.0, last_mean)))
    log1p_mean_hist = float(np.log1p(max(0.0, mean_hist)))
    log1p_median_weekly_mean = float(np.log1p(max(0.0, median_weekly)))

    lw_mn = last_min if last_min is not None else nan
    lw_mx = last_max if last_max is not None else nan
    log1p_last_min = (
        float(np.log1p(max(0.0, lw_mn))) if last_min is not None else 0.0
    )
    log1p_last_max = (
        float(np.log1p(max(0.0, lw_mx))) if last_max is not None else 0.0
    )
    last_week_spread = (
        float(lw_mx - lw_mn)
        if last_min is not None and last_max is not None
        else 0.0
    )

    intraweek_ranges: list[float] = []
    for i in range(len(means)):
        mn, mx = mins[i], maxs[i]
        if mn is not None and mx is not None:
            intraweek_ranges.append(float(mx - mn))
    mean_intraweek_range_avg = float(np.mean(intraweek_ranges)) if intraweek_ranges else 0.0
    mean_intraweek_range_last = (
        float(last_max - last_min)
        if last_min is not None and last_max is not None
        else 0.0
    )

    max_dd = _max_drawdown_relative(arr)

    coverage = 1.0 if n_weeks >= 4 else 0.0

    core: dict[str, float] = {
        "log1p_last_mean": log1p_last_mean,
        "n_weeks_observed": float(n_weeks),
        "price_slope_4w": slope_4w,
        "price_vol": vol,
        "price_coverage": coverage,
    }

    if mode == "core":
        return core

    ext = {
        **core,
        "log1p_last_min": log1p_last_min,
        "log1p_last_max": log1p_last_max,
        "last_week_spread": last_week_spread,
        "log1p_mean_hist": log1p_mean_hist,
        "log1p_median_weekly_mean": log1p_median_weekly_mean,
        "mean_vs_last_ratio": mean_vs_last_ratio,
        "price_slope_12w": slope_12w,
        "price_slope_full": slope_full,
        "pct_change_first_to_last": pct_change,
        "price_cv": cv,
        "price_iqr": iqr,
        "max_drawdown_mean": max_dd,
        "weeks_since_last_obs": weeks_since_last,
        "calendar_span_weeks": calendar_span_weeks,
        "trailing_null_frac_12w": trailing_null_frac_12w,
        "mean_intraweek_range_last": mean_intraweek_range_last,
        "mean_intraweek_range_avg": mean_intraweek_range_avg,
    }
    return ext


def _empty_price_features(mode: PriceFeatureMode) -> dict[str, float]:
    core_names = (
        "log1p_last_mean",
        "n_weeks_observed",
        "price_slope_4w",
        "price_vol",
        "price_coverage",
    )
    ext_names = (
        "log1p_last_min",
        "log1p_last_max",
        "last_week_spread",
        "log1p_mean_hist",
        "log1p_median_weekly_mean",
        "mean_vs_last_ratio",
        "price_slope_12w",
        "price_slope_full",
        "pct_change_first_to_last",
        "price_cv",
        "price_iqr",
        "max_drawdown_mean",
        "weeks_since_last_obs",
        "calendar_span_weeks",
        "trailing_null_frac_12w",
        "mean_intraweek_range_last",
        "mean_intraweek_range_avg",
    )
    out = {k: 0.0 for k in core_names}
    if mode == "extended":
        for k in ext_names:
            out[k] = 0.0
    return out


def price_features_for_bgg_id(
    bgg_id: str,
    *,
    bgo: BgoMappingResult,
    price_histories_root: Path,
    as_of: datetime | None,
    mode: PriceFeatureMode,
) -> dict[str, float]:
    key = bgo.bgg_to_key.get(bgg_id)
    if not key:
        return _empty_price_features(mode)
    path = price_histories_root / f"{key}.json"
    series = load_price_series(path)
    return compute_price_features_for_series(series, as_of=as_of, mode=mode)


SCALAR_COLUMNS_PRICE_CORE: tuple[str, ...] = (
    "log1p_last_mean",
    "n_weeks_observed",
    "price_slope_4w",
    "price_vol",
    "price_coverage",
)

SCALAR_COLUMNS_PRICE_EXTENDED_ONLY: tuple[str, ...] = tuple(
    k for k in _empty_price_features("extended") if k not in SCALAR_COLUMNS_PRICE_CORE
)

PRICE_EXT = SCALAR_COLUMNS_PRICE_EXTENDED_ONLY
