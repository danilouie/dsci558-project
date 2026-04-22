"""Per-bgg_id aggregation: embeddings + sentiment + reviewer + optional concepts."""

from __future__ import annotations

from typing import Any

import numpy as np

from game_feature_export.schema import EPS_POS_NEG, EPS_SENT_STD, POS_NEG_RATIO_CAP


def _percentiles_linear(x: np.ndarray, qs: list[float]) -> np.ndarray:
    """Return quantiles for qs in [0,100] (NumPy ``method='linear'``)."""
    if x.size == 0:
        return np.zeros(len(qs), dtype=np.float64)
    return np.percentile(x, qs, method="linear")


def aggregate_one_game(
    embeddings: np.ndarray,
    sentiment: np.ndarray | None,
    reviewer_ids: list[str],
    *,
    good_vec: np.ndarray | None,
    bad_vec: np.ndarray | None,
    extended: bool,
    include_sentiment_features: bool,
) -> dict[str, Any]:
    """
    embeddings: (n, d) float32
    sentiment: (n,) float64/float32 when ``include_sentiment_features``; else ignored (pass ``None``).
    reviewer_ids: parallel list of str
    """
    n, d = embeddings.shape
    if len(reviewer_ids) != n:
        raise ValueError("embeddings and reviewer_ids length mismatch")
    if include_sentiment_features:
        if sentiment is None:
            raise ValueError("sentiment required when include_sentiment_features is True")
        if sentiment.shape[0] != n:
            raise ValueError("embeddings and sentiment length mismatch")

    s = np.asarray(sentiment, dtype=np.float64) if include_sentiment_features else None
    mean_emb = np.mean(embeddings.astype(np.float64), axis=0)
    deltas = np.linalg.norm(embeddings.astype(np.float64) - mean_emb, axis=1)
    dispersion = float(np.mean(deltas)) if n else 0.0
    med_dist = float(np.median(deltas)) if n else 0.0
    max_dist = float(np.max(deltas)) if n else 0.0

    if include_sentiment_features and s is not None:
        if n == 1:
            sent_std = 0.0
        else:
            sent_std = float(np.std(s, ddof=1))

        sent_mean = float(np.mean(s)) if n else 0.0
        sent_min = float(np.min(s)) if n else 0.0
        sent_max = float(np.max(s)) if n else 0.0
        sent_med = float(np.median(s)) if n else 0.0

        pos_n = int(np.sum(s > 0.0))
        neg_n = int(np.sum(s < 0.0))
        neu_n = int(np.sum(s == 0.0))
        pos_fraction = pos_n / n if n else 0.0
        neg_fraction = neg_n / n if n else 0.0
        pos_neg_ratio = min(
            pos_fraction / (neg_fraction + EPS_POS_NEG), POS_NEG_RATIO_CAP
        )

        if n == 1 or sent_std == 0.0:
            sent_dist_ratio = 0.0
        else:
            sent_dist_ratio = float((sent_max - sent_min) / sent_std)
    else:
        neu_n = 0

    # Reviewer concentration: only non-empty reviewer ids
    known = [u for u in reviewer_ids if (u or "").strip()]
    if not known:
        n_unique = 0
        rpm_mean = 0.0
        rpm_std = 0.0
    else:
        uniq: dict[str, int] = {}
        for raw_u in known:
            ukey = raw_u.strip()
            uniq[ukey] = uniq.get(ukey, 0) + 1
        counts = np.array(list(uniq.values()), dtype=np.float64)
        n_unique = int(len(counts))
        rpm_mean = float(np.mean(counts))
        if len(counts) <= 1:
            rpm_std = 0.0
        else:
            rpm_std = float(np.std(counts, ddof=1))

    out: dict[str, Any] = {
        "mean_embedding": mean_emb.astype(np.float32).tolist(),
        "n_reviews": int(n),
        "review_count_feature": float(np.log1p(n)),
        "dispersion": dispersion,
        "median_distance_to_mean": med_dist,
        "max_distance_to_mean": max_dist,
        "n_unique_reviewers": n_unique,
        "reviews_per_user_mean": rpm_mean,
        "reviews_per_user_std": rpm_std,
    }

    if include_sentiment_features and s is not None:
        out["sentiment_mean"] = sent_mean
        out["sentiment_std"] = sent_std
        out["sentiment_min"] = sent_min
        out["sentiment_max"] = sent_max
        out["sentiment_median"] = sent_med
        out["pos_fraction"] = float(pos_fraction)
        out["neg_fraction"] = float(neg_fraction)
        out["pos_neg_ratio"] = float(pos_neg_ratio)
        out["sentiment_distribution_ratio"] = float(sent_dist_ratio)

    if good_vec is not None and bad_vec is not None:
        g = good_vec.astype(np.float64).reshape(1, -1)
        b = bad_vec.astype(np.float64).reshape(1, -1)
        e = embeddings.astype(np.float64)
        # inner product = cosine similarity when rows are L2-normalized (meta.normalize)
        v = (e @ g.T).ravel() - (e @ b.T).ravel()
        out["value_score_mean"] = float(np.mean(v))
        out["value_score_std"] = 0.0 if n == 1 else float(np.std(v, ddof=1))

    if extended:
        if not include_sentiment_features or s is None:
            raise ValueError("extended sentiment columns require include_sentiment_features=True")
        p25, _, p75 = _percentiles_linear(s, [25.0, 50.0, 75.0]).tolist()
        neutral_fraction = neu_n / n if n else 0.0
        out["sentiment_p25"] = float(p25)
        out["sentiment_p75"] = float(p75)
        out["neutral_fraction"] = float(neutral_fraction)
        out["sentiment_iqr"] = float(p75 - p25)

    return out


def run_length_group_boundaries(keys: np.ndarray) -> list[tuple[int, int, Any]]:
    """Contiguous runs of equal keys; **keys must be sorted** (see ``run.py``)."""
    if keys.size == 0:
        return []
    if keys.size > 1:
        ks = keys.tolist()
        if any(ks[i] < ks[i - 1] for i in range(1, len(ks))):
            raise ValueError("run_length_group_boundaries: keys are not sorted ascending")
    boundaries: list[tuple[int, int, Any]] = []
    start = 0
    cur = keys[0]
    for i in range(1, len(keys)):
        if keys[i] != cur:
            boundaries.append((start, i, cur))
            start = i
            cur = keys[i]
    boundaries.append((start, len(keys), cur))
    return boundaries
