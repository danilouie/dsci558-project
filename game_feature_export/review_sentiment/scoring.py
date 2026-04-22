"""Map HuggingFace sentiment pipeline outputs to a single float score per review."""

from __future__ import annotations

from typing import Any


def sentiment_dict_to_score(result: dict[str, Any]) -> float:
    """
    Map one pipeline result dict (label + score) to [-1, 1]-ish float.

    Heuristic: NEG* -> -score, POS* -> +score, else 0 (neutral / unknown).
    ``score`` is model confidence in (0, 1] for many pipelines.
    """
    label = str(result.get("label", "")).upper()
    score = float(result.get("score", 0.0))
    if "NEG" in label:
        return -abs(score)
    if "POS" in label:
        return abs(score)
    return 0.0


def batch_scores(results: list[dict[str, Any]]) -> list[float]:
    return [sentiment_dict_to_score(r) for r in results]
