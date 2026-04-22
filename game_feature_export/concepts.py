"""Encode good/bad value concept strings with the same encoder family as the FAISS index."""

from __future__ import annotations

import numpy as np


def encode_concept_pair(
    *,
    model_name: str,
    good_text: str,
    bad_text: str,
    normalize_embeddings: bool,
    device: str | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Return (good_vec, bad_vec) each shape (d,) float32, using sentence-transformers
    with the same settings as the embedding build (model + normalize flag).
    """
    from sentence_transformers import SentenceTransformer  # noqa: PLC0415

    model = SentenceTransformer(model_name, device=device or "cpu")
    texts = [good_text.strip(), bad_text.strip()]
    if not texts[0] or not texts[1]:
        raise ValueError("good_text and bad_text must be non-empty after stripping")
    emb = model.encode(
        texts,
        normalize_embeddings=normalize_embeddings,
        convert_to_numpy=True,
        show_progress_bar=False,
    )
    g = np.asarray(emb[0], dtype=np.float32).reshape(-1)
    b = np.asarray(emb[1], dtype=np.float32).reshape(-1)
    return g, b
