"""FAISS embedding index aligned with neo4j/SCHEMA.md primary keys."""

from __future__ import annotations

from embeddings.layout import ArtifactPaths, EmbeddingMeta

__all__ = [
    "ArtifactPaths",
    "EmbeddingMeta",
]


def __getattr__(name: str):
    if name == "FaissNeo4jIndex":
        from embeddings.search import FaissNeo4jIndex

        return FaissNeo4jIndex
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
