from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FAISS_INDEX_MODES = ("flat", "hnsw")  # IndexFlatIP or IndexHNSWFlat


@dataclass
class EmbeddingMeta:
    """Written to meta.json alongside FAISS artifacts."""

    model_name: str
    embedding_dim: int
    normalize: bool
    faiss_index_type: str
    metric: str
    num_vectors: int
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    schema_ref: str = "neo4j/SCHEMA.md"
    hnsw_m: int | None = None

    def to_json(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "model_name": self.model_name,
            "embedding_dim": self.embedding_dim,
            "normalize": self.normalize,
            "faiss_index_type": self.faiss_index_type,
            "metric": self.metric,
            "num_vectors": self.num_vectors,
            "created_at": self.created_at,
            "schema_ref": self.schema_ref,
        }
        if self.hnsw_m is not None:
            d["hnsw_m"] = self.hnsw_m
        return d

    @staticmethod
    def from_json(data: dict[str, Any]) -> EmbeddingMeta:
        return EmbeddingMeta(
            model_name=str(data["model_name"]),
            embedding_dim=int(data["embedding_dim"]),
            normalize=bool(data["normalize"]),
            faiss_index_type=str(data["faiss_index_type"]),
            metric=str(data["metric"]),
            num_vectors=int(data["num_vectors"]),
            created_at=str(data.get("created_at", "")),
            schema_ref=str(data.get("schema_ref", "neo4j/SCHEMA.md")),
            hnsw_m=int(data["hnsw_m"]) if data.get("hnsw_m") is not None else None,
        )


@dataclass(frozen=True)
class ArtifactPaths:
    """On-disk layout under a single versioned directory (e.g. embeddings/bge-small-en-v1.5/)."""

    root: Path

    @property
    def meta_json(self) -> Path:
        return self.root / "meta.json"

    @property
    def manifest_parquet(self) -> Path:
        """Doc manifest written before encoding; same row order as final id_map (resume)."""
        return self.root / "build_manifest.parquet"

    @property
    def checkpoint_json(self) -> Path:
        return self.root / "checkpoint.json"

    @property
    def shards_dir(self) -> Path:
        return self.root / "shards"

    @property
    def id_map_parquet(self) -> Path:
        return self.root / "id_map.parquet"

    @property
    def vectors_npy(self) -> Path:
        return self.root / "vectors.npy"

    @property
    def index_faiss(self) -> Path:
        return self.root / "index.faiss"

    def ensure_dirs(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.shards_dir.mkdir(parents=True, exist_ok=True)

    def write_meta(self, meta: EmbeddingMeta) -> None:
        self.meta_json.write_text(
            json.dumps(meta.to_json(), indent=2) + "\n", encoding="utf-8"
        )

    @staticmethod
    def read_meta(path: Path) -> EmbeddingMeta:
        data = json.loads(path.read_text(encoding="utf-8"))
        return EmbeddingMeta.from_json(data)
