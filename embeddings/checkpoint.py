from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from embeddings.documents import EmbeddingDocument
from embeddings.layout import ArtifactPaths


CHECKPOINT_VERSION = 1


def fingerprint_documents(docs: list[EmbeddingDocument]) -> str:
    """Stable hash of document order and content (text_sha256 per row)."""
    lines: list[str] = []
    for d in docs:
        lines.append(f"{d.doc_kind}\t{d.sort_key}\t{d.text_sha256()}")
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def write_manifest(artifacts: ArtifactPaths, docs: list[EmbeddingDocument]) -> None:
    n = len(docs)
    tbl = pa.table(
        {
            "faiss_id": list(range(n)),
            "doc_kind": [d.doc_kind for d in docs],
            "review_id": [d.review_id for d in docs],
            "bgg_review_id": [d.bgg_review_id for d in docs],
            "bgg_id": [d.bgg_id for d in docs],
            "text_sha256": [d.text_sha256() for d in docs],
        }
    )
    pq.write_table(tbl, artifacts.manifest_parquet)


@dataclass(frozen=True)
class BuildCheckpoint:
    version: int
    documents_fingerprint: str
    total_rows: int
    completed_rows: int
    embedding_dim: int
    model_name: str
    neo4j_import: str
    limit_documents: int | None
    include_games: bool
    include_bgq: bool
    include_bgg_reviews: bool
    batch_size: int
    index_mode: str
    hnsw_m: int
    shard_rows: int
    store_text_in_shards: bool
    encoder_device: str

    def to_json(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "documents_fingerprint": self.documents_fingerprint,
            "total_rows": self.total_rows,
            "completed_rows": self.completed_rows,
            "embedding_dim": self.embedding_dim,
            "model_name": self.model_name,
            "neo4j_import": self.neo4j_import,
            "limit_documents": self.limit_documents,
            "include_games": self.include_games,
            "include_bgq": self.include_bgq,
            "include_bgg_reviews": self.include_bgg_reviews,
            "batch_size": self.batch_size,
            "index_mode": self.index_mode,
            "hnsw_m": self.hnsw_m,
            "shard_rows": self.shard_rows,
            "store_text_in_shards": self.store_text_in_shards,
            "encoder_device": self.encoder_device,
        }

    @staticmethod
    def from_json(data: dict[str, Any]) -> BuildCheckpoint:
        return BuildCheckpoint(
            version=int(data.get("version", 1)),
            documents_fingerprint=str(data["documents_fingerprint"]),
            total_rows=int(data["total_rows"]),
            completed_rows=int(data["completed_rows"]),
            embedding_dim=int(data["embedding_dim"]),
            model_name=str(data["model_name"]),
            neo4j_import=str(data["neo4j_import"]),
            limit_documents=(
                int(data["limit_documents"])
                if data.get("limit_documents") is not None
                else None
            ),
            include_games=bool(data["include_games"]),
            include_bgq=bool(data["include_bgq"]),
            include_bgg_reviews=bool(data["include_bgg_reviews"]),
            batch_size=int(data["batch_size"]),
            index_mode=str(data["index_mode"]),
            hnsw_m=int(data["hnsw_m"]),
            shard_rows=int(data["shard_rows"]),
            store_text_in_shards=bool(data["store_text_in_shards"]),
            encoder_device=str(data.get("encoder_device", "cpu")),
        )


def save_checkpoint(path: Path, ckpt: BuildCheckpoint) -> None:
    path.write_text(json.dumps(ckpt.to_json(), indent=2) + "\n", encoding="utf-8")


def load_checkpoint(path: Path) -> BuildCheckpoint:
    data = json.loads(path.read_text(encoding="utf-8"))
    return BuildCheckpoint.from_json(data)


def delete_checkpoint(path: Path) -> None:
    if path.is_file():
        path.unlink()
