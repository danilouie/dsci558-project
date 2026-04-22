from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from embeddings.documents import normalize_query_text
from embeddings.layout import ArtifactPaths, EmbeddingMeta


@dataclass(frozen=True)
class SearchHit:
    score: float
    faiss_id: int
    doc_kind: str
    review_id: str | None
    bgg_review_id: str | None
    bgg_id: str | None
    text_sha256: str


class FaissNeo4jIndex:
    """
    Load meta.json + index.faiss + id_map.parquet; search with the same SentenceTransformer.
    """

    def __init__(self, artifact_dir: Path, device: str | None = None) -> None:
        import faiss  # noqa: PLC0415
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415

        self.paths = ArtifactPaths(root=artifact_dir.resolve())
        if not self.paths.index_faiss.is_file():
            raise FileNotFoundError(self.paths.index_faiss)
        if not self.paths.id_map_parquet.is_file():
            raise FileNotFoundError(self.paths.id_map_parquet)

        self.meta: EmbeddingMeta = ArtifactPaths.read_meta(self.paths.meta_json)
        self._model = SentenceTransformer(
            self.meta.model_name,
            device=device or "cpu",
        )
        self._index = faiss.read_index(str(self.paths.index_faiss))
        # Improve HNSW retrieval quality by default (ignored for Flat).
        if hasattr(self._index, "hnsw"):
            ef_con = getattr(self._index.hnsw, "efConstruction", 200)
            self._index.hnsw.efSearch = max(64, ef_con // 2)

        tbl = pq.read_table(self.paths.id_map_parquet)
        self._tbl = tbl
        self._cols = {n: tbl.column(n) for n in tbl.column_names}

    def encode_query(self, query: str) -> np.ndarray:
        q = normalize_query_text(query)
        v = self._model.encode(
            [q],
            normalize_embeddings=self.meta.normalize,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        x = np.ascontiguousarray(v.astype(np.float32))
        return x

    def search(self, query: str, k: int = 10) -> list[SearchHit]:
        qv = self.encode_query(query)
        scores, ids = self._index.search(qv, min(k, self._index.ntotal))
        out: list[SearchHit] = []
        for score, fid in zip(scores[0].tolist(), ids[0].tolist()):
            if fid < 0:
                continue
            doc_kind = self._cols["doc_kind"][fid].as_py()
            rid = self._cols["review_id"][fid].as_py()
            brid = self._cols["bgg_review_id"][fid].as_py()
            bgg = self._cols["bgg_id"][fid].as_py()
            th = self._cols["text_sha256"][fid].as_py()
            out.append(
                SearchHit(
                    score=float(score),
                    faiss_id=int(fid),
                    doc_kind=str(doc_kind),
                    review_id=rid if rid else None,
                    bgg_review_id=brid if brid else None,
                    bgg_id=bgg if bgg else None,
                    text_sha256=str(th),
                )
            )
        return out


def search_cli_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Query a FAISS embedding index.")
    parser.add_argument(
        "--artifacts",
        type=Path,
        required=True,
        help="Directory containing meta.json, index.faiss, id_map.parquet",
    )
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("-k", type=int, default=10)
    parser.add_argument(
        "--device",
        type=str,
        default=None,
        help="sentence-transformers device e.g. cpu, mps, cuda",
    )
    args = parser.parse_args(argv)

    idx = FaissNeo4jIndex(args.artifacts, device=args.device)
    hits = idx.search(args.query, k=args.k)
    for h in hits:
        print(
            f"{h.score:.4f}",
            h.doc_kind,
            f"faiss_id={h.faiss_id}",
            f"review_id={h.review_id!r}",
            f"bgg_review_id={h.bgg_review_id!r}",
            f"bgg_id={h.bgg_id!r}",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(search_cli_main())
