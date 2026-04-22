"""Create tiny synthetic artifacts and run build_per_game_features (for local validation)."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from embeddings.layout import ArtifactPaths, EmbeddingMeta

from game_feature_export.run import build_per_game_features


def main() -> None:
    d = 8
    # faiss_id 0,1 -> game 100; 2,3,4 -> game 200; 5 -> game 300 (n=1 edge case)
    id_rows = pa.table(
        {
            "faiss_id": pa.array([0, 1, 2, 3, 4, 5], type=pa.int64()),
            "doc_kind": pa.array(
                [
                    "bgq_review",
                    "bgq_review",
                    "bgg_review",
                    "bgg_review",
                    "bgg_review",
                    "bgq_review",
                ]
            ),
            "review_id": pa.array(["r0", "r1", None, None, None, "r_single"]),
            "bgg_review_id": pa.array([None, None, "b0", "b1", "b2", None]),
            "bgg_id": pa.array(["100", "100", "200", "200", "200", "300"]),
            "text_sha256": pa.array(["a", "b", "c", "d", "e", "f"]),
        }
    )
    rng = np.random.default_rng(0)
    vecs = rng.standard_normal((6, d)).astype(np.float32)
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    vecs = (vecs / norms).astype(np.float32)

    sent = pa.table(
        {
            "faiss_id": pa.array([0, 1, 2, 3, 4, 5], type=pa.int64()),
            "sentiment_score": pa.array(
                [0.5, -0.2, 0.0, 0.8, -0.5, 0.3], type=pa.float32()
            ),
        }
    )

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "emb"
        paths = ArtifactPaths(root=root)
        paths.ensure_dirs()
        pq.write_table(id_rows, paths.id_map_parquet)
        np.save(paths.vectors_npy, vecs)
        meta = EmbeddingMeta(
            model_name="test-model",
            embedding_dim=d,
            normalize=True,
            faiss_index_type="IndexFlatIP",
            metric="inner_product",
            num_vectors=6,
        )
        paths.write_meta(meta)

        sent_path = Path(td) / "sent.parquet"
        pq.write_table(sent, sent_path)

        neo = Path(td) / "neo4j"
        neo.mkdir()
        # empty reviewer maps
        (neo / "reviews.csv").write_text("review_id,author\n", encoding="utf-8")
        (neo / "bgg_reviews.tsv").write_text("bgg_review_id\tusername\n", encoding="utf-8")
        coll_hdr = "owner_username,bgg_id,collid,num_plays,last_modified,name\n"
        # Game 100: o=2, w=wb=wt=1 -> shares 2/5, 1/5, 1/5, 1/5
        (neo / "user_game_owns.csv").write_text(
            coll_hdr + "u1,100,,,,,\nu2,100,,,,,\n",
            encoding="utf-8",
        )
        (neo / "user_game_wants.csv").write_text(coll_hdr + "u3,100,,,,,\n", encoding="utf-8")
        (neo / "user_game_wants_to_buy.csv").write_text(coll_hdr + "u4,100,,,,,\n", encoding="utf-8")
        (neo / "user_game_wants_to_trade.csv").write_text(coll_hdr + "u5,100,,,,,\n", encoding="utf-8")

        out_dir = Path(td) / "out"
        p = build_per_game_features(
            embedding_root=root,
            neo4j_import=neo,
            sentiment_parquet=sent_path,
            output_dir=out_dir,
            extended=True,
            run_id="smoke",
            include_sentiment_features=True,
        )
        tbl = pq.read_table(p)
        assert tbl.num_rows == 3, tbl.num_rows
        ids = tbl.column("bgg_id").to_pylist()
        yi = ids.index("100")
        assert abs(tbl.column("coll_share_owns")[yi].as_py() - 0.4) < 1e-9
        assert abs(tbl.column("coll_share_wants")[yi].as_py() - 0.2) < 1e-9
        assert abs(tbl.column("coll_share_wtb")[yi].as_py() - 0.2) < 1e-9
        assert abs(tbl.column("coll_share_wtt")[yi].as_py() - 0.2) < 1e-9
        meta_out = json.loads((out_dir / "run_meta.json").read_text(encoding="utf-8"))
        assert meta_out["rows_written"] == 3
        assert meta_out["extended_features"] is True
        print("smoke_test OK:", p)


if __name__ == "__main__":
    main()
