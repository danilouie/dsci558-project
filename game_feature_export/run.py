"""Build features_per_game.parquet from embedding artifacts, optional sentiment, + neo4j import."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from embeddings.layout import ArtifactPaths

from game_feature_export.aggregate import aggregate_one_game, run_length_group_boundaries
from game_feature_export.bgg_tabular import (
    load_games_by_bgg_id,
    load_ranks_by_bgg_id,
    merge_tabular_for_bgg_id,
)
from game_feature_export.collection_counts import (
    collection_shares,
    load_collection_counts_by_bgg_id,
)
from game_feature_export.concepts import encode_concept_pair
from game_feature_export.io import (
    add_reviewer_column,
    fetch_vectors,
    filter_review_rows_with_bgg_id,
    join_sentiment,
    load_all_bgg_username_maps,
    load_bgq_review_author_map,
    load_concept_vectors,
    load_game_description_faiss_map,
    load_id_map_table,
    load_sentiment_by_faiss_id,
    open_vectors_mmap,
    read_embedding_meta,
)
from game_feature_export.price_features import (
    PRICE_EXT,
    SCALAR_COLUMNS_PRICE_CORE,
    load_bgo_bgg_to_key,
    price_features_for_bgg_id,
)
from game_feature_export.schema import (
    EPS_POS_NEG,
    EPS_SENT_STD,
    POS_NEG_RATIO_CAP,
    FEATURE_VERSION_DEFAULT,
    SCALAR_COLUMNS_BGG_TABULAR,
    SCALAR_COLUMNS_COLLECTION_SHARES,
    SCALAR_COLUMNS_CONCEPT,
    SCALAR_COLUMNS_EMBEDDING,
    SCALAR_COLUMNS_EXTENDED,
    SCALAR_COLUMNS_SENTIMENT,
    STRING_COLUMNS_BGG_TABULAR,
    STRING_COLUMNS_SPLITS,
)
from game_feature_export.splits import (
    assign_stage_a_split,
    build_stage_c_splits,
    compute_bgq_review_bgg_ids_from_id_map,
    load_splits_json_optional,
)

PriceFeatureMode = Literal["core", "extended"]


def _sort_indices_bgg_id(tbl: pa.Table) -> pa.Table:
    idx = pc.sort_indices(tbl, sort_keys=[("bgg_id", "ascending")])
    return tbl.take(idx)


def build_per_game_features(
    *,
    embedding_root: Path,
    neo4j_import: Path,
    sentiment_parquet: Path | None,
    output_dir: Path,
    good_vec_path: Path | None = None,
    bad_vec_path: Path | None = None,
    good_value_text: str | None = None,
    bad_value_text: str | None = None,
    concept_encoder_model: str | None = None,
    concept_encoder_device: str | None = None,
    extended: bool = False,
    run_id: str | None = None,
    reviewer_missing_policy: str = "empty_string",
    include_sentiment_features: bool = False,
    repo_root: Path | None = None,
    bgo_map_path: Path | None = None,
    price_histories_dir: Path | None = None,
    price_as_of: datetime | None = None,
    price_features_mode: PriceFeatureMode = "extended",
    stage_a_extra_test_fraction: float = 0.0,
    split_seed: int = 42,
    splits_json_path: Path | None = None,
    write_splits_json: bool = True,
    skip_price_features: bool = False,
    skip_bgg_tabular: bool = False,
    skip_collection_features: bool = False,
    skip_description_embedding: bool = False,
    skip_splits: bool = False,
) -> Path:
    """
    Write features_per_game.parquet and run_meta.json under output_dir.

    Optional enrichment (defaults on): BGG CSV tabulars, description embedding,
    BGO prices, stage_a_split / stage_c_split (writes splits.json unless disabled).
    """
    embedding_root = embedding_root.resolve()
    neo4j_import = neo4j_import.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    root = repo_root.resolve() if repo_root is not None else Path.cwd().resolve()

    if reviewer_missing_policy not in ("empty_string",):
        raise ValueError(
            f"Unsupported reviewer_missing_policy={reviewer_missing_policy!r}; "
            "only 'empty_string' is implemented (missing author/username -> '')."
        )

    if extended and not include_sentiment_features:
        raise ValueError("--extended requires sentiment features; pass --include-sentiment-features.")

    meta = read_embedding_meta(embedding_root)
    dim = meta.embedding_dim
    paths = ArtifactPaths(root=embedding_root)

    id_tbl = load_id_map_table(embedding_root)
    id_tbl = filter_review_rows_with_bgg_id(id_tbl)
    if include_sentiment_features:
        if sentiment_parquet is None:
            raise ValueError("sentiment_parquet is required when include_sentiment_features is True.")
        sent_tbl = load_sentiment_by_faiss_id(sentiment_parquet)
        joined = join_sentiment(id_tbl, sent_tbl)
    else:
        joined = id_tbl

    bgq_map = load_bgq_review_author_map(neo4j_import / "reviews.csv")
    bgg_map = load_all_bgg_username_maps(neo4j_import)
    joined = add_reviewer_column(joined, bgq_map=bgq_map, bgg_map=bgg_map)

    sorted_tbl = _sort_indices_bgg_id(joined)
    bgg_ids_py = sorted_tbl.column("bgg_id").to_pylist()
    faiss_py = sorted_tbl.column("faiss_id").to_pylist()
    rev_py = sorted_tbl.column("reviewer_id").to_pylist()
    keys = np.array([str(x) for x in bgg_ids_py], dtype=object)
    faiss_arr = np.asarray(faiss_py, dtype=np.int64)
    sent_arr = (
        np.asarray(sorted_tbl.column("sentiment_score").to_pylist(), dtype=np.float64)
        if include_sentiment_features
        else np.empty((0,), dtype=np.float64)
    )
    n_total = len(keys)

    vectors_mm, faiss_index = open_vectors_mmap(embedding_root)

    desc_faiss_map: dict[str, int] = {}
    if not skip_description_embedding:
        desc_faiss_map = load_game_description_faiss_map(embedding_root)

    games_tbl: dict[str, dict[str, Any]] = {}
    ranks_tbl: dict[str, dict[str, Any]] = {}
    if not skip_bgg_tabular:
        games_tbl = load_games_by_bgg_id(neo4j_import / "games.csv")
        ranks_tbl = load_ranks_by_bgg_id(neo4j_import / "ranks.csv")

    coll_by_bgg: dict[str, tuple[int, int, int, int]] = {}
    coll_loader_meta: dict[str, Any] = {}
    if not skip_collection_features:
        coll_by_bgg, coll_loader_meta = load_collection_counts_by_bgg_id(neo4j_import)

    bgo = load_bgo_bgg_to_key((bgo_map_path or root / "bgo_key_bgg_map.tsv").resolve())
    price_root = (price_histories_dir or root / "price_histories").resolve()

    id_map_parquet_path = ArtifactPaths(root=embedding_root).id_map_parquet
    bgq_from_id_map = compute_bgq_review_bgg_ids_from_id_map(id_map_parquet_path)

    stage_c_map: dict[str, str] = {}
    stage_c_meta: dict[str, Any] = {}
    if not skip_splits:
        loaded = load_splits_json_optional(splits_json_path)
        if loaded:
            stage_c_map = loaded
            stage_c_meta = {"source": str(splits_json_path), "loaded": True}
        else:
            out_splits = output_dir / "splits.json" if write_splits_json else None
            stage_c_map, stage_c_meta = build_stage_c_splits(
                neo4j_import / "reviews.csv",
                seed=split_seed,
                splits_json_out=out_splits,
            )

    gt = (good_value_text or "").strip()
    bt = (bad_value_text or "").strip()
    has_text = bool(gt and bt)
    has_npy = good_vec_path is not None and bad_vec_path is not None
    partial_text = bool(gt or bt) and not has_text
    partial_npy = (good_vec_path is not None) ^ (bad_vec_path is not None)
    if partial_text:
        raise ValueError("Provide both --good-value-text and --bad-value-text, or neither.")
    if partial_npy:
        raise ValueError("Provide both --good-vec and --bad-vec, or neither.")
    if has_text and has_npy:
        raise ValueError("Use either concept text (--good-value-text/--bad-value-text) or .npy vectors, not both.")

    good_vec: np.ndarray | None
    bad_vec: np.ndarray | None
    concept_source: str
    enc_model_used: str | None = None
    if has_text:
        enc_model_used = concept_encoder_model or meta.model_name
        good_vec, bad_vec = encode_concept_pair(
            model_name=enc_model_used,
            good_text=gt,
            bad_text=bt,
            normalize_embeddings=meta.normalize,
            device=concept_encoder_device,
        )
        concept_on = True
        concept_source = "encoded_text"
    else:
        good_vec, bad_vec = load_concept_vectors(good_vec_path, bad_vec_path)
        concept_on = good_vec is not None and bad_vec is not None
        concept_source = "npy_files" if concept_on else "none"

    if concept_on and (good_vec.shape[0] != dim or bad_vec.shape[0] != dim):
        raise ValueError(
            f"Concept vectors dim {good_vec.shape[0]} / {bad_vec.shape[0]} != meta.embedding_dim {dim}"
        )

    boundaries = run_length_group_boundaries(keys)
    rows: list[dict[str, Any]] = []
    for lo, hi, bgg_id in boundaries:
        sl = slice(lo, hi)
        fid_chunk = faiss_arr[sl]
        emb = fetch_vectors(fid_chunk, vectors_mm=vectors_mm, faiss_index=faiss_index, dim=dim)
        s_chunk = sent_arr[sl] if include_sentiment_features else None
        r_chunk = [rev_py[i] for i in range(lo, hi)]
        feat = aggregate_one_game(
            emb,
            s_chunk,
            r_chunk,
            good_vec=good_vec if concept_on else None,
            bad_vec=bad_vec if concept_on else None,
            extended=extended,
            include_sentiment_features=include_sentiment_features,
        )
        bid = str(bgg_id)
        feat["bgg_id"] = bid

        tab = merge_tabular_for_bgg_id(bid, games_tbl, ranks_tbl)
        for k in SCALAR_COLUMNS_BGG_TABULAR:
            v = tab[k]
            feat[k] = float(v) if isinstance(v, (int, float)) else float("nan")
        for k in STRING_COLUMNS_BGG_TABULAR:
            feat[k] = str(tab.get(k) or "")

        if skip_description_embedding:
            feat["has_description_embedding"] = 0.0
            feat["description_embedding"] = [0.0] * dim
        elif bid in desc_faiss_map:
            dvec = fetch_vectors(
                np.asarray([desc_faiss_map[bid]], dtype=np.int64),
                vectors_mm=vectors_mm,
                faiss_index=faiss_index,
                dim=dim,
            )
            feat["description_embedding"] = dvec[0].astype(np.float32).tolist()
            feat["has_description_embedding"] = 1.0
        else:
            feat["description_embedding"] = [0.0] * dim
            feat["has_description_embedding"] = 0.0

        if not skip_price_features:
            pf = price_features_for_bgg_id(
                bid,
                bgo=bgo,
                price_histories_root=price_root,
                as_of=price_as_of,
                mode=price_features_mode,
            )
            feat.update(pf)

        if skip_collection_features:
            for name in SCALAR_COLUMNS_COLLECTION_SHARES:
                feat[name] = float("nan")
        else:
            o, w, wb, wt = coll_by_bgg.get(bid, (0, 0, 0, 0))
            shares = collection_shares(o, w, wb, wt)
            for name, v in zip(SCALAR_COLUMNS_COLLECTION_SHARES, shares):
                feat[name] = v

        rows.append(feat)

    if not rows:
        raise ValueError(
            "No per-game rows produced: check id_map review rows (and sentiment overlap if using "
            "--include-sentiment-features) and non-empty bgg_id."
        )

    export_ids = [r["bgg_id"] for r in rows]
    stage_a_assign: dict[str, str] = {}
    if not skip_splits:
        stage_a_assign = assign_stage_a_split(
            export_ids,
            bgq_review_bgg_ids=bgq_from_id_map,
            seed=split_seed,
            extra_test_fraction=stage_a_extra_test_fraction,
        )
    for r in rows:
        bid = r["bgg_id"]
        if skip_splits:
            r["stage_a_split"] = "train"
            r["stage_c_split"] = "none"
        else:
            r["stage_a_split"] = stage_a_assign.get(bid, "train")
            r["stage_c_split"] = stage_c_map.get(bid, "none")

    d = dim
    list_type = pa.list_(pa.float32(), list_size=d)
    mean_col = pa.array([r["mean_embedding"] for r in rows], type=list_type)
    desc_col = pa.array([r["description_embedding"] for r in rows], type=list_type)

    scalar_names = list(SCALAR_COLUMNS_EMBEDDING)
    if include_sentiment_features:
        scalar_names += list(SCALAR_COLUMNS_SENTIMENT)
    if concept_on:
        scalar_names += list(SCALAR_COLUMNS_CONCEPT)
    if extended:
        scalar_names += list(SCALAR_COLUMNS_EXTENDED)

    scalar_names += list(SCALAR_COLUMNS_BGG_TABULAR)
    scalar_names += list(SCALAR_COLUMNS_COLLECTION_SHARES)
    scalar_names += ["has_description_embedding"]
    if not skip_price_features:
        scalar_names += list(SCALAR_COLUMNS_PRICE_CORE)
        if price_features_mode == "extended":
            scalar_names += list(PRICE_EXT)

    cols_dict: dict[str, Any] = {
        "bgg_id": pa.array([r["bgg_id"] for r in rows], type=pa.large_string()),
        "mean_embedding": mean_col,
        "description_embedding": desc_col,
    }
    for name in scalar_names:
        cols_dict[name] = pa.array([r[name] for r in rows])

    for name in STRING_COLUMNS_BGG_TABULAR:
        cols_dict[name] = pa.array([r[name] for r in rows], type=pa.large_string())
    if not skip_splits:
        for name in STRING_COLUMNS_SPLITS:
            cols_dict[name] = pa.array([r[name] for r in rows], type=pa.large_string())

    out_tbl = pa.table(cols_dict)

    out_parquet = output_dir / "features_per_game.parquet"
    pq.write_table(out_tbl, out_parquet)

    rid = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    run_meta = {
        "run_id": rid,
        "feature_version": FEATURE_VERSION_DEFAULT,
        "extended_features": extended,
        "concept_features": concept_on,
        "concept_source": concept_source,
        "concept_encoder_model": enc_model_used,
        "concept_encoder_device": concept_encoder_device,
        "good_value_text": gt if has_text else None,
        "bad_value_text": bt if has_text else None,
        "good_vec_path": str(good_vec_path) if good_vec_path else None,
        "bad_vec_path": str(bad_vec_path) if bad_vec_path else None,
        "embedding_root": str(embedding_root),
        "neo4j_import": str(neo4j_import),
        "repo_root": str(root),
        "include_sentiment_features": include_sentiment_features,
        "sentiment_parquet": str(sentiment_parquet) if sentiment_parquet else None,
        "embedding_meta": json.loads(paths.meta_json.read_text(encoding="utf-8")),
        "epsilon_pos_neg": EPS_POS_NEG,
        "epsilon_sent_std": EPS_SENT_STD,
        "sentiment_distribution_ratio_denominator": "sentiment_std (no epsilon when std>0)",
        "pos_neg_ratio_cap": POS_NEG_RATIO_CAP,
        "sentiment_std_ddof": 1,
        "sentiment_median_method": "numpy.median",
        "percentile_method": "linear (numpy.percentile)",
        "reviewer_missing_policy": reviewer_missing_policy,
        "rows_written": len(rows),
        "joined_review_rows": int(n_total),
        "scalar_columns": (
            ["mean_embedding", "description_embedding", *scalar_names, *STRING_COLUMNS_BGG_TABULAR]
            + (list(STRING_COLUMNS_SPLITS) if not skip_splits else [])
        ),
        "schema_ref": "neo4j/SCHEMA.md",
        "collection_share_formula": (
            "coll_share_* = count_* / (owns + wants + wants_to_buy + wants_to_trade) per bgg_id"
        ),
        "collection_csv_meta": {} if skip_collection_features else coll_loader_meta,
        "bgo_duplicate_bgg_ids": bgo.duplicate_bgg_ids,
        "price_features_mode": None if skip_price_features else price_features_mode,
        "price_as_of_utc": price_as_of.isoformat() if price_as_of else None,
        "bgo_map_path": str((bgo_map_path or root / "bgo_key_bgg_map.tsv").resolve()),
        "price_histories_dir": str(price_root),
        "split_seed": split_seed,
        "stage_a_extra_test_fraction": stage_a_extra_test_fraction,
        "stage_c_splits_meta": stage_c_meta if not skip_splits else {},
        "splits_json_used": str(splits_json_path) if splits_json_path else None,
        "skip_price_features": skip_price_features,
        "skip_bgg_tabular": skip_bgg_tabular,
        "skip_collection_features": skip_collection_features,
        "skip_description_embedding": skip_description_embedding,
        "skip_splits": skip_splits,
    }
    (output_dir / "run_meta.json").write_text(
        json.dumps(run_meta, indent=2) + "\n", encoding="utf-8"
    )
    return out_parquet
