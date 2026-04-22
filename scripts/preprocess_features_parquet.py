#!/usr/bin/env python3
"""
Standardize ``features_per_game.parquet`` for ML/DL: RobustScaler or StandardScaler on
tabular columns (fit on train IDs only); optional L2 row-normalization on ``mean_embedding``.

Example:
  python scripts/preprocess_features_parquet.py \\
    --input game_feature_export/artifacts/embed_only/features_per_game.parquet \\
    --output game_feature_export/artifacts/embed_only/features_standardized.parquet \\
    --train-fraction 0.8 \\
    --fit-bgq-review-games \\
    --embedding-root embeddings/all-MiniLM-L6-v2-full \\
    --tabular-scaler robust \\
    --embedding l2 \\
    --nn-safe \\
    --pipeline-out game_feature_export/artifacts/embed_only/preprocess.joblib

Use ``--nn-safe`` for a tight train-set quantile clip (default 99th pct of |scaled FIT|, capped at 4),
or ``--tabular-clip C`` for a fixed bound (stored in the joblib bundle).

``--fit-bgq-review-games`` limits the scaler FIT set to BGQ games only.

``--reserve-bgq-for-test`` fits only on games **without** BGQ reviews (BGQ games are never used
for fitting — use them as evaluation / test). ``--train-fraction`` applies within that non-BGQ pool.

``--match-embedding-tabular-scale`` (default on with ``--nn-safe``) scales the embedding vector so
median row L2 norm matches tabular block norms after scaling (reduces one block dominating concat inputs).
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from embeddings.layout import ArtifactPaths

from game_feature_export.preprocessing import (
    DEFAULT_NN_TABULAR_CLIP_CAP,
    DEFAULT_NN_TABULAR_CLIP_QUANTILE,
    bgq_review_bgg_ids_from_id_map,
    discover_scalar_columns,
    eligible_rows_bgq_games,
    eligible_rows_non_bgq_games,
    fit_ohe_vocab_from_strings,
    fit_preprocess_bundle,
    parquet_safe_ohe_names,
    split_train_fraction,
    split_train_fraction_eligible,
    standardized_table,
    table_to_arrays,
    train_mask_from_ids,
    write_bundle,
    write_preprocess_meta,
)
from game_feature_export.schema import DEFAULT_PRICE_COLUMNS_MEAN_DIVIDE, SUPERVISION_SCALAR_COLUMNS
import numpy as np
import pyarrow.parquet as pq


def _read_train_ids(path: Path) -> set[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return {ln.strip() for ln in lines if ln.strip()}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Standardize features_per_game.parquet (train-only fit)")
    p.add_argument("--input", type=Path, required=True, help="Input features_per_game.parquet")
    p.add_argument("--output", type=Path, required=True, help="Output parquet path")
    p.add_argument(
        "--train-bgg-ids",
        type=Path,
        default=None,
        help="Text file: one bgg_id per line for rows used to FIT the scaler",
    )
    p.add_argument(
        "--train-fraction",
        type=float,
        default=None,
        help="If set (0,1], ignore --train-bgg-ids and take this random fraction as train for fitting",
    )
    p.add_argument("--seed", type=int, default=42, help="RNG seed for --train-fraction")
    p.add_argument(
        "--tabular-scaler",
        choices=("robust", "standard"),
        default="robust",
        help="robust=RobustScaler (recommended for outliers); standard=StandardScaler",
    )
    p.add_argument(
        "--embedding",
        choices=("l2", "raw"),
        default="l2",
        help="l2=L2-normalize mean_embedding rows; raw=leave as-is (float32)",
    )
    p.add_argument(
        "--pipeline-out",
        type=Path,
        default=None,
        help="Write joblib PreprocessBundle for transform() at inference time",
    )
    p.add_argument(
        "--embedding-col",
        type=str,
        default="mean_embedding",
        help="Name of the embedding list column",
    )
    p.add_argument(
        "--embedding-root",
        type=Path,
        default=None,
        help=(
            "Embedding artifact dir (must contain id_map.parquet); required with "
            "--fit-bgq-review-games or --reserve-bgq-for-test"
        ),
    )
    fit_pool = p.add_mutually_exclusive_group()
    fit_pool.add_argument(
        "--fit-bgq-review-games",
        action="store_true",
        help=(
            "Restrict scaler FIT rows to games that have at least one BGQ review in id_map.parquet "
            "(under --embedding-root). With --train-fraction, only those games enter the random split."
        ),
    )
    fit_pool.add_argument(
        "--reserve-bgq-for-test",
        action="store_true",
        help=(
            "Fit scaler only on games that have **no** BGQ review in id_map (under --embedding-root). "
            "Games with BGQ reviews are excluded from the FIT set — use them as test/eval. "
            "Mutually exclusive with --fit-bgq-review-games."
        ),
    )
    p.add_argument(
        "--tabular-clip",
        type=float,
        default=None,
        metavar="C",
        help=(
            "After scaling, clip each tabular feature to [-C, C]. Mutually exclusive with "
            "--tabular-clip-quantile."
        ),
    )
    p.add_argument(
        "--tabular-clip-quantile",
        type=float,
        default=None,
        metavar="Q",
        help=(
            "FIT-only: symmetric clip bound = Q-quantile of absolute scaled tabular values on FIT rows "
            f"(default Q with --nn-safe: {DEFAULT_NN_TABULAR_CLIP_QUANTILE}). "
            "Typical Q=0.99 trims ~1%% tail outliers per dimension pooled. Mutually exclusive with "
            "--tabular-clip."
        ),
    )
    p.add_argument(
        "--tabular-clip-cap",
        type=float,
        default=None,
        metavar="M",
        help=(
            "With --tabular-clip-quantile (including --nn-safe), never exceed symmetric bound M "
            f"(default with --nn-safe only: {DEFAULT_NN_TABULAR_CLIP_CAP}). "
            "Ignored when using fixed --tabular-clip."
        ),
    )
    p.add_argument(
        "--nn-safe",
        action="store_true",
        help=(
            "Neural-net preset when neither --tabular-clip nor --tabular-clip-quantile is set: "
            f"use quantile {DEFAULT_NN_TABULAR_CLIP_QUANTILE} of |scaled FIT tabular|, "
            f"capped at {DEFAULT_NN_TABULAR_CLIP_CAP} (--tabular-clip-cap overrides the cap)."
        ),
    )
    p.add_argument(
        "--match-embedding-tabular-scale",
        dest="match_embedding_tabular_scale",
        action="store_true",
        help=(
            "After L2/raw policy, multiply the embedding vector by a scalar so median train row "
            "L2 norm matches the tabular block (fits on FIT rows only). Reduces concat imbalance for NNs."
        ),
    )
    p.add_argument(
        "--no-match-embedding-tabular-scale",
        dest="match_embedding_tabular_scale",
        action="store_false",
        help="Disable --match-embedding-tabular-scale (overrides --nn-safe default).",
    )
    p.set_defaults(match_embedding_tabular_scale=None)
    p.add_argument(
        "--divide-columns-by-mean",
        type=str,
        default=None,
        metavar="COLS",
        help=(
            "Comma-separated scalar column names: divide raw values by FIT-split column mean "
            "before Robust/Standard scaling (leak-safe)."
        ),
    )
    p.add_argument(
        "--divide-price-columns-by-mean",
        action="store_true",
        help=(
            "Shorthand: divide columns listed in schema DEFAULT_PRICE_COLUMNS_MEAN_DIVIDE "
            "(present in input parquet)."
        ),
    )
    p.add_argument(
        "--skip-one-hot-pipe-fields",
        action="store_true",
        help=(
            "Do not append multi-label one-hot columns from pipe-separated ``categories`` and "
            "``mechanisms`` strings (FIT-split vocabulary by frequency)."
        ),
    )
    p.add_argument(
        "--no-one-hot-categories",
        action="store_true",
        help="With pipe-field one-hot enabled, skip ``categories`` tokens only.",
    )
    p.add_argument(
        "--no-one-hot-mechanisms",
        action="store_true",
        help="With pipe-field one-hot enabled, skip ``mechanisms`` tokens only.",
    )
    p.add_argument(
        "--one-hot-max-category-tokens",
        type=int,
        default=256,
        metavar="K",
        help="Keep top-K category tokens by FIT-split frequency (0 = no limit). Default: 256.",
    )
    p.add_argument(
        "--one-hot-max-mechanism-tokens",
        type=int,
        default=256,
        metavar="K",
        help="Keep top-K mechanism tokens by FIT-split frequency (0 = no limit). Default: 256.",
    )
    args = p.parse_args(argv)

    if args.tabular_clip is not None and args.tabular_clip_quantile is not None:
        p.error("Use only one of --tabular-clip and --tabular-clip-quantile")

    tabular_clip: float | None = args.tabular_clip
    tabular_clip_quantile: float | None = args.tabular_clip_quantile
    tabular_clip_cap: float | None = args.tabular_clip_cap

    if args.nn_safe:
        if tabular_clip is None:
            if tabular_clip_quantile is None:
                tabular_clip_quantile = DEFAULT_NN_TABULAR_CLIP_QUANTILE
            if tabular_clip_cap is None:
                tabular_clip_cap = DEFAULT_NN_TABULAR_CLIP_CAP

    match_emb = args.match_embedding_tabular_scale
    if match_emb is None:
        match_emb = bool(args.nn_safe)

    if (args.fit_bgq_review_games or args.reserve_bgq_for_test) and args.embedding_root is None:
        p.error("--embedding-root is required when --fit-bgq-review-games or --reserve-bgq-for-test is set")

    inp = args.input.resolve()
    if not inp.is_file():
        raise FileNotFoundError(inp)

    tbl = pq.read_table(inp)
    scalar_cols = discover_scalar_columns(tbl, embedding_col=args.embedding_col)
    supervision = set(SUPERVISION_SCALAR_COLUMNS)
    scalar_cols = [c for c in scalar_cols if c not in supervision]

    divide_tuple = tuple(
        x.strip() for x in (args.divide_columns_by_mean or "").split(",") if x.strip()
    )
    if args.divide_price_columns_by_mean:
        present = set(scalar_cols)
        divide_tuple = tuple(
            dict.fromkeys(
                [*divide_tuple, *[c for c in DEFAULT_PRICE_COLUMNS_MEAN_DIVIDE if c in present]]
            )
        )
    if not scalar_cols:
        raise ValueError("No scalar columns found to scale (check parquet schema)")
    X_tab, X_emb, bgg_ids = table_to_arrays(
        tbl, scalar_cols=scalar_cols, embedding_col=args.embedding_col
    )
    d = X_emb.shape[1]

    eligible_mask = np.ones(len(bgg_ids), dtype=bool)
    bgq_feature_row_count = 0
    non_bgq_fit_pool_count = 0
    bgq_set: set[str] | None = None
    if args.fit_bgq_review_games or args.reserve_bgq_for_test:
        id_map = ArtifactPaths(root=args.embedding_root.resolve()).id_map_parquet
        bgq_set = bgq_review_bgg_ids_from_id_map(id_map)
        if args.fit_bgq_review_games:
            eligible_mask = eligible_rows_bgq_games(bgg_ids, bgq_set)
            bgq_feature_row_count = int(eligible_mask.sum())
            if bgq_feature_row_count == 0:
                raise ValueError(
                    "No feature rows match games with BGQ reviews in id_map.parquet; "
                    "check --embedding-root matches the index used for --input."
                )
        else:
            eligible_mask = eligible_rows_non_bgq_games(bgg_ids, bgq_set)
            non_bgq_fit_pool_count = int(eligible_mask.sum())
            bgq_feature_row_count = int(eligible_rows_bgq_games(bgg_ids, bgq_set).sum())
            if non_bgq_fit_pool_count == 0:
                raise ValueError(
                    "No rows left for fitting when excluding BGQ games (--reserve-bgq-for-test); "
                    "check id_map and features_per_game overlap."
                )

    if args.train_fraction is not None:
        if args.fit_bgq_review_games or args.reserve_bgq_for_test:
            train_mask = split_train_fraction_eligible(
                bgg_ids,
                eligible_mask,
                fraction=args.train_fraction,
                seed=args.seed,
            )
        else:
            train_mask = split_train_fraction(bgg_ids, fraction=args.train_fraction, seed=args.seed)
    elif args.train_bgg_ids is not None:
        tid = _read_train_ids(args.train_bgg_ids.resolve())
        train_mask = train_mask_from_ids(bgg_ids, tid)
        if args.fit_bgq_review_games or args.reserve_bgq_for_test:
            train_mask = train_mask & eligible_mask
        if train_mask.sum() == 0:
            raise ValueError(
                "No rows match train selection; check --train-bgg-ids and BGQ pool flags"
            )
    else:
        raise ValueError("Provide either --train-bgg-ids or --train-fraction")

    X_tab_train = X_tab[train_mask]
    if X_tab_train.shape[0] == 0:
        raise ValueError("Train split is empty")

    train_id_list = sorted({str(bgg_ids[i]) for i in range(len(bgg_ids)) if train_mask[i]})

    X_emb_train = X_emb[train_mask] if match_emb else None

    bundle = fit_preprocess_bundle(
        X_tab_train=X_tab_train,
        scalar_columns=scalar_cols,
        scaler_kind=args.tabular_scaler,
        embedding_policy=args.embedding,
        embedding_dim=d,
        train_bgg_ids=train_id_list,
        tabular_clip=tabular_clip,
        tabular_clip_quantile=tabular_clip_quantile,
        tabular_clip_cap=tabular_clip_cap,
        X_emb_train=X_emb_train,
        balance_embedding_scale=match_emb,
        divide_columns_by_mean=divide_tuple if divide_tuple else None,
    )

    cat_vocab_t: tuple[str, ...] = ()
    mech_vocab_t: tuple[str, ...] = ()
    cat_ohe_cols: tuple[str, ...] = ()
    mech_ohe_cols: tuple[str, ...] = ()
    if not args.skip_one_hot_pipe_fields:
        cap_cat = None if args.one_hot_max_category_tokens <= 0 else args.one_hot_max_category_tokens
        cap_mech = None if args.one_hot_max_mechanism_tokens <= 0 else args.one_hot_max_mechanism_tokens
        want_cat = not args.no_one_hot_categories and "categories" in tbl.column_names
        want_mech = not args.no_one_hot_mechanisms and "mechanisms" in tbl.column_names
        if want_cat:
            cats_py = tbl.column("categories").to_pylist()
            cat_train = [cats_py[i] for i in range(tbl.num_rows) if train_mask[i]]
            cv = fit_ohe_vocab_from_strings(cat_train, max_tokens=cap_cat)
            cat_vocab_t = tuple(cv)
            cat_ohe_cols = parquet_safe_ohe_names("cat", cat_vocab_t)
        if want_mech:
            mech_py = tbl.column("mechanisms").to_pylist()
            mech_train = [mech_py[i] for i in range(tbl.num_rows) if train_mask[i]]
            mv = fit_ohe_vocab_from_strings(mech_train, max_tokens=cap_mech)
            mech_vocab_t = tuple(mv)
            mech_ohe_cols = parquet_safe_ohe_names("mech", mech_vocab_t)
        meta_ohe = dict(bundle.meta)
        meta_ohe.update(
            {
                "n_category_ohe_tokens": len(cat_vocab_t),
                "n_mechanism_ohe_tokens": len(mech_vocab_t),
                "pipe_field_one_hot": True,
            }
        )
        bundle = replace(
            bundle,
            category_vocab=cat_vocab_t,
            mechanism_vocab=mech_vocab_t,
            category_ohe_columns=cat_ohe_cols,
            mechanism_ohe_columns=mech_ohe_cols,
            meta=meta_ohe,
        )
    else:
        meta_ohe = dict(bundle.meta)
        meta_ohe["pipe_field_one_hot"] = False
        bundle = replace(bundle, meta=meta_ohe)

    out_tbl = standardized_table(
        tbl, bundle, scalar_cols=scalar_cols, embedding_col=args.embedding_col
    )

    outp = args.output.resolve()
    outp.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(out_tbl, outp)

    if args.pipeline_out:
        po = args.pipeline_out.resolve()
        write_bundle(po, bundle)

    extra = {
        "input": str(inp),
        "output": str(outp),
        "tabular_scaler": args.tabular_scaler,
        "embedding_policy": bundle.embedding_policy,
        "train_selection": (
            f"fraction={args.train_fraction},seed={args.seed}"
            if args.train_fraction is not None
            else f"file={args.train_bgg_ids!s}"
        ),
        "fit_bgq_review_games": args.fit_bgq_review_games,
        "reserve_bgq_for_test": args.reserve_bgq_for_test,
        "embedding_root_for_id_map": str(args.embedding_root.resolve()) if args.embedding_root else None,
        "n_rows_bgq_games_in_features": bgq_feature_row_count if bgq_set is not None else None,
        "n_rows_non_bgq_fit_pool": non_bgq_fit_pool_count if args.reserve_bgq_for_test else None,
        "n_rows": tbl.num_rows,
        "n_fit_rows": int(train_mask.sum()),
        "scalar_columns": scalar_cols,
        "pipeline_joblib": str(args.pipeline_out) if args.pipeline_out else None,
        "tabular_clip": bundle.tabular_clip,
        "tabular_clip_quantile": tabular_clip_quantile,
        "tabular_clip_cap": tabular_clip_cap,
        "nn_safe": bool(args.nn_safe),
        "match_embedding_tabular_scale": match_emb,
        "embedding_block_scale": bundle.embedding_block_scale,
        "divide_columns_by_mean": list(divide_tuple),
        "divide_price_columns_by_mean": bool(args.divide_price_columns_by_mean),
        "pipe_field_one_hot": not bool(args.skip_one_hot_pipe_fields),
        "one_hot_max_category_tokens": args.one_hot_max_category_tokens,
        "one_hot_max_mechanism_tokens": args.one_hot_max_mechanism_tokens,
        "n_category_ohe_columns": len(bundle.category_ohe_columns),
        "n_mechanism_ohe_columns": len(bundle.mechanism_ohe_columns),
    }
    write_preprocess_meta(outp.parent, bundle, extra)

    print(f"Wrote {outp}", flush=True)
    if args.pipeline_out:
        print(f"Wrote bundle {args.pipeline_out.resolve()}", flush=True)
    msg = f"Fit scaler on {int(train_mask.sum())}/{tbl.num_rows} rows"
    if args.fit_bgq_review_games:
        msg += f" (BGQ-only fit pool: {bgq_feature_row_count} rows)"
    if args.reserve_bgq_for_test:
        msg += (
            f" (non-BGQ fit pool: {non_bgq_fit_pool_count}; "
            f"{bgq_feature_row_count} BGQ rows held out for test)"
        )
    if match_emb:
        msg += f"; embedding_block_scale={bundle.embedding_block_scale:.6g}"
    print(msg, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
