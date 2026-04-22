"""CLI for per-game feature export."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from game_feature_export.run import build_per_game_features


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description="Build features_per_game.parquet from embeddings (+ optional sentiment + optional concepts)."
    )
    p.add_argument(
        "--embedding-root",
        type=Path,
        required=True,
        help="Directory containing meta.json, id_map.parquet, vectors.npy or index.faiss",
    )
    p.add_argument(
        "--neo4j-import",
        type=Path,
        default=Path("neo4j/import"),
        help="neo4j/import with reviews.csv and bgg_reviews*.tsv for reviewer ids",
    )
    p.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help="Repo root for default bgo_key_bgg_map.tsv and price_histories/ (default: current working directory)",
    )
    p.add_argument(
        "--bgo-map",
        type=Path,
        default=None,
        help="Override path to bgo_key_bgg_map.tsv",
    )
    p.add_argument(
        "--price-histories",
        type=Path,
        default=None,
        help="Directory of <BGO_KEY>.json price histories",
    )
    p.add_argument(
        "--price-as-of",
        type=str,
        default=None,
        metavar="ISO_DATETIME",
        help="UTC cutoff for price rows (ISO); default: use latest observation",
    )
    p.add_argument(
        "--price-features",
        choices=("extended", "core"),
        default="extended",
        help="extended=all Stage B + extra price scalars; core=Stage B only",
    )
    p.add_argument(
        "--split-seed",
        type=int,
        default=42,
        help="RNG seed for stage_a extra test holdout and stage_c splits",
    )
    p.add_argument(
        "--stage-a-extra-test-fraction",
        type=float,
        default=0.0,
        help="Fraction of non-BGQ games randomly assigned stage_a_split=test",
    )
    p.add_argument(
        "--splits-json",
        type=Path,
        default=None,
        help="Optional frozen splits.json (train_ids/val_ids/test_ids) for stage_c_split",
    )
    p.add_argument(
        "--no-write-splits-json",
        action="store_true",
        help="Do not write output_dir/splits.json when generating stage_c splits",
    )
    p.add_argument(
        "--skip-price-features",
        action="store_true",
        help="Omit price columns",
    )
    p.add_argument(
        "--skip-bgg-tabular",
        action="store_true",
        help="Omit games.csv/ranks.csv tabular joins (columns filled with NaN/empty)",
    )
    p.add_argument(
        "--skip-collection-features",
        action="store_true",
        help=(
            "Omit aggregation of user_game_owns/wants CSVs under --neo4j-import "
            "(coll_share_* set to NaN)"
        ),
    )
    p.add_argument(
        "--skip-description-embedding",
        action="store_true",
        help="Omit description_embedding (zeros + has_description_embedding=0)",
    )
    p.add_argument(
        "--skip-splits",
        action="store_true",
        help="Omit stage_a_split/stage_c_split columns",
    )
    p.add_argument(
        "--sentiment-parquet",
        type=Path,
        default=None,
        help="Parquet with faiss_id (int64) and sentiment_score (float); required with "
        "--include-sentiment-features",
    )
    p.add_argument(
        "--include-sentiment-features",
        action="store_true",
        help="Include sentiment aggregates (mean, std, fractions, …); requires --sentiment-parquet",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to write features_per_game.parquet and run_meta.json",
    )
    concept = p.add_argument_group(
        "value concepts (optional): encode two prompts with SentenceTransformer (same model family as FAISS by default)"
    )
    concept.add_argument(
        "--good-value-text",
        type=str,
        default=None,
        help="Prompt text for “good value”; encoded with --concept-encoder-model (default: meta.json model_name)",
    )
    concept.add_argument(
        "--bad-value-text",
        type=str,
        default=None,
        help="Prompt text for “bad value / overpriced”; must be set together with --good-value-text",
    )
    concept.add_argument(
        "--concept-encoder-model",
        type=str,
        default=None,
        help="SentenceTransformer model id (default: read from embedding meta.json so it matches FAISS)",
    )
    concept.add_argument(
        "--concept-encoder-device",
        type=str,
        default=None,
        help="Device for concept encoding (default: cpu), e.g. mps, cuda",
    )
    p.add_argument(
        "--extended",
        action="store_true",
        help="Add sentiment_p25, sentiment_p75, neutral_fraction, sentiment_iqr",
    )
    p.add_argument("--run-id", type=str, default=None, help="Optional run id for run_meta.json")
    args = p.parse_args(argv)

    if args.include_sentiment_features and args.sentiment_parquet is None:
        p.error("--sentiment-parquet is required when --include-sentiment-features is set")

    price_as_of = None
    if args.price_as_of:
        raw = args.price_as_of.strip().replace("Z", "+00:00")
        price_as_of = datetime.fromisoformat(raw)
        if price_as_of.tzinfo is None:
            from datetime import timezone as tz

            price_as_of = price_as_of.replace(tzinfo=tz.utc)

    repo_root = args.repo_root.resolve() if args.repo_root else Path.cwd().resolve()

    out = build_per_game_features(
        embedding_root=args.embedding_root,
        neo4j_import=args.neo4j_import,
        sentiment_parquet=args.sentiment_parquet,
        output_dir=args.output_dir,
        good_vec_path=None,
        bad_vec_path=None,
        good_value_text=args.good_value_text,
        bad_value_text=args.bad_value_text,
        concept_encoder_model=args.concept_encoder_model,
        concept_encoder_device=args.concept_encoder_device,
        extended=args.extended,
        run_id=args.run_id,
        include_sentiment_features=args.include_sentiment_features,
        repo_root=repo_root,
        bgo_map_path=args.bgo_map,
        price_histories_dir=args.price_histories,
        price_as_of=price_as_of,
        price_features_mode=args.price_features,
        stage_a_extra_test_fraction=args.stage_a_extra_test_fraction,
        split_seed=args.split_seed,
        splits_json_path=args.splits_json,
        write_splits_json=not args.no_write_splits_json,
        skip_price_features=args.skip_price_features,
        skip_bgg_tabular=args.skip_bgg_tabular,
        skip_collection_features=args.skip_collection_features,
        skip_description_embedding=args.skip_description_embedding,
        skip_splits=args.skip_splits,
    )
    print(f"Wrote {out}", flush=True)


if __name__ == "__main__":
    main()
