"""CLI: score all embedded reviews and write review_sentiment.parquet."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from game_feature_export.review_sentiment.run_batch import run_review_sentiment


def _default_out_path(embedding_root: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return (
        Path(__file__).resolve().parent
        / "artifacts"
        / ts
        / "review_sentiment.parquet"
    )


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description="Score review rows (bgq_review, bgg_review) using shard text; writes Parquet for game_feature_export."
    )
    p.add_argument(
        "--embedding-root",
        type=Path,
        required=True,
        help="Embedding artifact dir; shards must include a text column (build embeddings with --store-text)",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output parquet path (default: game_feature_export/review_sentiment/artifacts/<UTC>/review_sentiment.parquet)",
    )
    p.add_argument(
        "--sentiment-model",
        type=str,
        default="cardiffnlp/twitter-roberta-base-sentiment-latest",
        help="HuggingFace model id for transformers sentiment-analysis pipeline",
    )
    p.add_argument("--batch-size", type=int, default=32)
    p.add_argument(
        "--device",
        type=str,
        default="mps",
        help="Transformers pipeline device (default: mps for Apple Silicon). Use -1 or cpu on machines without MPS.",
    )
    p.add_argument("--max-rows", type=int, default=None, help="Stop after this many scored review rows (debug)")
    p.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bar (and skip the doc_kind pre-scan)",
    )
    p.add_argument(
        "--resume",
        action="store_true",
        help="Continue after an interruption using checkpoint + part files next to --out",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Ignore existing output / checkpoint / parts and start fresh",
    )
    args = p.parse_args(argv)

    out = args.out or _default_out_path(args.embedding_root)
    try:
        dev: int | str = int(args.device)
    except ValueError:
        dev = args.device

    path = run_review_sentiment(
        embedding_root=args.embedding_root,
        out_parquet=out,
        sentiment_model=args.sentiment_model,
        batch_size=args.batch_size,
        device=dev,
        max_rows=args.max_rows,
        show_progress=not args.no_progress,
        resume=args.resume,
        overwrite=args.overwrite,
    )
    print(f"Wrote {path}", flush=True)


if __name__ == "__main__":
    main()
