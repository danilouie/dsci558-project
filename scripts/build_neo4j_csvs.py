from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from kg_etl.export_csvs import ExportConfig, export_all
from kg_etl.paths import default_paths


def main() -> None:
    paths = default_paths()

    parser = argparse.ArgumentParser(description="Export Neo4j-ready CSVs for board game KG.")
    parser.add_argument(
        "--out",
        default=str(paths.neo4j_import_dir),
        help="Output directory (Neo4j import dir). Defaults to neo4j/import/",
    )
    parser.add_argument("--limit-games", type=int, default=None)
    parser.add_argument("--limit-price-files", type=int, default=None)
    parser.add_argument("--limit-reviews", type=int, default=None)
    parser.add_argument("--limit-ranks", type=int, default=None)
    parser.add_argument(
        "--limit-bgg-batch-files",
        type=int,
        default=None,
        help="Cap JSONL files scanned under game_review_batches/ (dev/smoke).",
    )
    parser.add_argument(
        "--only-collection-user",
        default=None,
        help="If set, only load user/{username}_collection.jsonl. Default: all user/*_collection.jsonl files.",
    )
    parser.add_argument(
        "--limit-user-collection-files",
        type=int,
        default=None,
        help="Cap how many user/*_collection.jsonl files are scanned (dev/smoke).",
    )
    parser.add_argument(
        "--overlap-only",
        action="store_true",
        default=False,
        help="Only export games whose bgg_id appears in bgo_key_bgg_map.tsv (BGO↔BGG overlap).",
    )
    args = parser.parse_args()

    cfg = ExportConfig(
        out_dir=Path(args.out),
        limit_games=args.limit_games,
        limit_price_files=args.limit_price_files,
        limit_reviews=args.limit_reviews,
        limit_ranks=args.limit_ranks,
        limit_bgg_batch_files=args.limit_bgg_batch_files,
        limit_user_collection_files=args.limit_user_collection_files,
        only_collection_username=args.only_collection_user,
        overlap_only=args.overlap_only,
    )

    counts = export_all(paths, cfg)
    print("Export complete:")
    for k, v in counts.items():
        print(f"- {k}: {v}")


if __name__ == "__main__":
    main()

