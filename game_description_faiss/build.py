from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from embeddings.documents import iter_games_descriptions, sort_documents
from embeddings.pipeline import _encoder_device, build_faiss_index
from sentence_transformers import SentenceTransformer

from game_description_faiss.categories import (
    build_documents_by_category,
    category_slug,
    stable_slug_suffix,
    uncategorized_registry_key,
)

ALL_GAMES_DIR = "all_games"


def build_registry_cli_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build FAISS indices from games.csv descriptions: one per BGG category, "
            f"one for games with no categories (cat/uncategorized/ by default), and one "
            f"over all games ({ALL_GAMES_DIR}/ by default). Artifacts match "
            "embeddings.search.FaissNeo4jIndex."
        )
    )
    parser.add_argument(
        "--neo4j-import",
        type=Path,
        default=None,
        help="Directory containing games.csv (default: repo/neo4j/import)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help=(
            "Artifact root directory (cat/<slug>/ per category, all_games/, registry.json)"
        ),
    )
    parser.add_argument(
        "--model",
        type=str,
        default="BAAI/bge-small-en-v1.5",
        help="sentence-transformers model id",
    )
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument(
        "--index",
        choices=("flat", "hnsw"),
        default="flat",
        help="flat=IndexFlatIP; hnsw=IndexHNSWFlat",
    )
    parser.add_argument("--hnsw-m", type=int, default=32)
    parser.add_argument("--shard-rows", type=int, default=50_000)
    parser.add_argument(
        "--store-text",
        action="store_true",
        help="Include full text in shards/shard_*.parquet",
    )
    parser.add_argument(
        "--no-vectors-npy",
        action="store_true",
        help="Do not persist vectors.npy after build",
    )
    parser.add_argument(
        "--min-games",
        type=int,
        default=1,
        metavar="N",
        help="Skip categories with fewer than N games",
    )
    parser.add_argument(
        "--skip-uncategorized",
        action="store_true",
        help="Do not put games with empty categories under cat/uncategorized/ (default: include them)",
    )
    parser.add_argument(
        "--uncategorized",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--skip-all-games-index",
        action="store_true",
        help=f"Do not write {ALL_GAMES_DIR}/ (single index over every game description)",
    )
    parser.add_argument(
        "--categories",
        type=str,
        default=None,
        metavar="LIST",
        help="Comma-separated category labels to build only (exact match)",
    )
    parser.add_argument(
        "--device",
        type=str,
        default=None,
        help="PyTorch device for encoding (cpu, cuda, mps, …)",
    )
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help=f"Remove each cat/<slug>/ and {ALL_GAMES_DIR}/ before rebuilding (needed to "
        "overwrite prior runs that left vectors.npy)",
    )

    args = parser.parse_args(argv)
    if args.uncategorized and not args.quiet:
        print(
            "Note: --uncategorized is obsolete (Uncategorized bucket is built by default). "
            "Use --skip-uncategorized to omit cat/uncategorized/.",
            flush=True,
        )

    repo_root = Path(__file__).resolve().parents[1]
    neo = args.neo4j_import or (repo_root / "neo4j" / "import")
    games_csv = neo / "games.csv"
    if not games_csv.is_file():
        raise SystemExit(f"Missing games.csv at {games_csv}")

    buckets = build_documents_by_category(
        games_csv, uncategorized=not args.skip_uncategorized
    )
    docs_all_games = sort_documents(list(iter_games_descriptions(games_csv)))
    if args.categories and str(args.categories).strip():
        allowed = {x.strip() for x in str(args.categories).split(",") if x.strip()}
        buckets = {k: v for k, v in buckets.items() if k in allowed}
        missing = allowed - frozenset(buckets.keys())
        if missing:
            raise SystemExit(f"No games found for categories: {sorted(missing)}")

    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=True)

    enc_dev = _encoder_device(args.device)
    embed_model = SentenceTransformer(args.model, device=enc_dev)

    unc_key = uncategorized_registry_key()
    used_slugs: set[str] = set()

    registry_categories: dict[str, dict[str, Any]] = {}

    ordered_labels = sorted(buckets.keys(), key=lambda s: (s != unc_key, s.lower()))
    for label in ordered_labels:
        docs = buckets[label]
        if len(docs) < args.min_games:
            continue

        plain_slug = category_slug(label)
        slug = plain_slug
        if slug in used_slugs:
            slug = category_slug(label, collision_suffix=stable_slug_suffix(label))
        idx = 0
        while slug in used_slugs:
            idx += 1
            slug = f"{plain_slug}_{idx}"
        used_slugs.add(slug)

        cat_dir = root / "cat" / slug
        if args.force and cat_dir.exists():
            shutil.rmtree(cat_dir)
        if not args.quiet:
            print(f"Building {slug!r} ({label}): {len(docs)} games → {cat_dir}", flush=True)

        meta = build_faiss_index(
            neo4j_import=neo,
            output_dir=cat_dir,
            model_name=args.model,
            batch_size=args.batch_size,
            index_mode=args.index,
            hnsw_m=args.hnsw_m,
            shard_rows=args.shard_rows,
            store_text_in_shards=args.store_text,
            skip_vectors_npy=args.no_vectors_npy,
            limit_documents=None,
            include_games=False,
            include_bgq=False,
            include_bgg_reviews=False,
            documents=docs,
            show_progress=not args.no_progress and not args.quiet,
            quiet=args.quiet,
            resume=False,
            encoder_device=args.device,
            encoder_model=embed_model,
        )

        reg_key = "Uncategorized" if label == unc_key else label
        registry_categories[reg_key] = {
            "slug": slug,
            "path": str(Path("cat") / slug),
            "canonical_label": label,
            "num_vectors": meta.num_vectors,
        }

    all_games_payload: dict[str, Any] | None = None
    if not args.skip_all_games_index:
        if not docs_all_games:
            if not args.quiet:
                print("Skipping all_games: no games with non-empty descriptions.", flush=True)
        else:
            ag_dir = root / ALL_GAMES_DIR
            if args.force and ag_dir.exists():
                shutil.rmtree(ag_dir)
            if not args.quiet:
                print(
                    f"Building {ALL_GAMES_DIR!r}: {len(docs_all_games)} games → {ag_dir}",
                    flush=True,
                )
            meta_all = build_faiss_index(
                neo4j_import=neo,
                output_dir=ag_dir,
                model_name=args.model,
                batch_size=args.batch_size,
                index_mode=args.index,
                hnsw_m=args.hnsw_m,
                shard_rows=args.shard_rows,
                store_text_in_shards=args.store_text,
                skip_vectors_npy=args.no_vectors_npy,
                limit_documents=None,
                include_games=False,
                include_bgq=False,
                include_bgg_reviews=False,
                documents=docs_all_games,
                show_progress=not args.no_progress and not args.quiet,
                quiet=args.quiet,
                resume=False,
                encoder_device=args.device,
                encoder_model=embed_model,
            )
            all_games_payload = {
                "path": ALL_GAMES_DIR,
                "num_vectors": meta_all.num_vectors,
            }

    payload = {
        "schema_ref": "neo4j/SCHEMA.md",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "neo4j_import": str(neo),
        "games_csv": str(games_csv),
        "model_name": args.model,
        "min_games": args.min_games,
        "include_uncategorized": not args.skip_uncategorized,
        "force_rebuild_used": args.force,
        "categories": registry_categories,
        "all_games_index": all_games_payload,
    }
    reg_path = root / "registry.json"
    reg_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    if not args.quiet:
        extra = f", all_games={all_games_payload['num_vectors']}" if all_games_payload else ""
        print(
            f"Wrote {reg_path} ({len(registry_categories)} category indices{extra})",
            flush=True,
        )
    return 0


def main() -> None:
    raise SystemExit(build_registry_cli_main())


if __name__ == "__main__":
    main()
