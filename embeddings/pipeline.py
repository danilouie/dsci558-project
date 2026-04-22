from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from embeddings.checkpoint import (
    CHECKPOINT_VERSION,
    BuildCheckpoint,
    delete_checkpoint,
    fingerprint_documents,
    load_checkpoint,
    save_checkpoint,
    write_manifest,
)
from embeddings.documents import EmbeddingDocument, collect_all_documents, sort_documents
from embeddings.layout import ArtifactPaths, EmbeddingMeta
from numpy.lib.format import open_memmap


def _encoder_device(device: str | None) -> str:
    return device if device else "cpu"


def _write_shard_parquet(
    path: Path,
    rows: list[EmbeddingDocument],
    include_text: bool,
) -> None:
    doc_kind = [r.doc_kind for r in rows]
    review_id = [r.review_id for r in rows]
    bgg_review_id = [r.bgg_review_id for r in rows]
    bgg_id = [r.bgg_id for r in rows]
    text_sha256 = [r.text_sha256() for r in rows]
    cols: dict[str, list] = {
        "doc_kind": doc_kind,
        "review_id": review_id,
        "bgg_review_id": bgg_review_id,
        "bgg_id": bgg_id,
        "text_sha256": text_sha256,
    }
    if include_text:
        cols["text"] = [r.text for r in rows]
    table = pa.table(cols)
    pq.write_table(table, path)


def _write_all_shards(
    artifacts: ArtifactPaths,
    docs: list[EmbeddingDocument],
    shard_rows: int,
    store_text_in_shards: bool,
    *,
    show_progress: bool = False,
) -> None:
    artifacts.shards_dir.mkdir(parents=True, exist_ok=True)
    for p in artifacts.shards_dir.glob("shard_*.parquet"):
        p.unlink()
    n = len(docs)
    shard_idx = 0
    starts = range(0, n, shard_rows)
    num_shards = math.ceil(n / shard_rows) if shard_rows > 0 and n > 0 else 0
    if show_progress and num_shards > 0:
        try:
            from tqdm import tqdm  # noqa: PLC0415

            starts = tqdm(
                starts,
                total=num_shards,
                desc="Writing shards",
                unit="shard",
                smoothing=0.05,
            )
        except ImportError:  # pragma: no cover
            pass
    for start in starts:
        chunk = docs[start : start + shard_rows]
        out = artifacts.shards_dir / f"shard_{shard_idx:05d}.parquet"
        _write_shard_parquet(out, chunk, store_text_in_shards)
        shard_idx += 1


def _finalize_index(
    *,
    faiss_mod: object,
    artifacts: ArtifactPaths,
    docs: list[EmbeddingDocument],
    x: np.ndarray,
    dim: int,
    index_mode: str,
    hnsw_m: int,
    shard_rows: int,
    store_text_in_shards: bool,
    model_name: str,
    quiet: bool,
    show_progress: bool = True,
) -> EmbeddingMeta:
    n = len(docs)
    if not quiet:
        print(f"Building FAISS index ({index_mode})...", flush=True)

    if index_mode == "flat":
        idx = faiss_mod.IndexFlatIP(dim)
        idx.add(x)
        faiss_type = "IndexFlatIP"
        hnsw_meta = None
    elif index_mode == "hnsw":
        idx = faiss_mod.IndexHNSWFlat(dim, hnsw_m)
        idx.add(x)
        faiss_type = "IndexHNSWFlat"
        hnsw_meta = hnsw_m
    else:
        raise ValueError(f"Unknown index_mode {index_mode!r}, expected flat|hnsw")

    faiss_mod.write_index(idx, str(artifacts.index_faiss))

    if not quiet:
        print("Writing shards, id_map.parquet, meta.json...", flush=True)

    _write_all_shards(
        artifacts,
        docs,
        shard_rows,
        store_text_in_shards,
        show_progress=show_progress and not quiet,
    )

    faiss_ids = np.arange(n, dtype=np.int64)
    doc_kinds = [d.doc_kind for d in docs]
    rev_ids = [d.review_id for d in docs]
    br_ids = [d.bgg_review_id for d in docs]
    bgg_ids = [d.bgg_id for d in docs]
    hashes = [d.text_sha256() for d in docs]

    id_tbl = pa.table(
        {
            "faiss_id": faiss_ids,
            "doc_kind": doc_kinds,
            "review_id": rev_ids,
            "bgg_review_id": br_ids,
            "bgg_id": bgg_ids,
            "text_sha256": hashes,
        }
    )
    pq.write_table(id_tbl, artifacts.id_map_parquet)

    meta = EmbeddingMeta(
        model_name=model_name,
        embedding_dim=dim,
        normalize=True,
        faiss_index_type=faiss_type,
        metric="inner_product",
        num_vectors=n,
        hnsw_m=hnsw_meta,
    )
    artifacts.write_meta(meta)
    return meta


def _checkpoint_for_args(
    *,
    fingerprint: str,
    total_rows: int,
    completed_rows: int,
    embedding_dim: int,
    model_name: str,
    neo4j_import: str,
    limit_documents: int | None,
    include_games: bool,
    include_bgq: bool,
    include_bgg_reviews: bool,
    batch_size: int,
    index_mode: str,
    hnsw_m: int,
    shard_rows: int,
    store_text_in_shards: bool,
    encoder_device: str,
) -> BuildCheckpoint:
    return BuildCheckpoint(
        version=CHECKPOINT_VERSION,
        documents_fingerprint=fingerprint,
        total_rows=total_rows,
        completed_rows=completed_rows,
        embedding_dim=embedding_dim,
        model_name=model_name,
        neo4j_import=neo4j_import,
        limit_documents=limit_documents,
        include_games=include_games,
        include_bgq=include_bgq,
        include_bgg_reviews=include_bgg_reviews,
        batch_size=batch_size,
        index_mode=index_mode,
        hnsw_m=hnsw_m,
        shard_rows=shard_rows,
        store_text_in_shards=store_text_in_shards,
        encoder_device=encoder_device,
    )


def _validate_resume_matches(
    ckpt: BuildCheckpoint,
    *,
    fingerprint: str,
    n_docs: int,
    model_name: str,
    neo_resolved: str,
    limit_documents: int | None,
    include_games: bool,
    include_bgq: bool,
    include_bgg_reviews: bool,
    batch_size: int,
    index_mode: str,
    hnsw_m: int,
    shard_rows: int,
    store_text_in_shards: bool,
    embedding_dim: int,
    encoder_device: str,
) -> None:
    if ckpt.documents_fingerprint != fingerprint:
        raise ValueError(
            "Resume failed: document set fingerprint does not match (import data or "
            "flags changed). Use a fresh --output or rebuild without --resume."
        )
    if ckpt.total_rows != n_docs:
        raise ValueError(
            f"Resume failed: checkpoint total_rows={ckpt.total_rows} but collect "
            f"returned {n_docs} documents."
        )
    if ckpt.model_name != model_name:
        raise ValueError(
            f"Resume failed: model_name must match checkpoint ({ckpt.model_name!r})."
        )
    if ckpt.neo4j_import != neo_resolved:
        raise ValueError(
            "Resume failed: --neo4j-import path must match the checkpoint "
            f"({ckpt.neo4j_import})."
        )
    if ckpt.limit_documents != limit_documents:
        raise ValueError("Resume failed: --limit must match the original run.")
    if (
        ckpt.include_games != include_games
        or ckpt.include_bgq != include_bgq
        or ckpt.include_bgg_reviews != include_bgg_reviews
    ):
        raise ValueError("Resume failed: skip flags must match the original run.")
    if ckpt.batch_size != batch_size:
        raise ValueError("Resume failed: --batch-size must match the original run.")
    if ckpt.index_mode != index_mode or ckpt.hnsw_m != hnsw_m:
        raise ValueError("Resume failed: --index and --hnsw-m must match the original run.")
    if ckpt.shard_rows != shard_rows:
        raise ValueError("Resume failed: --shard-rows must match the original run.")
    if ckpt.store_text_in_shards != store_text_in_shards:
        raise ValueError("Resume failed: --store-text must match the original run.")
    if ckpt.embedding_dim != embedding_dim:
        raise ValueError(
            f"Resume failed: model embedding dim {embedding_dim} != checkpoint dim {ckpt.embedding_dim}."
        )
    if ckpt.encoder_device != encoder_device:
        raise ValueError(
            f"Resume failed: --device must match checkpoint ({ckpt.encoder_device!r}); "
            "mixed CPU/MPS batches can skew the index."
        )


def build_faiss_index(
    *,
    neo4j_import: Path,
    output_dir: Path,
    model_name: str,
    batch_size: int,
    index_mode: str,
    hnsw_m: int,
    shard_rows: int,
    store_text_in_shards: bool,
    skip_vectors_npy: bool,
    limit_documents: int | None = None,
    include_games: bool = True,
    include_bgq: bool = True,
    include_bgg_reviews: bool = True,
    only_bgg_ids: frozenset[str] | None = None,
    documents: list[EmbeddingDocument] | None = None,
    show_progress: bool = True,
    quiet: bool = False,
    resume: bool = False,
    encoder_device: str | None = None,
    encoder_model: object | None = None,
) -> EmbeddingMeta:
    enc_dev = _encoder_device(encoder_device)
    if documents is not None and resume:
        raise ValueError("explicit documents list cannot be combined with resume")
    if encoder_model is not None and resume:
        raise ValueError("encoder_model cannot be combined with resume")
    if resume and skip_vectors_npy:
        raise ValueError("--resume requires writing vectors.npy (do not pass --no-vectors-npy).")

    try:
        import faiss  # noqa: PLC0415
    except ImportError as e:
        raise RuntimeError(
            "faiss-cpu is required. Install with: pip install faiss-cpu"
        ) from e
    from sentence_transformers import SentenceTransformer  # noqa: PLC0415

    artifacts = ArtifactPaths(root=output_dir)
    artifacts.ensure_dirs()
    neo_resolved = str(neo4j_import.resolve())

    if documents is not None:
        docs = sort_documents(documents)
        if not docs:
            raise RuntimeError("explicit documents list is empty")
        if not quiet:
            print(f"Using {len(docs)} explicit documents (no CSV collect).", flush=True)
    else:
        if not quiet:
            print(
                "Collecting documents from neo4j/import CSVs (can take a while with BGG chunks)...",
                flush=True,
            )

        docs = collect_all_documents(
            neo4j_import,
            limit=limit_documents,
            include_games=include_games,
            include_bgq=include_bgq,
            include_bgg_reviews=include_bgg_reviews,
            only_bgg_ids=only_bgg_ids,
        )
        if not docs:
            raise RuntimeError(
                f"No embeddable documents found under {neo4j_import}. "
                "Expected games.csv, reviews.csv, and/or bgg_reviews.tsv."
            )

    fingerprint = fingerprint_documents(docs)
    n = len(docs)

    if not quiet:
        print(f"Collected {n} documents.", flush=True)

    if encoder_model is not None:
        model = encoder_model
        if not quiet:
            print(f"Using provided embedding model ({model_name!r}).", flush=True)
    else:
        if not quiet:
            print(f"Loading embedding model {model_name!r}...", flush=True)
        model = SentenceTransformer(model_name, device=enc_dev)
    dim = (
        model.get_embedding_dimension()
        if hasattr(model, "get_embedding_dimension")
        else model.get_sentence_embedding_dimension()
    )

    resume_start = 0
    if resume:
        if not artifacts.manifest_parquet.is_file():
            raise FileNotFoundError(
                f"Resume requires {artifacts.manifest_parquet} (missing). Cannot continue."
            )
        if not artifacts.checkpoint_json.is_file():
            raise FileNotFoundError(
                f"Resume requires {artifacts.checkpoint_json} (missing). Cannot continue."
            )
        if not artifacts.vectors_npy.is_file():
            raise FileNotFoundError(
                f"Resume requires {artifacts.vectors_npy} (missing). Cannot continue."
            )
        ck_resume = load_checkpoint(artifacts.checkpoint_json)
        _validate_resume_matches(
            ck_resume,
            fingerprint=fingerprint,
            n_docs=n,
            model_name=model_name,
            neo_resolved=neo_resolved,
            limit_documents=limit_documents,
            include_games=include_games,
            include_bgq=include_bgq,
            include_bgg_reviews=include_bgg_reviews,
            batch_size=batch_size,
            index_mode=index_mode,
            hnsw_m=hnsw_m,
            shard_rows=shard_rows,
            store_text_in_shards=store_text_in_shards,
            embedding_dim=dim,
            encoder_device=enc_dev,
        )
        resume_start = ck_resume.completed_rows
        if resume_start > n:
            raise ValueError(
                f"Checkpoint completed_rows={resume_start} exceeds document count {n}."
            )
        if not quiet:
            print(
                f"Resuming encoding from row {resume_start}/{n} (checkpoint.json).",
                flush=True,
            )
    else:
        stale = artifacts.checkpoint_json.is_file() or (
            artifacts.vectors_npy.is_file() and not skip_vectors_npy
        )
        if stale:
            raise RuntimeError(
                f"Output directory {output_dir} already has checkpoint.json and/or vectors.npy. "
                "Use --resume to continue, delete those files, or pick a different --output."
            )

    vectors_path = artifacts.vectors_npy
    if skip_vectors_npy:
        vectors_block: np.ndarray | np.memmap = np.empty((n, dim), dtype=np.float32)
    else:
        if resume:
            mm = open_memmap(str(vectors_path), mode="r+", dtype=np.float32)
            if mm.shape != (n, dim):
                raise ValueError(
                    f"vectors.npy shape {mm.shape} != expected ({n}, {dim}) for resume."
                )
            vectors_block = mm
        else:
            vectors_block = open_memmap(
                str(vectors_path), mode="w+", dtype=np.float32, shape=(n, dim)
            )

    if not resume and not skip_vectors_npy:
        write_manifest(artifacts, docs)
        save_checkpoint(
            artifacts.checkpoint_json,
            _checkpoint_for_args(
                fingerprint=fingerprint,
                total_rows=n,
                completed_rows=0,
                embedding_dim=dim,
                model_name=model_name,
                neo4j_import=neo_resolved,
                limit_documents=limit_documents,
                include_games=include_games,
                include_bgq=include_bgq,
                include_bgg_reviews=include_bgg_reviews,
                batch_size=batch_size,
                index_mode=index_mode,
                hnsw_m=hnsw_m,
                shard_rows=shard_rows,
                store_text_in_shards=store_text_in_shards,
                encoder_device=enc_dev,
            ),
        )

    texts = [d.text for d in docs]

    try:
        from tqdm import tqdm  # noqa: PLC0415
    except ImportError:  # pragma: no cover
        tqdm = None  # type: ignore[assignment,misc]

    use_bar = show_progress and not quiet and tqdm is not None
    if use_bar:
        print("Encoding batches (documents)...", flush=True)

    row_off = resume_start

    pbar = (
        tqdm(
            total=n,
            initial=row_off,
            desc="Encoding",
            unit="doc",
            smoothing=0.05,
            disable=False,
        )
        if use_bar
        else None
    )

    while row_off < n:
        batch_start = row_off
        end = min(row_off + batch_size, n)
        batch = texts[row_off:end]
        emb = model.encode(
            batch,
            batch_size=min(batch_size, len(batch)),
            normalize_embeddings=True,
            convert_to_numpy=True,
            show_progress_bar=False,
        )
        assert isinstance(emb, np.ndarray)
        emb = emb.astype(np.float32, copy=False)
        vectors_block[row_off:end] = emb

        if hasattr(vectors_block, "flush"):
            vectors_block.flush()

        row_off = end
        if not skip_vectors_npy:
            save_checkpoint(
                artifacts.checkpoint_json,
                _checkpoint_for_args(
                    fingerprint=fingerprint,
                    total_rows=n,
                    completed_rows=row_off,
                    embedding_dim=dim,
                    model_name=model_name,
                    neo4j_import=neo_resolved,
                    limit_documents=limit_documents,
                    include_games=include_games,
                    include_bgq=include_bgq,
                    include_bgg_reviews=include_bgg_reviews,
                    batch_size=batch_size,
                    index_mode=index_mode,
                    hnsw_m=hnsw_m,
                    shard_rows=shard_rows,
                    store_text_in_shards=store_text_in_shards,
                    encoder_device=enc_dev,
                ),
            )

        if pbar is not None:
            pbar.update(end - batch_start)
    if pbar is not None:
        pbar.close()

    if hasattr(vectors_block, "flush"):
        vectors_block.flush()

    x = np.ascontiguousarray(np.asarray(vectors_block), dtype=np.float32)

    meta = _finalize_index(
        faiss_mod=faiss,
        artifacts=artifacts,
        docs=docs,
        x=x,
        dim=dim,
        index_mode=index_mode,
        hnsw_m=hnsw_m,
        shard_rows=shard_rows,
        store_text_in_shards=store_text_in_shards,
        model_name=model_name,
        quiet=quiet,
        show_progress=show_progress,
    )
    if not skip_vectors_npy:
        delete_checkpoint(artifacts.checkpoint_json)
    return meta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build FAISS index + id_map.parquet from neo4j/import CSVs "
        "(keys aligned with neo4j/SCHEMA.md)."
    )
    parser.add_argument(
        "--neo4j-import",
        type=Path,
        default=None,
        help="Directory with games.csv, reviews.csv, bgg_reviews.tsv (default: neo4j/import)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output directory e.g. embeddings/bge-small-en-v1.5",
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
        help="flat=IndexFlatIP (exact cosine via normalized IP); hnsw=IndexHNSWFlat",
    )
    parser.add_argument("--hnsw-m", type=int, default=32, help="HNSW M parameter")
    parser.add_argument(
        "--shard-rows",
        type=int,
        default=50_000,
        help="Max rows per shards/shard_XXXXX.parquet",
    )
    parser.add_argument(
        "--store-text",
        action="store_true",
        help="Include full text column in shard parquets (larger files)",
    )
    parser.add_argument(
        "--only-bgg-ids",
        type=str,
        default=None,
        metavar="IDS",
        help="Comma-separated bgg_id values; embed only game rows and reviews for those games",
    )
    parser.add_argument(
        "--no-vectors-npy",
        action="store_true",
        help="Do not write vectors.npy (only index.faiss); incompatible with --resume",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Embed only the first N documents after global sort (use with --skip-bgg-reviews for fast smoke tests)",
    )
    parser.add_argument(
        "--skip-games",
        action="store_true",
        help="Omit game descriptions (Game.bgg_id + description)",
    )
    parser.add_argument(
        "--skip-bgq",
        action="store_true",
        help="Omit Board Game Quest reviews (Review.review_id)",
    )
    parser.add_argument(
        "--skip-bgg-reviews",
        action="store_true",
        help="Omit BGG user comments (BggReview.bgg_review_id); avoids scanning huge bgg_reviews chunk sets",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Continue from checkpoint.json + vectors.npy (same CLI flags as original run)",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable tqdm progress bar during encoding",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Minimal output (implies --no-progress)",
    )
    parser.add_argument(
        "--device",
        type=str,
        default=None,
        help="PyTorch device for the sentence-transformers model: cpu, mps (Apple Silicon), cuda, … (default: cpu)",
    )
    args = parser.parse_args(argv)
    if args.skip_games and args.skip_bgq and args.skip_bgg_reviews:
        parser.error("enable at least one of games / BGQ / BGG reviews (do not pass all three --skip flags)")

    repo_root = Path(__file__).resolve().parents[1]
    neo = args.neo4j_import or (repo_root / "neo4j" / "import")
    dev = _encoder_device(args.device)

    only_ids: frozenset[str] | None = None
    if args.only_bgg_ids and str(args.only_bgg_ids).strip():
        only_ids = frozenset(
            x.strip() for x in str(args.only_bgg_ids).split(",") if x.strip()
        )
        if not only_ids:
            only_ids = None

    meta = build_faiss_index(
        neo4j_import=neo,
        output_dir=args.output,
        model_name=args.model,
        batch_size=args.batch_size,
        index_mode=args.index,
        hnsw_m=args.hnsw_m,
        shard_rows=args.shard_rows,
        store_text_in_shards=args.store_text,
        skip_vectors_npy=args.no_vectors_npy,
        limit_documents=args.limit,
        include_games=not args.skip_games,
        include_bgq=not args.skip_bgq,
        include_bgg_reviews=not args.skip_bgg_reviews,
        only_bgg_ids=only_ids,
        show_progress=not args.no_progress and not args.quiet,
        quiet=args.quiet,
        resume=args.resume,
        encoder_device=dev,
    )
    print(f"Wrote {meta.num_vectors} vectors to {args.output}")
    print(f"Index: {meta.faiss_index_type} dim={meta.embedding_dim}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
