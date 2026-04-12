#!/usr/bin/env python3
"""
Split neo4j/import/bgg_reviews.tsv into many small TSVs for Neo4j LOAD CSV (low transaction memory).
Streaming: does not load the full source file into RAM.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("src", type=Path, help="Path to bgg_reviews.tsv")
    parser.add_argument("out_dir", type=Path, help="Output directory for bgg_reviews_000000.tsv")
    parser.add_argument(
        "lines_per_chunk",
        type=int,
        nargs="?",
        default=500,
        help="Data rows per chunk file (default 500).",
    )
    args = parser.parse_args()
    src: Path = args.src
    out_dir: Path = args.out_dir
    n: int = args.lines_per_chunk
    if n < 1:
        print("lines_per_chunk must be >= 1", file=sys.stderr)
        sys.exit(1)
    if not src.is_file():
        print(f"Missing source: {src}", file=sys.stderr)
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)
    for p in out_dir.glob("bgg_reviews_*.tsv"):
        p.unlink()

    with src.open("r", encoding="utf-8", newline="") as inf:
        header = inf.readline()
        if not header:
            print("Empty source", file=sys.stderr)
            sys.exit(1)
        if not header.endswith("\n"):
            header += "\n"

        chunk_idx = 0
        lines_in_chunk = 0
        outf = None

        def open_next() -> None:
            nonlocal outf, chunk_idx, lines_in_chunk
            if outf is not None:
                outf.close()
            name = out_dir / f"bgg_reviews_{chunk_idx:06d}.tsv"
            outf = name.open("w", encoding="utf-8", newline="")
            outf.write(header)
            chunk_idx += 1
            lines_in_chunk = 0

        open_next()
        for line in inf:
            if lines_in_chunk >= n:
                open_next()
            assert outf is not None
            outf.write(line)
            lines_in_chunk += 1
        if outf is not None:
            outf.close()

    count = len(list(out_dir.glob("bgg_reviews_*.tsv")))
    print(f"Wrote {count} chunk file(s) under {out_dir}")


if __name__ == "__main__":
    main()
