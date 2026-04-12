#!/usr/bin/env python3
"""
Split a large CSV (or TSV) into many small files for Neo4j LOAD CSV.
Streaming line split: safe only when no data field contains embedded newlines (true for our edge CSVs).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("src", type=Path)
    parser.add_argument("out_dir", type=Path)
    parser.add_argument(
        "prefix",
        help="Output basename prefix, e.g. game_bgg_edges_ -> game_bgg_edges_000000.csv",
    )
    parser.add_argument("lines_per_chunk", type=int, nargs="?", default=10_000)
    parser.add_argument("--ext", default="csv", help="File extension without dot (default csv)")
    args = parser.parse_args()
    src: Path = args.src
    out_dir: Path = args.out_dir
    prefix: str = args.prefix
    n: int = args.lines_per_chunk
    ext: str = args.ext.lstrip(".")
    if n < 1:
        print("lines_per_chunk must be >= 1", file=sys.stderr)
        sys.exit(1)
    if not src.is_file() or src.stat().st_size == 0:
        print(f"Missing or empty source: {src}", file=sys.stderr)
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)
    for p in out_dir.glob(f"{prefix}*.{ext}"):
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
            name = out_dir / f"{prefix}{chunk_idx:06d}.{ext}"
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

    nfiles = len(list(out_dir.glob(f"{prefix}*.{ext}")))
    print(f"Wrote {nfiles} chunk(s) {prefix}*.{ext} under {out_dir}")


if __name__ == "__main__":
    main()
