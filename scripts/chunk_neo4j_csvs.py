from __future__ import annotations

import argparse
import csv
from pathlib import Path


def split_csv(input_path: Path, output_dir: Path, prefix: str, rows_per_chunk: int) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    # Remove stale chunks for this prefix so reruns are deterministic.
    for old in output_dir.glob(f"{prefix}_*.csv"):
        old.unlink()
    chunks = 0

    with input_path.open("r", encoding="utf-8", newline="") as src:
        reader = csv.reader(src)
        header = next(reader, None)
        if not header:
            return 0

        writer = None
        chunk_file = None
        count_in_chunk = 0

        for row_idx, row in enumerate(reader, start=1):
            if writer is None or count_in_chunk >= rows_per_chunk:
                if chunk_file:
                    chunk_file.close()
                chunks += 1
                chunk_path = output_dir / f"{prefix}_{chunks:04d}.csv"
                chunk_file = chunk_path.open("w", encoding="utf-8", newline="")
                writer = csv.writer(chunk_file)
                writer.writerow(header)
                count_in_chunk = 0

            writer.writerow(row)
            count_in_chunk += 1

        if chunk_file:
            chunk_file.close()

    return chunks


def main() -> None:
    parser = argparse.ArgumentParser(description="Split large Neo4j import CSV files into chunks.")
    parser.add_argument("--import-dir", default="neo4j/import", help="Input import directory")
    parser.add_argument("--out-dir", default="neo4j/import/chunks", help="Output chunks directory")
    parser.add_argument("--rows", type=int, default=500_000, help="Rows per chunk")
    args = parser.parse_args()

    import_dir = Path(args.import_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    targets = [("price_points.csv", "price_points")]

    for filename, prefix in targets:
        src = import_dir / filename
        if not src.exists():
            print(f"skip {filename} (not found)")
            continue
        n = split_csv(src, out_dir, prefix, args.rows)
        print(f"{filename}: {n} chunks")


if __name__ == "__main__":
    main()

