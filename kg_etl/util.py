from __future__ import annotations

import csv
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Optional

from dateutil import parser as date_parser


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def parse_bool(v: Any) -> Optional[bool]:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False
    return None


def parse_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


def parse_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def parse_date_yyyy_mm_dd_from_iso(dt: str) -> Optional[date]:
    if not dt:
        return None
    try:
        # Handles "2020-09-20T00:00:00.000Z" etc.
        return date_parser.isoparse(dt).date()
    except Exception:
        return None


def parse_datetime_iso(dt: str) -> Optional[datetime]:
    if not dt:
        return None
    try:
        d = date_parser.isoparse(dt)
        # keep timezone if present; Neo4j datetime can parse offset
        return d
    except Exception:
        return None


def jsonl_iter(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def tsv_iter(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            yield {k: (v if v is not None else "") for k, v in row.items()}


def csv_iter(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield {k: (v if v is not None else "") for k, v in row.items()}


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> int:
    ensure_dir(path.parent)
    count = 0

    def clean(v: Any) -> Any:
        if isinstance(v, str):
            # Neo4j LOAD CSV is sensitive to malformed quoted fields.
            # Normalize control chars/newlines and strip embedded double quotes.
            s = v.replace("\x00", " ")
            s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
            s = s.replace('"', "'")
            return " ".join(s.split())
        return v

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: clean(row.get(k, "")) for k in fieldnames})
            count += 1
    return count

