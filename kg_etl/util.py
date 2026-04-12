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


# Curly/smart quotes and similar must not remain as U+201C/U+201D — Neo4j LOAD CSV treats ASCII " as special.
_NEO4J_CSV_QUOTE_TRANSLATE = str.maketrans(
    {
        "\u201c": "'",  # "
        "\u201d": "'",  # "
        "\u201e": "'",  # „
        "\u201f": "'",  # ‟
        "\u00ab": "'",  # «
        "\u00bb": "'",  # »
        "\u2033": "'",  # ″
        "\u2036": "'",  # ‶
        "\uff02": "'",  # FULLWIDTH QUOTATION MARK
        "\u275d": "'",  # HEAVY DOUBLE COMMA QUOTATION MARK ORNAMENT
        "\u275e": "'",  # HEAVY DOUBLE TURNED COMMA QUOTATION MARK ORNAMENT
        "\u301d": "'",  # REVERSED DOUBLE PRIME QUOTATION MARK
        "\u301e": "'",  # DOUBLE PRIME QUOTATION MARK
        "\u301f": "'",  # LOW DOUBLE PRIME QUOTATION MARK
    }
)


def neo4j_csv_cell(v: Any) -> str:
    """
    Normalize a cell for Neo4j LOAD CSV: no raw newlines/tabs, no ASCII or curly double-quotes,
    no NUL, no zero-width / line-separator chars that confuse parsers. Output is always a string.
    """
    if v is None:
        return ""
    if not isinstance(v, str):
        s = str(v)
    else:
        s = v
    s = s.replace("\x00", " ").replace("\x0b", " ")
    s = s.translate(_NEO4J_CSV_QUOTE_TRANSLATE)
    s = s.replace('"', "'")
    # Strip BOM / ZWSP / ZWJ etc. and Unicode line/paragraph separators (often in scraped HTML).
    for ch in ("\ufeff", "\u200b", "\u200c", "\u200d", "\u2060"):
        s = s.replace(ch, "")
    s = s.replace("\u2028", " ").replace("\u2029", " ")
    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    return " ".join(s.split())


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> int:
    ensure_dir(path.parent)
    count = 0

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
            doublequote=True,
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({k: neo4j_csv_cell(row.get(k, "")) for k in fieldnames})
            count += 1
    return count


def write_tsv_neo4j(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> int:
    """
    Tab-separated export for Neo4j LOAD CSV with FIELDTERMINATOR '\\t'.
    Avoids comma/quote ambiguity in long free-text fields (e.g. BGG comments).
    """
    ensure_dir(path.parent)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
            delimiter="\t",
            quoting=csv.QUOTE_MINIMAL,
            doublequote=True,
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({k: neo4j_csv_cell(row.get(k, "")) for k in fieldnames})
            count += 1
    return count

