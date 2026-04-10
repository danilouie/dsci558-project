"""
BoardGameGeek Review Scraper

Per game: all pages go under output_dir/<bgg_id>/page_0001.jsonl, ...
Downloaded page numbers come from listing page_*.jsonl (a set in memory); manifest.json
stores metadata like expected_total only (no separate registry file).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
from dataclasses import dataclass
import hashlib
import json
import math
import os
import random
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import requests
try:
    import httpx
except ImportError:  # optional dependency, required only for async engine
    httpx = None  # type: ignore[assignment]

API_TOKEN = os.environ.get("BGG_API_TOKEN", "85cbfc9b-e2f9-4b82-b76c-1eaabb547586")
CSV_FILE = Path(__file__).parent / "boardgames_ranks.csv"
OUTPUT_DIR = Path(__file__).parent / "game_review_batches"
API_THING = "https://boardgamegeek.com/xmlapi2/thing"

DEFAULT_DELAY_SECONDS = 1.0
RETRY_DELAY_SECONDS = 1.0
MAX_RETRIES = 8
BACKOFF_CAP_SECONDS = 2.0
MAX_RANDOM_PAGE_RESAMPLES = 5
DEFAULT_CONCURRENCY = 4
DEFAULT_MAX_RPS = 1.0
DEFAULT_BURST = 2
DEFAULT_MANIFEST_EVERY_PAGES = 10


@dataclass
class ScrapeStats:
    fetch_calls: int = 0
    fetch_retries: int = 0
    fetch_failures: int = 0
    fetch_latency_s: float = 0.0
    parse_latency_s: float = 0.0
    write_latency_s: float = 0.0
    bytes_downloaded: int = 0
    pages_written: int = 0
    rows_written: int = 0
    start_ts: float = 0.0

    def start(self) -> None:
        self.start_ts = time.perf_counter()

    def wall_s(self) -> float:
        if self.start_ts <= 0:
            return 0.0
        return max(0.0, time.perf_counter() - self.start_ts)

    def as_dict(self) -> dict[str, Any]:
        wall = self.wall_s()
        return {
            "fetch_calls": self.fetch_calls,
            "fetch_retries": self.fetch_retries,
            "fetch_failures": self.fetch_failures,
            "fetch_latency_s": round(self.fetch_latency_s, 3),
            "parse_latency_s": round(self.parse_latency_s, 3),
            "write_latency_s": round(self.write_latency_s, 3),
            "bytes_downloaded": self.bytes_downloaded,
            "pages_written": self.pages_written,
            "rows_written": self.rows_written,
            "wall_s": round(wall, 3),
            "pages_per_s": round(self.pages_written / wall, 3) if wall > 0 else 0.0,
            "rows_per_s": round(self.rows_written / wall, 3) if wall > 0 else 0.0,
        }


class TokenBucket:
    def __init__(self, rate: float, burst: int) -> None:
        self.rate = max(0.01, rate)
        self.capacity = max(1.0, float(burst))
        self.tokens = self.capacity
        self.updated_at = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self.lock:
                now = time.monotonic()
                elapsed = now - self.updated_at
                self.updated_at = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                need = (1.0 - self.tokens) / self.rate
            await asyncio.sleep(max(0.01, need))


def game_dir_path(output_root: Path, bgg_id: str) -> Path:
    return output_root / bgg_id


def page_file_path(game_dir: Path, page: int) -> Path:
    return game_dir / f"page_{page:04d}.jsonl"


def manifest_path(game_dir: Path) -> Path:
    return game_dir / "manifest.json"


def get_session() -> requests.Session:
    session = requests.Session()
    token = (API_TOKEN or "").strip()
    headers = {"User-Agent": "bgg_reviews_scraper (Vishal Sankarram)"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    session.headers.update(headers)
    return session


def _retry_delay_seconds(
    status_code: int | None, attempt: int, response_headers: dict[str, str] | None
) -> float:
    base = float(RETRY_DELAY_SECONDS) * (1.5 ** max(0, attempt - 1))
    if status_code in {429, 202}:
        base *= 2.0
    retry_after = None
    if response_headers:
        ra = response_headers.get("Retry-After")
        if ra and ra.isdigit():
            retry_after = float(ra)
    delay = max(base, retry_after or 0.0)
    jitter = random.uniform(0.0, 0.35 * max(1.0, delay))
    return min(delay + jitter, BACKOFF_CAP_SECONDS)


def load_games(ranked_only: bool = False, no_expansions: bool = False) -> list[dict[str, Any]]:
    games: list[dict[str, Any]] = []
    with CSV_FILE.open(encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            if no_expansions and row.get("is_expansion", "0") == "1":
                continue
            rank_raw = (row.get("rank") or "").strip()
            rank = int(rank_raw) if rank_raw.isdigit() else None
            if ranked_only and rank is None:
                continue
            games.append(
                {
                    "bgg_id": (row.get("id") or "").strip(),
                    "name": (row.get("name") or "").strip(),
                    "rank": rank,
                }
            )
    return games


def fetch_comments_page(
    session: requests.Session,
    bgg_id: str,
    mode: str,
    page: int,
    pagesize: int,
) -> tuple[str | None, int, int]:
    params = {"id": bgg_id, "type": "boardgame", mode: 1, "page": page, "pagesize": pagesize}
    for attempt in range(1, MAX_RETRIES + 1):
        status_code: int | None = None
        if attempt > 3:
            print("  [!] Server slow — backing off...")
            time.sleep(10)
        try:
            response = session.get(API_THING, params=params, timeout=60)
        except requests.RequestException as exc:
            print(f"[api] request error {bgg_id} p{page}: {exc} ({attempt}/{MAX_RETRIES})")
            time.sleep(_retry_delay_seconds(None, attempt, None))
            continue
        status_code = response.status_code
        if response.status_code == 200:
            return response.text, len(response.content or b""), max(0, attempt - 1)
        if response.status_code in {202, 429, 500, 502, 503, 504}:
            print(f"[api] HTTP {response.status_code} for {bgg_id} p{page}; backing off...")
            time.sleep(_retry_delay_seconds(response.status_code, attempt, dict(response.headers)))
            continue
        print(f"[api] HTTP {response.status_code} for {bgg_id} p{page} (not retryable)")
        return None, 0, max(0, attempt - 1)
    print(f"[api] gave up on game {bgg_id} page {page}")
    return None, 0, MAX_RETRIES - 1


async def fetch_comments_page_async(
    client: httpx.AsyncClient,
    limiter: TokenBucket,
    bgg_id: str,
    mode: str,
    page: int,
    pagesize: int,
) -> tuple[str | None, int, int]:
    params = {"id": bgg_id, "type": "boardgame", mode: 1, "page": page, "pagesize": pagesize}
    for attempt in range(1, MAX_RETRIES + 1):
        await limiter.acquire()
        try:
            response = await client.get(API_THING, params=params, timeout=60.0)
        except httpx.HTTPError as exc:
            print(f"[api] async request error {bgg_id} p{page}: {exc} ({attempt}/{MAX_RETRIES})")
            await asyncio.sleep(_retry_delay_seconds(None, attempt, None))
            continue
        if response.status_code == 200:
            return response.text, len(response.content or b""), max(0, attempt - 1)
        if response.status_code in {202, 429, 500, 502, 503, 504}:
            await asyncio.sleep(
                _retry_delay_seconds(response.status_code, attempt, dict(response.headers))
            )
            continue
        print(f"[api] HTTP {response.status_code} for {bgg_id} p{page} (not retryable)")
        return None, 0, max(0, attempt - 1)
    print(f"[api] async gave up on game {bgg_id} page {page}")
    return None, 0, MAX_RETRIES - 1


def parse_comments_xml(
    xml_text: str,
    bgg_id: str,
    game_name: str,
    page: int,
) -> tuple[int, list[dict[str, Any]]]:
    root = ET.fromstring(xml_text)
    item = root.find("item")
    if item is None:
        return 0, []
    comments_node = item.find("comments")
    if comments_node is None:
        return 0, []
    totalitems_raw = comments_node.get("totalitems", "0")
    totalitems = int(totalitems_raw) if totalitems_raw.isdigit() else 0

    rows: list[dict[str, Any]] = []
    for comment in comments_node.findall("comment"):
        username = (comment.get("username") or "").strip()
        rating_raw = (comment.get("rating") or "").strip()
        text = (comment.get("value") or "").strip()
        rating: float | None = None
        if rating_raw and rating_raw != "N/A":
            try:
                rating = float(rating_raw)
            except ValueError:
                rating = None
        review_key = hashlib.sha256(f"{bgg_id}|{username}|{text}".encode("utf-8")).hexdigest()
        rows.append(
            {
                "review_key": review_key,
                "bgg_id": bgg_id,
                "game_name": game_name,
                "username": username or None,
                "rating": rating,
                "comment_text": text or None,
                "page": page,
            }
        )
    return totalitems, rows


def count_jsonl_lines(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open(encoding="utf-8") as f:
        for _ in f:
            count += 1
    return count


def write_jsonl_atomic(path: Path, records: list[dict[str, Any]], fsync_enabled: bool = True) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        if fsync_enabled:
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
    tmp.replace(path)


def write_json_atomic(path: Path, data: dict[str, Any], fsync_enabled: bool = True) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))
        if fsync_enabled:
            f.flush()
            try:
                os.fsync(f.fileno())
            except OSError:
                pass
    tmp.replace(path)


def read_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def total_rows_in_dir(game_dir: Path) -> int:
    total = 0
    for p in game_dir.glob("page_*.jsonl"):
        total += count_jsonl_lines(p)
    return total


def load_saved_page_numbers(game_dir: Path) -> set[int]:
    """Page indices that already have a page_####.jsonl file on disk."""
    pages: set[int] = set()
    for p in game_dir.glob("page_*.jsonl"):
        stem = p.stem
        try:
            pages.add(int(stem.split("_")[1]))
        except (IndexError, ValueError):
            continue
    return pages


def load_existing_progress(game_dir: Path) -> tuple[set[int], int, dict[int, int]]:
    pages: set[int] = set()
    rows_per_page: dict[int, int] = {}
    total_rows = 0
    for p in game_dir.glob("page_*.jsonl"):
        stem = p.stem
        try:
            page_num = int(stem.split("_")[1])
        except (IndexError, ValueError):
            continue
        rows = count_jsonl_lines(p)
        pages.add(page_num)
        rows_per_page[page_num] = rows
        total_rows += rows
    return pages, total_rows, rows_per_page


def expected_total_from_manifest(game_dir: Path) -> int | None:
    m = read_manifest(manifest_path(game_dir))
    et = m.get("expected_total")
    return int(et) if isinstance(et, int) else None


def manifest_compatible_for_resume(manifest: dict[str, Any], args: argparse.Namespace) -> bool:
    """True if saved manifest matches CLI knobs (so a completed crawl can be skipped safely)."""
    if manifest.get("mode") != args.mode:
        return False
    if manifest.get("pagesize") != args.pagesize:
        return False
    if manifest.get("page_order") != args.page_order:
        return False
    stored = manifest.get("max_comments_per_game")
    if stored is None:
        if args.max_comments_per_game != 0:
            return False
    elif not isinstance(stored, int) or stored != args.max_comments_per_game:
        return False
    return True


def should_skip_completed_game(manifest: dict[str, Any], args: argparse.Namespace) -> bool:
    if not manifest.get("crawl_complete"):
        return False
    return manifest_compatible_for_resume(manifest, args)


def pick_random_unclaimed_page(pool: list[int], game_dir: Path) -> int | None:
    """Random page from pool; if file already exists, drop it and redraw (up to MAX_RANDOM_PAGE_RESAMPLES)."""
    for _ in range(MAX_RANDOM_PAGE_RESAMPLES):
        if not pool:
            return None
        page = random.choice(pool)
        if page_file_path(game_dir, page).exists():
            pool.remove(page)
            continue
        return page
    return None


def bootstrap_page_one(
    session: requests.Session,
    game_dir: Path,
    bgg_id: str,
    game_name: str,
    mode: str,
    pagesize: int,
    max_comments: int,
    saved_pages: set[int],
    expected_total_hint: int | None = None,
    stats: ScrapeStats | None = None,
    fsync_mode: str = "manifest",
) -> tuple[int | None, bool]:
    """
    Learn expected_total (hint, manifest, or API page 1) and write page 1 if missing.
    Returns (expected_total, fetch_failed).
    """
    expected_total = (
        expected_total_hint if isinstance(expected_total_hint, int) else expected_total_from_manifest(game_dir)
    )

    probe_reviews: list[dict[str, Any]] | None = None
    if expected_total is None:
        t0 = time.perf_counter()
        xml_text, downloaded_bytes, retries = fetch_comments_page(session, bgg_id, mode, 1, pagesize)
        if stats:
            stats.fetch_calls += 1
            stats.fetch_retries += retries
            stats.bytes_downloaded += downloaded_bytes
            stats.fetch_latency_s += time.perf_counter() - t0
        if not xml_text:
            if stats:
                stats.fetch_failures += 1
            return None, True
        try:
            p0 = time.perf_counter()
            totalitems, reviews = parse_comments_xml(xml_text, bgg_id, game_name, 1)
            if stats:
                stats.parse_latency_s += time.perf_counter() - p0
        except ET.ParseError as exc:
            print(f"  XML parse error on page 1: {exc}")
            return None, True
        expected_total = totalitems
        print(f"  expected total comments: {expected_total}")
        m = read_manifest(manifest_path(game_dir))
        m["expected_total"] = expected_total
        write_json_atomic(manifest_path(game_dir), m, fsync_enabled=(fsync_mode != "never"))
        probe_reviews = reviews

    assert expected_total is not None

    eff, warn = compute_effective_target(max_comments, expected_total)
    if warn:
        print(f"  {warn}")
    take_target = eff if eff is not None else expected_total
    assert take_target is not None

    if 1 in saved_pages:
        print("  page 1: already on disk")
        return expected_total, False

    if probe_reviews is not None:
        take = min(len(probe_reviews), take_target)
        w0 = time.perf_counter()
        write_jsonl_atomic(
            page_file_path(game_dir, 1), probe_reviews[:take], fsync_enabled=(fsync_mode == "always")
        )
        if stats:
            stats.write_latency_s += time.perf_counter() - w0
            stats.pages_written += 1
            stats.rows_written += take
        saved_pages.add(1)
        print(f"  page 1: downloaded ({take} rows)")
        return expected_total, False

    t0 = time.perf_counter()
    xml_text, downloaded_bytes, retries = fetch_comments_page(session, bgg_id, mode, 1, pagesize)
    if stats:
        stats.fetch_calls += 1
        stats.fetch_retries += retries
        stats.bytes_downloaded += downloaded_bytes
        stats.fetch_latency_s += time.perf_counter() - t0
    if not xml_text:
        if stats:
            stats.fetch_failures += 1
        return None, True
    try:
        p0 = time.perf_counter()
        _, reviews = parse_comments_xml(xml_text, bgg_id, game_name, 1)
        if stats:
            stats.parse_latency_s += time.perf_counter() - p0
    except ET.ParseError as exc:
        print(f"  XML parse error on page 1: {exc}")
        return None, True
    take = max(0, min(len(reviews), take_target - total_rows_in_dir(game_dir)))
    if take > 0:
        w0 = time.perf_counter()
        write_jsonl_atomic(
            page_file_path(game_dir, 1), reviews[:take], fsync_enabled=(fsync_mode == "always")
        )
        if stats:
            stats.write_latency_s += time.perf_counter() - w0
            stats.pages_written += 1
            stats.rows_written += take
        saved_pages.add(1)
        print(f"  page 1: downloaded ({take} rows)")
    return expected_total, False


def compute_effective_target(
    requested: int, expected_total: int | None
) -> tuple[int | None, str | None]:
    if expected_total is None:
        return (requested if requested > 0 else None), None
    if requested <= 0:
        return expected_total, None
    if requested > expected_total:
        return expected_total, (
            f"failsafe: requested {requested} reviews but BGG totalitems is {expected_total}; "
            f"capping to {expected_total}"
        )
    return requested, None


def crawl_game_sequential(
    session: requests.Session,
    game_dir: Path,
    bgg_id: str,
    game_name: str,
    mode: str,
    pagesize: int,
    delay: float,
    max_pages: int,
    max_comments: int,
    expected_total_hint: int | None = None,
    stats: ScrapeStats | None = None,
    fsync_mode: str = "manifest",
) -> tuple[int | None, int, bool, bool, dict[str, Any]]:
    saved_pages, fetched_so_far, _rows_per_page = load_existing_progress(game_dir)
    expected_total: int | None = expected_total_hint
    page = 1
    crawl_complete = False
    fetch_failed = False
    target_rows: int | None = max_comments if max_comments > 0 else None
    if target_rows is not None and expected_total is not None:
        target_rows, w0 = compute_effective_target(max_comments, expected_total)
        if w0:
            print(f"  {w0}")

    while True:
        if max_pages > 0 and page > max_pages:
            print("  reached --max-pages safety cap")
            break
        if target_rows is not None and fetched_so_far >= target_rows:
            print(f"  reached --max-comments-per-game cap ({target_rows})")
            crawl_complete = True
            break

        while page in saved_pages:
            print(f"  page {page}: skip (file already on disk)")
            page += 1
            if page > 500_000:
                print("  abort: too many pages (unexpected)")
                fetch_failed = True
                break
        if fetch_failed:
            break

        pfile = page_file_path(game_dir, page)
        t0 = time.perf_counter()
        xml_text, downloaded_bytes, retries = fetch_comments_page(session, bgg_id, mode, page, pagesize)
        if stats:
            stats.fetch_calls += 1
            stats.fetch_retries += retries
            stats.bytes_downloaded += downloaded_bytes
            stats.fetch_latency_s += time.perf_counter() - t0
        if not xml_text:
            if stats:
                stats.fetch_failures += 1
            fetch_failed = True
            break

        try:
            p0 = time.perf_counter()
            totalitems, reviews = parse_comments_xml(xml_text, bgg_id, game_name, page)
            if stats:
                stats.parse_latency_s += time.perf_counter() - p0
        except ET.ParseError as exc:
            print(f"  XML parse error on page {page}: {exc}")
            fetch_failed = True
            break

        if expected_total is None:
            expected_total = totalitems
            print(f"  expected total comments: {expected_total}")
            m = read_manifest(manifest_path(game_dir))
            m["expected_total"] = expected_total
            write_json_atomic(manifest_path(game_dir), m, fsync_enabled=(fsync_mode != "never"))
            if max_comments > 0:
                target_rows, w = compute_effective_target(max_comments, expected_total)
                if w:
                    print(f"  {w}")

        if target_rows is not None:
            remaining_cap = target_rows - fetched_so_far
            if remaining_cap <= 0:
                crawl_complete = True
                break
            if len(reviews) > remaining_cap:
                reviews = reviews[:remaining_cap]

        w0 = time.perf_counter()
        write_jsonl_atomic(pfile, reviews, fsync_enabled=(fsync_mode == "always"))
        if stats:
            stats.write_latency_s += time.perf_counter() - w0
            stats.pages_written += 1
            stats.rows_written += len(reviews)
        saved_pages.add(page)

        batch_count = len(reviews)
        fetched_so_far += batch_count
        print(f"  page {page}: downloaded ({batch_count} rows)")

        if target_rows is not None and fetched_so_far >= target_rows:
            crawl_complete = True
            break
        if expected_total is not None and fetched_so_far >= expected_total:
            crawl_complete = True
            break
        if batch_count < pagesize:
            crawl_complete = True
            break

        page += 1
        if stats.fetch_calls!=0:
            time.sleep(delay)

    eff_display = target_rows if target_rows is not None else expected_total
    stats: dict[str, Any] = {
        "requested_reviews": max_comments if max_comments > 0 else None,
        "effective_target": eff_display,
        "rows_achieved": fetched_so_far,
        "fetch_rounds_used": 1,
    }
    if target_rows is not None and fetched_so_far < target_rows and not fetch_failed:
        stats["shortfall"] = True
        if expected_total is not None and fetched_so_far >= expected_total:
            stats["shortfall_reason"] = "bgg_total_comments_reached"
        else:
            stats["shortfall_reason"] = "stopped_before_target"
    else:
        stats["shortfall"] = False
        stats["shortfall_reason"] = None

    return expected_total, fetched_so_far, crawl_complete, fetch_failed, stats


def crawl_game_random_pages(
    session: requests.Session,
    game_dir: Path,
    bgg_id: str,
    game_name: str,
    mode: str,
    pagesize: int,
    delay: float,
    max_comments: int,
    expected_total_hint: int | None = None,
    max_fetch_rounds: int = 80,
    stats: ScrapeStats | None = None,
    fsync_mode: str = "manifest",
) -> tuple[int | None, int, bool, bool, dict[str, Any]]:
    fetch_failed = False
    empty_stats: dict[str, Any] = {
        "requested_reviews": max_comments if max_comments > 0 else None,
        "effective_target": None,
        "rows_achieved": 0,
        "shortfall": False,
        "shortfall_reason": None,
        "fetch_rounds_used": 0,
    }

    saved_pages, rows_downloaded, _rows_per_page = load_existing_progress(game_dir)
    expected_total, page_one_failed = bootstrap_page_one(
        session,
        game_dir,
        bgg_id,
        game_name,
        mode,
        pagesize,
        max_comments,
        saved_pages,
        expected_total_hint,
        stats=stats,
        fsync_mode=fsync_mode,
    )
    if page_one_failed or expected_total is None:
        return None, rows_downloaded, False, True, empty_stats

    saved_pages, rows_downloaded, _rows_per_page = load_existing_progress(game_dir)

    num_pages = max(1, math.ceil(expected_total / pagesize)) if expected_total > 0 else 1
    effective_target, eff_warn = compute_effective_target(max_comments, expected_total)
    if eff_warn:
        print(f"  {eff_warn}")
    if max_comments <= 0:
        effective_target = expected_total

    if effective_target is not None and rows_downloaded >= effective_target:
        print(f"  already have {rows_downloaded} rows on disk (target {effective_target})")
        stats = {
            "requested_reviews": max_comments if max_comments > 0 else None,
            "effective_target": effective_target,
            "rows_achieved": rows_downloaded,
            "shortfall": False,
            "shortfall_reason": None,
            "fetch_rounds_used": 0,
        }
        return expected_total, rows_downloaded, True, False, stats

    crawl_complete = False
    round_num = 0
    stagnant_rounds = 0
    pool_exhausted = False

    while (
        effective_target is not None
        and rows_downloaded < effective_target
        and round_num < max_fetch_rounds
    ):
        round_num += 1
        pool = [p for p in range(1, num_pages + 1) if p not in saved_pages]
        if not pool:
            print("  no pages left to fetch (all page files exist)")
            pool_exhausted = True
            break

        rows_at_round_start = rows_downloaded
        print(f"  fetch round {round_num}: {len(pool)} page(s) not yet on disk")

        while effective_target is not None and rows_downloaded < effective_target and pool:
            page = pick_random_unclaimed_page(pool, game_dir)
            if page is None:
                print(
                    f"  could not pick a page after {MAX_RANDOM_PAGE_RESAMPLES} "
                    "random draws — ending this round"
                )
                break

            pfile = page_file_path(game_dir, page)
            t0 = time.perf_counter()
            xml_text, downloaded_bytes, retries = fetch_comments_page(
                session, bgg_id, mode, page, pagesize
            )
            if stats:
                stats.fetch_calls += 1
                stats.fetch_retries += retries
                stats.bytes_downloaded += downloaded_bytes
                stats.fetch_latency_s += time.perf_counter() - t0
            if not xml_text:
                if stats:
                    stats.fetch_failures += 1
                print(f"  page {page}: temporary failure — will retry in a later round")
                continue

            try:
                p0 = time.perf_counter()
                _, reviews = parse_comments_xml(xml_text, bgg_id, game_name, page)
                if stats:
                    stats.parse_latency_s += time.perf_counter() - p0
            except ET.ParseError as exc:
                print(f"  page {page}: parse error — will retry later: {exc}")
                continue

            remaining = (
                (effective_target - rows_downloaded) if effective_target is not None else len(reviews)
            )
            if remaining <= 0:
                crawl_complete = True
                break
            if len(reviews) > remaining:
                reviews = reviews[:remaining]

            w0 = time.perf_counter()
            write_jsonl_atomic(pfile, reviews, fsync_enabled=(fsync_mode == "always"))
            if stats:
                stats.write_latency_s += time.perf_counter() - w0
                stats.pages_written += 1
                stats.rows_written += len(reviews)
            saved_pages.add(page)
            rows_downloaded += len(reviews)
            if page in pool:
                pool.remove(page)
            print(f"  page {page}: downloaded ({len(reviews)} rows)")

            if effective_target is not None and rows_downloaded >= effective_target:
                crawl_complete = True
                break
            if stats.fetch_calls!=0:
                time.sleep(delay)

        if rows_downloaded >= (effective_target or 0):
            crawl_complete = True
            break

        if rows_downloaded == rows_at_round_start:
            stagnant_rounds += 1
            if stagnant_rounds >= 3:
                print("  stopping retries: no new rows in 3 consecutive rounds")
                break
        else:
            stagnant_rounds = 0

    final_rows = rows_downloaded
    if not fetch_failed and effective_target is not None and final_rows >= effective_target:
        crawl_complete = True

    stats: dict[str, Any] = {
        "requested_reviews": max_comments if max_comments > 0 else None,
        "effective_target": effective_target,
        "rows_achieved": final_rows,
        "fetch_rounds_used": round_num,
    }
    if effective_target is None:
        stats["shortfall"] = False
        stats["shortfall_reason"] = None
    elif final_rows < effective_target and not fetch_failed:
        stats["shortfall"] = True
        if expected_total is not None and final_rows >= expected_total:
            stats["shortfall_reason"] = "bgg_total_comments_reached"
        elif pool_exhausted:
            stats["shortfall_reason"] = "no_pages_left_on_disk_pool"
        else:
            stats["shortfall_reason"] = "retries_exhausted_or_stagnant"
    else:
        stats["shortfall"] = False
        stats["shortfall_reason"] = None

    return expected_total, final_rows, crawl_complete, fetch_failed, stats


async def crawl_game_sequential_async(
    game_dir: Path,
    bgg_id: str,
    game_name: str,
    mode: str,
    pagesize: int,
    max_pages: int,
    max_comments: int,
    expected_total_hint: int | None,
    concurrency: int,
    max_rps: float,
    burst: int,
    stats: ScrapeStats | None = None,
    fsync_mode: str = "manifest",
) -> tuple[int | None, int, bool, bool, dict[str, Any]]:
    if httpx is None:
        raise RuntimeError("Async engine requires httpx. Install with: pip install httpx")
    saved_pages, rows_downloaded, _rows_per_page = load_existing_progress(game_dir)
    expected_total = expected_total_hint or expected_total_from_manifest(game_dir)
    fetch_failed = False
    crawl_complete = False

    token = (API_TOKEN or "").strip()
    headers = {"User-Agent": "bgg_reviews_scraper (Vishal Sankarram)"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    limiter = TokenBucket(max_rps, burst)
    limits = httpx.Limits(max_connections=max(2, concurrency * 2), max_keepalive_connections=concurrency)

    async with httpx.AsyncClient(headers=headers, limits=limits) as client:
        if expected_total is None:
            t0 = time.perf_counter()
            xml_text, downloaded_bytes, retries = await fetch_comments_page_async(
                client, limiter, bgg_id, mode, 1, pagesize
            )
            if stats:
                stats.fetch_calls += 1
                stats.fetch_retries += retries
                stats.bytes_downloaded += downloaded_bytes
                stats.fetch_latency_s += time.perf_counter() - t0
            if not xml_text:
                if stats:
                    stats.fetch_failures += 1
                return None, rows_downloaded, False, True, {"fetch_rounds_used": 1}
            try:
                p0 = time.perf_counter()
                totalitems, reviews = parse_comments_xml(xml_text, bgg_id, game_name, 1)
                if stats:
                    stats.parse_latency_s += time.perf_counter() - p0
            except ET.ParseError:
                return None, rows_downloaded, False, True, {"fetch_rounds_used": 1}
            expected_total = totalitems
            m = read_manifest(manifest_path(game_dir))
            m["expected_total"] = expected_total
            write_json_atomic(manifest_path(game_dir), m, fsync_enabled=(fsync_mode != "never"))
            if 1 not in saved_pages:
                w0 = time.perf_counter()
                write_jsonl_atomic(
                    page_file_path(game_dir, 1), reviews, fsync_enabled=(fsync_mode == "always")
                )
                if stats:
                    stats.write_latency_s += time.perf_counter() - w0
                    stats.pages_written += 1
                    stats.rows_written += len(reviews)
                saved_pages.add(1)
                rows_downloaded += len(reviews)

        assert expected_total is not None
        target_rows, _w = compute_effective_target(max_comments, expected_total)
        if target_rows is None:
            target_rows = expected_total

        max_page_by_total = max(1, math.ceil(expected_total / pagesize))
        page_ceiling = min(max_page_by_total, max_pages) if max_pages > 0 else max_page_by_total
        needed_pages_for_target = max(1, math.ceil(target_rows / pagesize))
        page_ceiling = min(page_ceiling, needed_pages_for_target)
        pages_to_fetch = [p for p in range(1, page_ceiling + 1) if p not in saved_pages]
        if not pages_to_fetch or rows_downloaded >= target_rows:
            crawl_complete = True
            return expected_total, rows_downloaded, crawl_complete, False, {
                "requested_reviews": max_comments if max_comments > 0 else None,
                "effective_target": target_rows,
                "rows_achieved": rows_downloaded,
                "shortfall": False,
                "shortfall_reason": None,
                "fetch_rounds_used": 1,
            }

        sem = asyncio.Semaphore(max(1, concurrency))

        async def worker(page: int) -> tuple[int, list[dict[str, Any]] | None]:
            async with sem:
                t0 = time.perf_counter()
                xml_text, downloaded_bytes, retries = await fetch_comments_page_async(
                    client, limiter, bgg_id, mode, page, pagesize
                )
                if stats:
                    stats.fetch_calls += 1
                    stats.fetch_retries += retries
                    stats.bytes_downloaded += downloaded_bytes
                    stats.fetch_latency_s += time.perf_counter() - t0
                if not xml_text:
                    if stats:
                        stats.fetch_failures += 1
                    return page, None
                try:
                    p0 = time.perf_counter()
                    _, reviews = parse_comments_xml(xml_text, bgg_id, game_name, page)
                    if stats:
                        stats.parse_latency_s += time.perf_counter() - p0
                except ET.ParseError:
                    return page, None
                return page, reviews

        results = await asyncio.gather(*(worker(page) for page in pages_to_fetch))
        result_map = {page: reviews for page, reviews in results}
        target_reached = rows_downloaded >= target_rows
        for page in sorted(pages_to_fetch):
            if target_reached:
                break
            reviews = result_map.get(page)
            if reviews is None:
                fetch_failed = True
                continue
            remaining = target_rows - rows_downloaded
            if remaining <= 0:
                target_reached = True
                break
            if len(reviews) > remaining:
                reviews = reviews[:remaining]
            if not reviews:
                continue
            w0 = time.perf_counter()
            write_jsonl_atomic(
                page_file_path(game_dir, page), reviews, fsync_enabled=(fsync_mode == "always")
            )
            if stats:
                stats.write_latency_s += time.perf_counter() - w0
                stats.pages_written += 1
                stats.rows_written += len(reviews)
            saved_pages.add(page)
            rows_downloaded += len(reviews)
            target_reached = rows_downloaded >= target_rows

    crawl_complete = rows_downloaded >= target_rows or rows_downloaded >= expected_total
    stats_out: dict[str, Any] = {
        "requested_reviews": max_comments if max_comments > 0 else None,
        "effective_target": target_rows,
        "rows_achieved": rows_downloaded,
        "fetch_rounds_used": 1,
        "shortfall": rows_downloaded < target_rows and not fetch_failed,
        "shortfall_reason": None,
    }
    if stats_out["shortfall"]:
        stats_out["shortfall_reason"] = "stopped_before_target"
    return expected_total, rows_downloaded, crawl_complete, fetch_failed, stats_out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download BGG comments to output_dir/<bgg_id>/page_*.jsonl (resume by existing files)."
    )
    parser.add_argument("--ranked-only", action="store_true")
    parser.add_argument("--no-expansions", action="store_true")
    parser.add_argument("--mode", choices=["comments", "ratingcomments"], default="comments")
    parser.add_argument("--pagesize", type=int, default=100)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--max-pages", type=int, default=0, help="0 = no cap")
    parser.add_argument("--max-comments-per-game", type=int, default=0, help="0 = no cap")
    parser.add_argument("--max-games", type=int, default=0, help="0 = all")
    parser.add_argument(
        "--skip-games",
        type=int,
        default=0,
        help="Skip first N CSV rows after filters (stable only if CSV order/filters unchanged); applied before --max-games",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip games whose manifest has crawl_complete and matches mode/pagesize/page_order/max-comments",
    )
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument(
        "--page-order",
        choices=["sequential", "random"],
        default="sequential",
        help="random: sample pages not yet saved on disk",
    )
    parser.add_argument("--random-seed", type=int, default=None)
    parser.add_argument("--engine", choices=["sync", "async"], default="sync")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--max-rps", type=float, default=DEFAULT_MAX_RPS)
    parser.add_argument("--burst", type=int, default=DEFAULT_BURST)
    parser.add_argument(
        "--fsync-mode",
        choices=["always", "manifest", "never"],
        default="manifest",
        help="Durability policy for writes: always fsync page+manifest, manifest only, or never",
    )
    parser.add_argument(
        "--manifest-every-pages",
        type=int,
        default=DEFAULT_MANIFEST_EVERY_PAGES,
        help="Checkpoint manifest every N newly downloaded pages (best effort)",
    )
    args = parser.parse_args()

    if args.pagesize < 10 or args.pagesize > 100:
        raise ValueError("--pagesize must be between 10 and 100")
    if args.delay < 0:
        raise ValueError("--delay must be >= 0")
    if args.random_seed is not None:
        random.seed(args.random_seed)
    if args.skip_games < 0:
        raise ValueError("--skip-games must be >= 0")
    if args.concurrency < 1:
        raise ValueError("--concurrency must be >= 1")
    if args.max_rps <= 0:
        raise ValueError("--max-rps must be > 0")
    if args.burst < 1:
        raise ValueError("--burst must be >= 1")
    if args.manifest_every_pages < 1:
        raise ValueError("--manifest-every-pages must be >= 1")
    if args.engine == "async" and httpx is None:
        raise RuntimeError("--engine async requires httpx (pip install httpx)")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    games = load_games(ranked_only=args.ranked_only, no_expansions=args.no_expansions)
    if args.skip_games > 0:
        games = games[args.skip_games :]
    if args.max_games > 0:
        games = games[: args.max_games]

    print(f"Loaded {len(games)} games (after --skip-games / --max-games)")
    print(f"Output dir: {args.output_dir}")
    print(f"Mode={args.mode}, pagesize={args.pagesize}, delay={args.delay}s")
    print(f"page-order={args.page_order}, resume={args.resume}, skip_games={args.skip_games}")

    session = get_session()
    total_new_pages = 0
    run_stats = ScrapeStats()
    run_stats.start()

    for idx, game in enumerate(games, start=1):
        bgg_id = game["bgg_id"]
        game_name = game["name"]
        print(f"\n[{idx}/{len(games)}] {game_name} ({bgg_id})")

        game_dir = game_dir_path(args.output_dir, bgg_id)
        game_dir.mkdir(parents=True, exist_ok=True)

        mpath = manifest_path(game_dir)
        manifest = read_manifest(mpath)
        if args.resume and should_skip_completed_game(manifest, args):
            print(f"\n[{idx}/{len(games)}] {game_name} ({bgg_id}) — skip, already complete")
            continue
        saved = load_saved_page_numbers(game_dir)
        page_files = sorted(game_dir.glob("page_*.jsonl"))
        print(f"  page_*.jsonl on disk ({len(page_files)} files): {[p.name for p in page_files]}")
        print(f"  saved page numbers: {sorted(saved)}")

        et_hint = expected_total_from_manifest(game_dir)
        pages_before = len(page_files)
        game_stats = ScrapeStats()
        game_stats.start()

        if args.engine == "async" and args.page_order == "random":
            print("  async engine currently supports sequential page order only; using sync random mode")

        if args.page_order == "random" or args.engine == "sync":
            expected_total, fetched_so_far, crawl_complete, fetch_failed, _stats = (
                crawl_game_random_pages(
                    session,
                    game_dir,
                    bgg_id,
                    game_name,
                    args.mode,
                    args.pagesize,
                    args.delay,
                    args.max_comments_per_game,
                    et_hint,
                    stats=game_stats,
                    fsync_mode=args.fsync_mode,
                )
                if args.page_order == "random"
                else crawl_game_sequential(
                    session,
                    game_dir,
                    bgg_id,
                    game_name,
                    args.mode,
                    args.pagesize,
                    args.delay,
                    args.max_pages,
                    args.max_comments_per_game,
                    et_hint,
                    stats=game_stats,
                    fsync_mode=args.fsync_mode,
                )
            )
        else:
            expected_total, fetched_so_far, crawl_complete, fetch_failed, _stats = asyncio.run(
                crawl_game_sequential_async(
                    game_dir=game_dir,
                    bgg_id=bgg_id,
                    game_name=game_name,
                    mode=args.mode,
                    pagesize=args.pagesize,
                    max_pages=args.max_pages,
                    max_comments=args.max_comments_per_game,
                    expected_total_hint=et_hint,
                    concurrency=args.concurrency,
                    max_rps=args.max_rps,
                    burst=args.burst,
                    stats=game_stats,
                    fsync_mode=args.fsync_mode,
                )
            )

        existing_pages = load_saved_page_numbers(game_dir)
        pages_after = len(list(game_dir.glob("page_*.jsonl")))
        total_new_pages += max(0, pages_after - pages_before)

        manifest.update(
            {
                "bgg_id": bgg_id,
                "game_name": game_name,
                "mode": args.mode,
                "pagesize": args.pagesize,
                "page_order": args.page_order,
                "max_comments_per_game": args.max_comments_per_game,
                "expected_total": expected_total,
                "fetched_rows_estimate": fetched_so_far,
                "crawl_complete": crawl_complete and not fetch_failed,
                "max_saved_page": max(existing_pages) if existing_pages else 0,
            }
        )
        write_json_atomic(mpath, manifest, fsync_enabled=(args.fsync_mode != "never"))
        print(
            f"  done: rows≈{fetched_so_far}, complete={manifest['crawl_complete']}, "
            f"max_page={manifest['max_saved_page']}"
        )
        print(f"  manifest: {mpath}")
        print(f"  perf: {json.dumps(game_stats.as_dict(), ensure_ascii=False)}")
        for key in (
            "fetch_calls",
            "fetch_retries",
            "fetch_failures",
            "fetch_latency_s",
            "parse_latency_s",
            "write_latency_s",
            "bytes_downloaded",
            "pages_written",
            "rows_written",
        ):
            run_stats.__dict__[key] += game_stats.__dict__[key]
        if game_stats.__dict__["fetch_calls"]!=0:
            time.sleep(args.delay)
    print(f"\nDone. New page files downloaded this run: {total_new_pages}")
    print(f"Run perf: {json.dumps(run_stats.as_dict(), ensure_ascii=False)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
