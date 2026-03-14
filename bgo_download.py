import json
import os
import re
import time
from pathlib import Path
from typing import Generator, Tuple

import requests


PRICE_HISTORY_URL = "https://www.boardgameoracle.com/api/trpc/pricehistory.list"
BOARDGAME_LIST_URL = "https://www.boardgameoracle.com/api/trpc/boardgame.list"
OUTPUT_DIR = Path("price_histories")
KEY_NAME_FILE = OUTPUT_DIR / "key_name.tsv"
REQUEST_DELAY_SECONDS = 0.1  # polite delay between API calls

session = requests.Session()


def iter_games(
    max_pages: int | None = None, delay_seconds: float = 0.2
) -> Generator[Tuple[str, str], None, None]:
    """
    Iterate over (key, title) using the official boardgame.list API.

    Uses the same filters as the website (board games + expansions in US region),
    and paginates via the `cursor` field.
    """
    cursor = 1
    pages_fetched = 0

    while True:
        input_payload = {
            "0": {
                "region": "us",
                "filters": {
                    "player": {
                        "min": {"comparator": "eq"},
                        "max": {"comparator": "eq"},
                    },
                    "playtime": {
                        "min": {"comparator": "eq"},
                        "max": {"comparator": "eq"},
                    },
                    "type": ["boardgame", "boardgameexpansion"],
                },
                "sort": "relevance",
                "cursor": cursor,
            }
        }

        params = {
            "batch": "1",
            "input": json.dumps(input_payload),
        }

        resp = session.get(BOARDGAME_LIST_URL, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        if not payload:
            break

        data = payload[0]["result"]["data"]
        items = data.get("items", [])

        if not items:
            break

        for item in items:
            key = item.get("key")
            title = item.get("title") or ""
            if not key:
                continue
            yield key, title

        pages_fetched += 1
        has_next = data.get("hasNextPage")
        if not has_next:
            break

        cursor = data.get("page", cursor) + 1

        if max_pages is not None and pages_fetched >= max_pages:
            break

        time.sleep(delay_seconds)


def fetch_price_history(key: str, region: str = "us", range_: str = "max") -> dict:
    """
    Fetch price history for a single game key.
    """
    input_payload = {"0": {"region": region, "key": key, "range": range_}}
    params = {
        "batch": "1",
        "input": json.dumps(input_payload),
    }

    resp = session.get(PRICE_HISTORY_URL, params=params, timeout=20)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    # Configure how many games you want to scrape in one run.
    # Set max_games to None to fetch ALL games available in Board Game Oracle.
    max_games: int | None = None
    max_pages = None  # or set to an int if you want to cap by pages instead

    OUTPUT_DIR.mkdir(exist_ok=True)

    results = []
    # Create the mapping file with a header if it doesn't exist yet.
    if not KEY_NAME_FILE.exists():
        with open(KEY_NAME_FILE, "w", encoding="utf-8") as f:
            f.write("key\tname\n")

    for i, (key, name) in enumerate(iter_games(max_pages=max_pages), start=1):
        if max_games is not None and i > max_games:
            break

        try:
            data = fetch_price_history(key)
        except Exception as e:
            # Skip games that fail for any reason.
            print(f"Failed to fetch price history for key={key}, name={name}: {e}")
            continue

        record = {"key": key, "name": name, "price_history": data}
        results.append(record)

        # Write each game's data to its own JSON file (key only to avoid long filenames).
        file_path = OUTPUT_DIR / f"{key}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        # Append key->name mapping (TSV) for later entity matching.
        safe_name_for_tsv = name.replace("\t", " ").replace("\n", " ").replace("\r", " ").strip()
        with open(KEY_NAME_FILE, "a", encoding="utf-8") as f:
            f.write(f"{key}\t{safe_name_for_tsv}\n")

        print(f"Fetched {i} games (latest key={key}, name={name}) -> {file_path}")

        # Be gentle on the API.
        time.sleep(REQUEST_DELAY_SECONDS)


if __name__ == "__main__":
    main()