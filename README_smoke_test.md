# BGG Review Scraper Smoke Test

This README explains how to run a small smoke test for `bgg_review_scraper.py`.

## Knowledge graph CSV export (Neo4j) and overlap-only games

To rebuild `neo4j/import/` from local data, use [`scripts/build_neo4j_csvs.py`](scripts/build_neo4j_csvs.py). Install deps with `pip install -r requirements.txt` (from the project root).

**Overlap-only KG** (only games that appear in both Board Game Oracle and BGG per [`bgo_key_bgg_map.tsv`](bgo_key_bgg_map.tsv)):

```bash
python3 scripts/build_neo4j_csvs.py --overlap-only
```

Combine with the same limit flags as a full export when smoke-testing:

```bash
python3 scripts/build_neo4j_csvs.py --overlap-only \
  --limit-games 500 \
  --limit-price-files 20 \
  --limit-reviews 100 \
  --limit-ranks 500
```

BGQ reviews are linked to `Game` nodes only when the article `game_name` matches BGG `Game.name` case-insensitively (no approximate string matching).

## 0) Prerequisite data file

Make sure `boardgames_ranks.csv` exists in the project root (same folder as `bgg_review_scraper.py`).

## 1) Add your API token

Prefer environment variable (safest for git):

```bash
export BGG_API_TOKEN='your_token_here'
```

Or set `API_TOKEN` in `bgg_review_scraper.py` for local runs only. Do not commit real tokens.

## 2) Create and use a virtual environment

From the project root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install requests
```

## 3) Run a smoke test (small and fast)

This runs against 2 games, only 1 page, and writes per-game/page files:

```bash
.venv/bin/python bgg_review_scraper.py \
  --max-games 2 \
  --max-pages 1 \
  --pagesize 10 \
  --output-dir game_reviews_smoke
```

For a clean rerun, remove the previous smoke output first:

```bash
rm -rf game_reviews_smoke
```

## 4) Check output

Preview first lines:

This should create folders like:

```text
game_reviews_smoke/
  224517/
    page_0001.jsonl
    manifest.json
  342942/
    page_0001.jsonl
    manifest.json
```

Each review JSON row includes:
- `review_key`
- `bgg_id`
- `game_name`
- `username`
- `rating`
- `comment_text`
- `page`

## 5) Run full collection later

Example command for a larger run:

```bash
.venv/bin/python bgg_review_scraper.py \
  --ranked-only \
  --pagesize 100 \
  --output-dir game_review_batches
```

## 6) Full crawl (all games, all reviews, safe defaults)

Defaults are tuned for a full run: **all rows in the CSV**, **all pages** until BGG reports the game is done, **100 comments per request** (fewest API calls), **5 seconds** between requests (BGG guidance), **exponential backoff** on 202/5xx/429, and **resume by page files** (missing pages are re-downloaded).

```bash
export BGG_API_TOKEN='your_token'
.venv/bin/python bgg_review_scraper.py --output-dir game_review_batches
```

For each game:

- `page_0001.jsonl`, `page_0002.jsonl`, ...
- `manifest.json` with crawl status (`expected_total`, `crawl_complete`, `max_saved_page`, `max_comments_per_game`, …)

To re-download everything from scratch: remove the output directory. To resume: rerun with the same `--output-dir`; existing `page_*.jsonl` files are skipped (page numbers come from filenames).

**Large CSVs (many games):** use `--resume` so games that already have `crawl_complete: true` in `manifest.json` are skipped quickly (no per-game page listing, no delay). Skipping only happens when `mode`, `pagesize`, `page_order`, and `max_comments_per_game` in the manifest match the current command. Use `--skip-games N` to drop the first *N* CSV rows after filters (same CSV order and filters as before); order is: load CSV → apply `--skip-games` → apply `--max-games`.

```bash
.venv/bin/python bgg_review_scraper.py \
  --output-dir game_review_batches \
  --resume
```

Optional safety knobs (only if you need limits):

- `--max-games N` — limit how many CSV rows to process (after `--skip-games`)
- `--skip-games N` — skip the first N rows after `--ranked-only` / `--no-expansions`
- `--max-pages N` — per-game page cap (not used by default; incomplete games are not marked complete)
- `--max-comments-per-game N` — cap reviews per game
- `--delay 5` — seconds between requests (increase if BGG returns 429 often)

### Layout and resume

Each game is a single folder `game_review_batches/<bgg_id>/` with `page_0001.jsonl`, … and `manifest.json`. The scraper builds a **set of page numbers** from `page_*.jsonl` filenames (and prints them at startup for games it actually crawls). It does **not** use a separate registry file—only `manifest.json` stores extras like `expected_total` from BGG.

Incomplete games still resume by **page files** on disk. Finished games are skipped in bulk with **`--resume`** when the manifest says complete and matches your flags.

If you still have files under an old `default/` subfolder (from a previous scraper version), move `page_*.jsonl` into `game_review_batches/<bgg_id>/` so resume works.

### Random page batches (~1k per game, less rating-order bias than “first pages only”)

BGG returns comments in rating-heavy order. To **not** only take the first sequential pages, use **random page order** with a cap:

```bash
.venv/bin/python bgg_review_scraper.py \
  --page-order random \
  --max-comments-per-game 1000 \
  --pagesize 100 \
  --output-dir game_review_batches \
  --random-seed 42
```

Behavior: one request always loads **page 1** (needed for `totalitems`). Further pages are chosen **at random** among page indices that do **not** yet have a `page_*.jsonl` file. If a draw collides with an existing file, the scraper **redraws** up to **5** times per attempt, then ends that inner attempt and continues with the next outer round. Progress stops when the row target is met, every page index has a file, or retry limits apply. This samples **random pages**, not a perfectly uniform sample of individual comments (comments are still clumped within each page).

## Optional useful flags

- `--mode comments` (default)
- `--mode ratingcomments`
- `--max-comments-per-game 200`
- `--no-expansions`
- `--output-dir /path/to/review_batches`
- `--page-order sequential` (default) or `random`
- `--random-seed N` — fixed RNG seed for `random` page draws (Python `random` module)
- `--resume` — skip games already `crawl_complete` with matching manifest knobs
- `--skip-games N` — skip first N CSV rows after filters (before `--max-games`)

