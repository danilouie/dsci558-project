# Per-game feature export

This folder builds **`features_per_game.parquet`** (one row per `bgg_id`) from your embedding index, optional **per-review sentiment** aggregates (off by default; enable with **`--include-sentiment-features`**), optional **value-concept prompts** encoded with the same SentenceTransformer family as FAISS, plus **BGG tabular fields** from Neo4j CSVs, **`description_embedding`** (`game_description` vectors), **BGO price scalars** (via `bgo_key_bgg_map.tsv` → `price_histories/`), and **`stage_a_split` / `stage_c_split`** aligned with [`docs/WORTH_MODEL.md`](../docs/WORTH_MODEL.md).

Run from repo root with **`PYTHONPATH=.`** (or `pip install -e .` if you package the repo).

---

## Prerequisites

1. **Python env** with dependencies (from repo root or this folder):

   ```bash
   cd /path/to/dsci558-project
   pip install -r requirements.txt
   pip install -r game_feature_export/requirements.txt
   ```

2. **Embedding artifacts** under e.g. `embeddings/all-MiniLM-L6-v2/` with **`meta.json`**, **`id_map.parquet`**, **`vectors.npy`** (or `index.faiss` Flat only), and **`shards/shard_*.parquet`**.

3. **Shard text:** the sentiment batch job needs a **`text`** column in shards. If you built the index **without** `--store-text`, rebuild embeddings with **`--store-text`** on `scripts/build_embeddings_faiss.py` (see `embeddings/README.md`).

4. **`neo4j/import`** (or pass `--neo4j-import`): contains **`games.csv`**, **`ranks.csv`**, **`reviews.csv`** (BGQ, `review_id`, `author`, `score`), and **`bgg_reviews.tsv`** or **`bgg_reviews_*.tsv`** (BGG review id + `username`) so reviewer concentration + tabular merges resolve.

   **Collection edges (BGG user collections):** if present, **`user_game_owns.csv`**, **`user_game_wants.csv`**, **`user_game_wants_to_buy.csv`**, **`user_game_wants_to_trade.csv`** (headers include `owner_username`, `bgg_id`; see [`neo4j/SCHEMA.md`](../neo4j/SCHEMA.md)) are scanned to populate **`coll_share_*`** columns: each relationship’s row count divided by **(owns + wants + wants_to_buy + wants_to_trade)** per game (same denominator for all four). If only chunked copies exist under **`bgg_rel_chunks/`** (flat file missing or empty), those chunks are used instead—never double-count flats and chunks. **`scripts/preprocess_features_parquet.py`** does **not** Robust-scale **`coll_share_*`** (already normalized).

5. **Repo-level data (defaults):** [`bgo_key_bgg_map.tsv`](../bgo_key_bgg_map.tsv) and the [`price_histories/`](../price_histories/) directory are read from the **current working directory** unless you pass **`--repo-root`**, **`--bgo-map`**, or **`--price-histories`**. Duplicate `bgg_id` rows in the TSV resolve to the **lexicographically smallest `key`** (see `run_meta.json` → `bgo_duplicate_bgg_ids`).

---

## Order of commands

- **With sentiment features:** run **Step 1 (sentiment)**, then **Step 2** with **`--include-sentiment-features`** and **`--sentiment-parquet`**.
- **Without sentiment features:** skip Step 1; run **Step 2** only (embedding + reviewer + optional value columns; no **`--sentiment-parquet`** needed).

### Step 1 — Score every review (writes sentiment sidecar)

Uses Hugging Face `sentiment-analysis`; default device is **`mps`** (Apple Silicon). On CPU-only machines pass **`--device cpu`** or **`--device -1`**.

```bash
cd /path/to/dsci558-project

python -m game_feature_export.review_sentiment \
  --embedding-root embeddings/all-MiniLM-L6-v2 \
  --sentiment-model cardiffnlp/twitter-roberta-base-sentiment-latest \
  --batch-size 32
```

Outputs (default):

- `game_feature_export/review_sentiment/artifacts/<UTC>/review_sentiment.parquet`
- `game_feature_export/review_sentiment/artifacts/<UTC>/review_sentiment_run_meta.json`

Optional:

- **`--out /custom/path/review_sentiment.parquet`** — fixed output location
- **`--max-rows 5000`** — smoke test without full corpus
- **`--device cpu`** — if `mps` fails or you are not on Apple Silicon

Columns: **`faiss_id`**, **`sentiment_score`**, **`sentiment_model`**, **`doc_kind`** (only `bgq_review` / `bgg_review` rows).

---

### Step 2 — Aggregate to per-game features

**Embedding-only (default — no sentiment columns):**

```bash
PYTHONPATH=. python -m game_feature_export \
  --embedding-root embeddings/all-MiniLM-L6-v2 \
  --neo4j-import neo4j/import \
  --output-dir game_feature_export/artifacts/my_run_v1
```

**With sentiment aggregates** (requires Step 1 output): pass **`--include-sentiment-features`** and **`--sentiment-parquet`**. Optionally add **`--good-value-text`** / **`--bad-value-text`** so concepts are encoded with **`--concept-encoder-model`** (default: model name from embedding **`meta.json`**, aligned with FAISS).

```bash
python -m game_feature_export \
  --embedding-root embeddings/all-MiniLM-L6-v2 \
  --neo4j-import neo4j/import \
  --include-sentiment-features \
  --sentiment-parquet game_feature_export/review_sentiment/artifacts/<UTC>/review_sentiment.parquet \
  --output-dir game_feature_export/artifacts/my_run_v1 \
  --good-value-text "This game is excellent value for money." \
  --bad-value-text "This game is overpriced and not worth the price."
```

Optional:

- **`--include-sentiment-features`** — add sentiment mean/std/fractions/etc.; requires **`--sentiment-parquet`**
- **`--concept-encoder-model sentence-transformers/all-MiniLM-L6-v2`** — override model id (otherwise read from `meta.json`)
- **`--concept-encoder-device mps`** — faster encoding on Apple Silicon (default encoder path uses CPU inside `sentence-transformers` unless you set this)
- **`--extended`** — extra sentiment columns (quartiles, neutral fraction, IQR); only valid with **`--include-sentiment-features`**
- **`--price-features extended|core`** — full BGO-derived price scalars vs Stage B core only (`log1p_last_mean`, `n_weeks_observed`, `price_slope_4w`, `price_vol`, `price_coverage`)
- **`--price-as-of ISO_DATETIME`** — UTC cutoff when parsing weekly JSON rows (default: use series end)
- **`--split-seed`**, **`--stage-a-extra-test-fraction`** — Stage A split: every game with **`bgq_review`** in `id_map` gets **`stage_a_split=test`**; optional random fraction of remaining games also **`test`**
- **`--splits-json`** — optional frozen **`train_ids` / `val_ids` / `test_ids`** for **`stage_c_split`**; otherwise **`splits.json`** is written under **`--output-dir`** using BGQ scores from **`reviews.csv`** (stratified ~560/70/70 when \(N{\approx}700\))
- **`--skip-price-features`**, **`--skip-bgg-tabular`**, **`--skip-collection-features`**, **`--skip-description-embedding`**, **`--skip-splits`** — omit blocks for debugging (`--skip-collection-features` sets **`coll_share_*`** to NaN without scanning collection CSVs)

Outputs:

- `features_per_game.parquet` (includes **`coll_share_owns`**, **`coll_share_wants`**, **`coll_share_wtb`**, **`coll_share_wtt`** unless skipped)
- `run_meta.json`
- `splits.json` (unless **`--no-write-splits-json`** or **`--skip-splits`** or **`--splits-json`** loads an existing file without regenerating — generation always writes when cohort is built)

---

## Quick sanity check

After Step 1, inspect row counts vs reviews-only rows in shards. After Step 2, open **`run_meta.json`** and confirm **`joined_review_rows`** and **`rows_written`** look reasonable.

---

## Optional: standardize features for ML/DL

[`preprocessing.py`](preprocessing.py) fits **RobustScaler** or **StandardScaler** on tabular columns **using train rows only**, optionally **L2-normalizes** `mean_embedding`, writes a standardized parquet and a **`PreprocessBundle`** (`joblib`) for inference.

```bash
PYTHONPATH=. python scripts/preprocess_features_parquet.py \
  --input game_feature_export/artifacts/embed_only/features_per_game.parquet \
  --output game_feature_export/artifacts/embed_only/features_standardized.parquet \
  --train-fraction 1.0 \
  --reserve-bgq-for-test \
  --embedding-root embeddings/all-MiniLM-L6-v2-full \
  --tabular-scaler robust \
  --embedding l2 \
  --nn-safe \
  --divide-price-columns-by-mean \
  --pipeline-out game_feature_export/artifacts/embed_only/preprocess.joblib
```

**Price scaling:** **`--divide-price-columns-by-mean`** divides level-like price columns by the **FIT-split** column mean before Robust/Standard scaling (see [`schema.py`](schema.py) `DEFAULT_PRICE_COLUMNS_MEAN_DIVIDE`). Alternatively pass an explicit comma list via **`--divide-columns-by-mean col1,col2,...`**.

**Second embedding:** When **`description_embedding`** is present, preprocessing applies the same **`--embedding`** policy (e.g. L2) as **`mean_embedding`** but does **not** fold it into the median tabular norm match (that still uses **`mean_embedding`** only).

For **neural nets**, **`--nn-safe`** (when you do not pass **`--tabular-clip`**) sets tabular bounds from FIT data: **quantile 0.99** of **|**scaled tabular**|**, then **caps at 4** (tunable via **`--tabular-clip-quantile`**, **`--tabular-clip-cap`**, or a fixed **`--tabular-clip`**). By default it also enables **`--match-embedding-tabular-scale`** so **median row L2 norm** of embeddings matches the tabular block. Use **`--no-match-embedding-tabular-scale`** to disable. Final clip and **`embedding_block_scale`** are stored in **`preprocess.joblib`**.

**BGQ pool (pick one; requires `--embedding-root` with `id_map.parquet`):**

- **`--reserve-bgq-for-test`** — FIT the scaler **only** on games with **no** **`bgq_review`** in `id_map` (typical: treat BGQ games as **test/eval**). Use **`--train-fraction 1.0`** to fit on **all** non-BGQ rows, or **`< 1`** for an extra random subset of non-BGQ for validation.
- **`--fit-bgq-review-games`** — FIT **only** on BGQ games (mutually exclusive with **`--reserve-bgq-for-test`**).

All rows in **`features_per_game.parquet`** are still written to the standardized parquet and transformed with that bundle.

Requires **`pip install scikit-learn`** (`game_feature_export/requirements.txt`).

**Pipe-field one-hot (`categories` / `mechanisms`):** By default, preprocessing builds **multi-label** binary columns from pipe-separated BGG strings (FIT-split vocabulary ordered by token frequency). Columns are named `cat__*` and `mech__*` and are **not** passed through RobustScaler (they stay 0/1). Disable with **`--skip-one-hot-pipe-fields`**, or skip one side with **`--no-one-hot-categories`** / **`--no-one-hot-mechanisms`**. Caps: **`--one-hot-max-category-tokens`** / **`--one-hot-max-mechanism-tokens`** (default `256`; use **`0`** for no limit).

---

## Troubleshooting

| Issue | What to do |
|--------|------------|
| Sentiment run **interrupted** | Re-run with the **same** flags plus **`--resume`**. Intermediate state is **`_checkpoint.json`** and a **`_parts/`** directory with **`part_*.parquet`** next to **`--out`**; when the run completes, everything is merged into **`--out`**. Use **`--overwrite`** to discard a partial run and start clean. |
| Embedding build **interrupted** | Use **`--resume`** on **`scripts/build_embeddings_faiss.py`** with the **same** `--output` and matching flags (**requires `vectors.npy`**; see **`embeddings/README.md`**). |
| Shard has no **`text`** | Rebuild embeddings with **`--store-text`**. |
| **`mps`** fails in Step 1 | **`--device cpu`** or **`--device -1`**. |
| Missing **`vectors.npy`** and IVF index | Regenerate **`vectors.npy`** or use a Flat index; non-Flat reconstruct path is restricted. |
| Few / zero games after Step 2 | With **`--include-sentiment-features`**, **`faiss_id`** must align between sentiment parquet and **`id_map`** (inner join). Without sentiment, all **`id_map`** review rows with non-empty **`bgg_id`** are used. |
