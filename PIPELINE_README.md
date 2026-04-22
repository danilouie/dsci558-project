# ML feature pipeline — step-by-step

This document is a **single ordered checklist** from raw project data to **tabular + embedding features** ready for classical ML or neural nets. For deep dives, use the linked READMEs.

| Topic | Where to read more |
|--------|-------------------|
| FAISS build, `--resume`, shards, `vectors.npy` | [`embeddings/README.md`](embeddings/README.md) |
| Per-game parquet, sentiment, Neo4j joins | [`game_feature_export/README.md`](game_feature_export/README.md) |
| Neo4j CSV ETL and graph load | [`README.md`](README.md) (Neo4j section) |
| Worth model flowchart (demand + BGO price + weak label) | [`docs/WORTH_MODEL.md`](docs/WORTH_MODEL.md) |

---

## Prerequisites

From the repo root:

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -r game_feature_export/requirements.txt
```

You need **`neo4j/import/`** populated (e.g. via `scripts/build_neo4j_csvs.py`) so embeddings and features can join reviews to games. Reviewer-related features also expect the usual BGQ/BGG review files described in [`game_feature_export/README.md`](game_feature_export/README.md).

---

## Step 0 — Import CSVs (if you use Neo4j)

Only if you maintain the graph locally. See the **Knowledge graph** section in [`README.md`](README.md) (build CSVs → chunk → Docker → `run_all.sh`).

The **ML pipeline** below can run **without** a running Neo4j instance as long as **`neo4j/import/`** files exist on disk.

---

## Step 1 — Build the FAISS embedding index

Encodes game descriptions and review text, writes **`meta.json`**, **`id_map.parquet`**, **`index.faiss`**, **`vectors.npy`**, and **`shards/shard_*.parquet`** under your output directory.

```bash
python scripts/build_embeddings_faiss.py \
  --output embeddings/all-MiniLM-L6-v2-full \
  --model sentence-transformers/all-MiniLM-L6-v2 \
  --index flat
```

**Important for downstream steps:**

- **`--store-text`** — needed if you will run **review sentiment** (Step 3): shards must contain a **`text`** column. If you already built without it, rebuild with this flag.
- **`--resume`** — after a failed long run, reuse the **same** `--output` and flags; see [`embeddings/README.md`](embeddings/README.md).

---

## Step 2 — (Optional) Score every review for sentiment

Skip this if you only want **embedding + reviewer/value features** without sentiment aggregates.

Uses HF `sentiment-analysis`; on Apple Silicon the default device is often **`mps`** — use **`--device cpu`** if needed.

```bash
python -m game_feature_export.review_sentiment \
  --embedding-root embeddings/all-MiniLM-L6-v2-full \
  --sentiment-model cardiffnlp/twitter-roberta-base-sentiment-latest \
  --batch-size 32
```

Produces something like **`game_feature_export/review_sentiment/artifacts/<timestamp>/review_sentiment.parquet`**.  
Interrupted run: add **`--resume`** (same output path). Details: [`game_feature_export/README.md`](game_feature_export/README.md).

---

## Step 3 — Aggregate to one row per game (`features_per_game.parquet`)

Joins the embedding index, Neo4j import files, and (optionally) sentiment parquet into **`features_per_game.parquet`** + **`run_meta.json`** under your chosen **`--output-dir`**.

**Embedding-only (no sentiment columns):**

```bash
python -m game_feature_export \
  --embedding-root embeddings/all-MiniLM-L6-v2-full \
  --neo4j-import neo4j/import \
  --output-dir game_feature_export/artifacts/embed_only
```

**With sentiment aggregates** (requires Step 2):

```bash
python -m game_feature_export \
  --embedding-root embeddings/all-MiniLM-L6-v2-full \
  --neo4j-import neo4j/import \
  --include-sentiment-features \
  --sentiment-parquet game_feature_export/review_sentiment/artifacts/<UTC>/review_sentiment.parquet \
  --output-dir game_feature_export/artifacts/with_sentiment
```

Check **`run_meta.json`** for sensible row counts.

---

## Step 4 — (Optional) Standardize features for ML / neural nets

Fits **RobustScaler** or **StandardScaler** on **train rows only**, optionally **L2-normalizes** **`mean_embedding`**, and writes:

- **`features_standardized.parquet`** — all games transformed  
- **`preprocess.joblib`** — `PreprocessBundle` for **inference** (same transform as training)

**Recommended when you want BGQ-reviewed games for evaluation / test:** fit the scaler only on games that **do not** have a **`bgq_review`** row in **`id_map`** (those BGQ games never enter the FIT set). Use **`--train-fraction 1.0`** to use **all** non-BGQ rows for fitting (or **`< 1`** to hold out some non-BGQ rows for validation).

```bash
python scripts/preprocess_features_parquet.py \
  --input game_feature_export/artifacts/embed_only/features_per_game.parquet \
  --output game_feature_export/artifacts/embed_only/features_standardized.parquet \
  --train-fraction 1.0 \
  --seed 42 \
  --reserve-bgq-for-test \
  --embedding-root embeddings/all-MiniLM-L6-v2-full \
  --tabular-scaler robust \
  --embedding l2 \
  --nn-safe \
  --pipeline-out game_feature_export/artifacts/embed_only/preprocess.joblib
```

- **`--reserve-bgq-for-test`** — FIT pool = games **without** BGQ reviews (per **`id_map.parquet`**). Rows for games **with** BGQ reviews are still **written** to the output parquet with the **same** transform as everyone else (scaler was not fit on them — a deliberate train/test-type split for evaluation).
- **`--fit-bgq-review-games`** (mutually exclusive with **`--reserve-bgq-for-test`**) — opposite policy: FIT pool is **only** BGQ games.
- **`--nn-safe`** — tabular bounds from the FIT set: **99th percentile** of absolute scaled values, **capped at 4** (override cap with **`--tabular-clip-cap`**; override strategy with **`--tabular-clip`** or **`--tabular-clip-quantile`**). Also turns on **`--match-embedding-tabular-scale`** by default. Disable matching with **`--no-match-embedding-tabular-scale`**. Resolved clip and **`embedding_block_scale`** are stored in **`preprocess.joblib`**.
- Fixed FIT IDs: **`--train-bgg-ids path/to/ids.txt`** (intersected with the BGQ pool flag if set).

**Inference:** load `preprocess.joblib`, call **`bundle.transform(X_tab, X_emb)`** with columns in **`bundle.scalar_columns`** order. See **`preprocess_meta.json`** for **`embedding_block_scale`**, **`tabular_clip`**, and column list.

**Training split:** filter training batches to **`bgg_id ∉ bgq_set`** (or the complement) in your trainer; **`preprocess_meta.json`** reports BGQ vs non-BGQ row counts when **`--reserve-bgq-for-test`** is used.

---

## Quick sanity checklist

| After | Check |
|--------|--------|
| Step 1 | `meta.json`, `id_map.parquet`, shards present; sentiment path needs `--store-text` |
| Step 3 | `run_meta.json` — `joined_review_rows`, `rows_written` |
| Step 4 | `preprocess_meta.json` — `n_fit_rows`, `reserve_bgq_for_test`, `embedding_block_scale`, `tabular_clip` |

---

## End state (what you train on)

| Artifact | Role |
|----------|------|
| `features_per_game.parquet` | Raw per-game features (embedding list + scalars) |
| `features_standardized.parquet` | Scaled + optional L2 + clip + optional embedding scale |
| `preprocess.joblib` | Same transforms at inference (includes `embedding_block_scale` when enabled) |

For troubleshooting (resume, missing `text`, sentiment join issues), see the tables at the bottom of [`game_feature_export/README.md`](game_feature_export/README.md).
