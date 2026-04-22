# FAISS embeddings

Build and query a local embedding index from the same CSV/TSV files used for Neo4j import. Primary keys match [`neo4j/SCHEMA.md`](../neo4j/SCHEMA.md). Join retrieval hits back to the graph using [`neo4j/EMBEDDINGS_FAISS_JOINS.md`](../neo4j/EMBEDDINGS_FAISS_JOINS.md).

## Prerequisites

From the repository root:

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Scripts assume the repo root is on `PYTHONPATH` (the wrappers under `scripts/` add it automatically).

## Input files (`neo4j/import/`)

| File | Purpose |
|------|---------|
| `games.csv` | `game_description` rows (`bgg_id`, `description`) |
| `reviews.csv` | BGQ `bgq_review` rows (`review_id`, text fields) |
| `bgg_reviews.tsv` **or** `**/bgg_reviews_*.tsv` | BGG `bgg_review` rows (`bgg_review_id`, `comment_text`) |
| `game_bgg_review_edges.csv` (optional) | Denormalizes `bgg_id` onto BGG review rows in `id_map` |

## Output directory layout

Written to the path passed as `--output`:

| Artifact | Description |
|----------|-------------|
| `meta.json` | Model name, embedding dimension, normalization flag, FAISS index type, vector count, timestamps |
| `index.faiss` | FAISS index (`IndexFlatIP` or `IndexHNSWFlat`) |
| `id_map.parquet` | One row per vector: `faiss_id`, `doc_kind`, join keys, `text_sha256` |
| `vectors.npy` | Float32 matrix `(N, dim)` in row order (omitted if `--no-vectors-npy`) |
| `build_manifest.parquet` | Same columns as `id_map` (written before encoding); used to validate `--resume` |
| `checkpoint.json` | Updated after each batch while encoding; lists `completed_rows`; **deleted** when the build finishes successfully |
| `shards/shard_*.parquet` | Metadata per shard; optional full `text` if `--store-text`; written **after** encoding completes |

---

## Build: `scripts/build_embeddings_faiss.py`

Runs the embedding model over collected documents, writes shards, stacks vectors, builds FAISS, then writes `id_map.parquet`.

### Examples

Full index (all three document types; long runtime if BGG chunks are huge):

```bash
python scripts/build_embeddings_faiss.py \
  --output embeddings/bge-small-en-v1.5 \
  --model BAAI/bge-small-en-v1.5 \
  --index flat \
  --batch-size 64
```

Fast smoke test (first N rows in global sort order; skips scanning all BGG chunk files):

```bash
python scripts/build_embeddings_faiss.py \
  --output embeddings/smoke \
  --model sentence-transformers/all-MiniLM-L6-v2 \
  --limit 50 \
  --skip-bgg-reviews \
  --no-vectors-npy
```

Approximate search index (larger RAM / faster queries at scale):

```bash
python scripts/build_embeddings_faiss.py \
  --output embeddings/hnsw \
  --index hnsw \
  --hnsw-m 32
```

### Progress and logging

The build runs in phases. Watch **stdout** (and stderr for Hugging Face / PyTorch):

1. **Collecting documents** — Can sit with no percentage for a long time if `--skip-bgg-reviews` is *not* set and you have many `bgg_reviews_*.tsv` chunks (sorting millions of rows). Then you should see `Collected N documents.`
2. **Loading embedding model** — First run may download weights from Hugging Face (your terminal shows download progress). Later runs use the cache.
3. **Encoding** — A **tqdm** bar counts **documents** embedded (`Encoding … doc`). That is your main throughput indicator.
4. **Building FAISS index** — Short line `Building FAISS index (flat|hnsw)...` then disk write.
5. **Writing** — `Writing shards, id_map.parquet, meta.json...` then the script prints total vectors.

Disable the tqdm bar only: `--no-progress`. Silence phase messages too: `--quiet` (implies no tqdm).

Rough disk progress: `vectors.npy` grows during encoding (via memory-mapped writes); `checkpoint.json` updates after each batch; `index.faiss`, `id_map.parquet`, `meta.json`, and `shards/` appear only **after** encoding finishes.

### Resume after an interruption

Resume **only** works when the first run wrote **`vectors.npy`** (do **not** use `--no-vectors-npy` if you want resume). The run must leave:

- `build_manifest.parquet`
- `checkpoint.json`
- `vectors.npy` (partially filled)

Run again with the **same** `--output`, **`--resume`**, and **matching** arguments as the original job (`--model`, `--neo4j-import`, `--limit`, all `--skip-*`, `--batch-size`, `--index`, `--hnsw-m`, `--shard-rows`, `--store-text`). If import data or flags changed, resume fails with a fingerprint mismatch—use a new `--output` or delete the partial directory.

Example:

```bash
python scripts/build_embeddings_faiss.py \
  --output embeddings/my-run \
  --resume \
  --model BAAI/bge-small-en-v1.5 \
  --index flat
```

If encoding finished but the process crashed during **FAISS / shard / id_map** writing, `--resume` skips the encode loop (`completed_rows` already equals `N`) and rebuilds those artifacts.

### Apple Silicon (MPS)

- **Queries** (`query_embeddings_faiss.py`): pass `--device mps` so the **encoder** runs on the GPU. FAISS search still runs on **CPU** (`faiss-cpu`), which is normal.
- **Building** (`build_embeddings_faiss.py`): pass `--device mps` to encode batches on MPS. Same requirement for **`--resume`**: use the same `--device` as the original run (stored in `checkpoint.json` as `encoder_device`). If a layer fails on MPS, fall back to `--device cpu`.

Stored vectors and `index.faiss` are device-agnostic; only the PyTorch model runtime uses MPS.

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--output` | *(required)* | Output directory for all artifacts (created if missing). |
| `--neo4j-import` | `<repo>/neo4j/import` | Directory containing `games.csv`, `reviews.csv`, and BGG review TSVs. |
| `--model` | `BAAI/bge-small-en-v1.5` | Hugging Face / sentence-transformers model id used for encoding. |
| `--device` | `cpu` | PyTorch device for encoding: `cpu`, `mps` (Apple GPU), `cuda`, … |
| `--batch-size` | `64` | Encoder batch size (tune for RAM vs throughput). |
| `--index` | `flat` | `flat`: `IndexFlatIP` on L2-normalized vectors (exact cosine similarity). `hnsw`: `IndexHNSWFlat` for faster approximate search. |
| `--hnsw-m` | `32` | HNSW connectivity parameter (only used when `--index hnsw`). |
| `--shard-rows` | `50000` | Maximum rows per `shards/shard_XXXXX.parquet` file. |
| `--store-text` | off | If set, include a `text` column in shard Parquet files (much larger on disk). |
| `--no-vectors-npy` | off | If set, do not write `vectors.npy` (only `index.faiss`). Saves disk; **cannot** be combined with `--resume`. |
| `--limit` | *(none)* | Embed only the first N documents after deterministic global sort (`game_description`, then BGQ, then BGG). Use with `--skip-bgg-reviews` for quick tests. |
| `--skip-games` | off | Exclude `games.csv` descriptions. |
| `--skip-bgq` | off | Exclude BGQ `reviews.csv`. |
| `--skip-bgg-reviews` | off | Exclude all BGG review TSVs (avoids scanning large chunk directories). |
| `--no-progress` | off | Hide the tqdm encoding bar (phase messages still print unless `--quiet`). |
| `--quiet` | off | Minimal output; no tqdm bar and no phase print lines. |
| `--resume` | off | Continue from `checkpoint.json` + `vectors.npy`; same flags as the original run. Requires `vectors.npy` on disk (omit `--no-vectors-npy`). |

You cannot pass all three `--skip-*` flags at once; at least one source must remain enabled.

---

## Query: `scripts/query_embeddings_faiss.py`

Loads `meta.json`, downloads the same `--model` from disk cache, loads `index.faiss` and `id_map.parquet`, encodes the query text, and prints top hits.

### Examples

```bash
python scripts/query_embeddings_faiss.py \
  --artifacts embeddings/bge-small-en-v1.5 \
  --query "cooperative dungeon crawler with dice combat" \
  -k 10
```

Use Apple Silicon GPU for encoding (if PyTorch MPS works with your model):

```bash
python scripts/query_embeddings_faiss.py \
  --artifacts embeddings/bge-small-en-v1.5 \
  --query "worker placement euro" \
  -k 5 \
  --device mps
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--artifacts` | *(required)* | Directory that contains `meta.json`, `index.faiss`, and `id_map.parquet`. |
| `--query` | *(required)* | Natural-language query string. |
| `-k` | `10` | Number of nearest neighbors to return. |
| `--device` | `cpu` | Device passed to sentence-transformers (`cpu`, `cuda`, `mps`, etc.). |

---

## Python API

```python
from pathlib import Path
from embeddings.search import FaissNeo4jIndex

idx = FaissNeo4jIndex(Path("embeddings/bge-small-en-v1.5"), device="cpu")
hits = idx.search("abstract strategy short playtime", k=10)
for h in hits:
    print(h.score, h.doc_kind, h.review_id, h.bgg_review_id, h.bgg_id)
```

See [`embeddings/layout.py`](layout.py) for `ArtifactPaths` / `EmbeddingMeta` paths.
