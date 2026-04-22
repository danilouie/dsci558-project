# Game description FAISS indices by BGG category

This folder builds FAISS indices from **only** `Game.description` in `games.csv`, keyed by **`bgg_id`** in `id_map.parquet` (same as `(:Game {bgg_id})` in Neo4j):

- **Per BGG category** — one index under `cat/<slug>/`.
- **Uncategorized** — games with an **empty** `categories` field go to `cat/uncategorized/` by default.
- **All games** — one index at `all_games/` over every game with a non-empty description (not split by category).

## Prerequisites

- `games.csv` under `--neo4j-import` (usually `neo4j/import`), produced by your ETL with pipe-delimited `categories` ([kg_etl/export_csvs.py](../kg_etl/export_csvs.py)).
- Python deps from repo root [`requirements.txt`](../requirements.txt) (`faiss-cpu`, `sentence-transformers`, …).

## Build all categories

From the repo root:

```bash
python scripts/build_game_description_faiss_by_category.py \
  --output artifacts/game_description_by_category/
```

Equivalent:

```bash
python -m game_description_faiss.build --output artifacts/game_description_by_category/
```

Common options:

| Flag | Meaning |
|------|---------|
| `--neo4j-import DIR` | Directory containing `games.csv` (default: `neo4j/import`). |
| `--model MODEL` | Sentence-transformers id (default: `BAAI/bge-small-en-v1.5`; match your global index if you compare scores). |
| `--index flat\|hnsw` | FAISS index type ([embeddings/layout.py](../embeddings/layout.py)). |
| `--min-games N` | Skip **per-category** buckets with fewer than `N` games (does not apply to `all_games/`). |
| `--skip-uncategorized` | Do **not** create `cat/uncategorized/`. Default is to include games with empty `categories` there. |
| `--skip-all-games-index` | Do **not** write `all_games/` (single index over every described game). |
| `--categories A,B,C` | Build **only** these category labels (exact match); `all_games/` is still built unless `--skip-all-games-index`. |
| `--force` | Delete each `cat/<slug>/` and `all_games/` before rebuilding. Use when a prior run left `vectors.npy` (otherwise `build_faiss_index` refuses to overwrite). |

The encoder weights are loaded **once** per CLI invocation and reused for every category.

Output layout:

- `registry.json` — `categories` maps name → `{ slug, path, num_vectors, canonical_label }`; `all_games_index` points at `all_games/` (or `null` if skipped).
- `cat/<slug>/` — per-category artifacts (same contract as [`embeddings/`](../embeddings/): `meta.json`, `index.faiss`, `id_map.parquet`, optional `shards/`, `vectors.npy` unless `--no-vectors-npy`).
- `all_games/` — same artifact contract; query **all** games by description without picking a category.

## Query flow (Neo4j join)

1. Pick a category or **all games**: read `registry.json`, resolve `path` (e.g. `cat/strategy_game` or `all_games`).
2. Load the encoder + FAISS bundle with **`embeddings.search.FaissNeo4jIndex`** pointing `--artifacts` at that directory:

   ```bash
   python scripts/query_embeddings_faiss.py \
     --artifacts artifacts/game_description_by_category/cat/strategy_game \
     --query "worker placement euro" \
     --k 10
   ```

3. Each hit includes **`bgg_id`**. Hydrate nodes in Neo4j:

   ```cypher
   MATCH (g:Game)
   WHERE g.bgg_id IN $ids
   RETURN g
   ```

   Order results in application code to match FAISS score order (`$ids` preserves rank if you pass the list in hit order).

## Embedding pipeline detail

[`game_description_faiss/categories.py`](categories.py) splits `games.csv` rows by the pipe-delimited `categories` column. Games with multiple categories get the **same description vector** indexed in **each** relevant category index. The **`all_games/`** index uses [`iter_games_descriptions`](../embeddings/documents.py) so every game with a non-empty description appears **once**, regardless of category flags.

[`embeddings.pipeline.build_faiss_index`](../embeddings/pipeline.py) accepts an explicit document list (`documents=`) and an optional pre-constructed **`encoder_model`** so we reuse encoding, checkpoints, and `id_map.parquet` without temporary CSV files. Explicit lists do **not** support `--resume` on the underlying builder (each category directory is rebuilt from scratch). Pass **`--force`** when you need to rebuild into an existing output tree.

## Frontend and backend integration

Choose one deployment shape:

**Contract-first (repos stay separate)**  
Publish a small REST API (your backend) that accepts `category_slug` / natural language `query` / `k`, loads `registry.json`, opens `cat/<slug>/index.faiss`, runs [`FaissNeo4jIndex.search`](../embeddings/search.py), returns `{ bgg_id, score }[]`. The SPA calls `fetch(API_BASE + '/similar-games')`. Document request/response in OpenAPI.

**Monorepo**  
Place `frontend/` (Vite, Next.js, …) beside `backend/` at the repo root; `docker-compose.yml` wires API + Neo4j + static assets. Frontend reads `VITE_API_URL` / `NEXT_PUBLIC_API_URL`; backend enables CORS for that origin.

**Thin API inside this repo**  
Only if the service is minimal: add e.g. `api/` FastAPI later that reads `artifacts/.../registry.json` and loads indexes lazily by slug — keep embeddings on disk paths configured via env (`GAME_FAISS_ROOT`). Larger UIs usually remain a separate package.

Checklist:

- **`bgg_id`** type is consistent end-to-end (string).
- Artifact paths differ between dev machines and production — use env vars.
- Match **embedding model** name in API meta with the model used at build time (`meta.json`).
