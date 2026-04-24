# dsci558-project

Creating a KG for board games

## Knowledge graph (Neo4j)

### Data model (current)
- `Game` is the primary node.
- `PricePoint` nodes are linked directly to `Game` via `(:Game)-[:HAS_PRICE_POINT]->(:PricePoint)`.
- `Review` nodes are linked via `(:Game)-[:HAS_REVIEW]->(:Review)`.
- `BGOKey` nodes are not used.
- `Rank` nodes are not used; rank CSV fields are merged into `Game` properties.

### 1) Build Neo4j import CSVs
Creates CSVs under `neo4j/import/`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Full export** (every game in `games.jsonl` and related sources, subject to normal ETL rules):

```bash
python3 scripts/build_neo4j_csvs.py
```

**BGO–BGG overlap only** (games whose `bgg_id` appears in `bgo_key_bgg_map.tsv`):

```bash
python3 scripts/build_neo4j_csvs.py --overlap-only
```

**Ridge / price-stats universe** (only `bgg_id`s in `ridge_predictions_with_price_stats.csv`; merges `pred_avg_quality`, `mean_of_mean`, `max_of_max`, `min_of_min` into `games.csv`). Use this when rebuilding the KG to match that file; then clear Neo4j and run [`neo4j/load/run_all.sh`](neo4j/load/run_all.sh) (see step 4–5).

```bash
python3 scripts/build_neo4j_csvs.py --ridge-whitelist ridge_predictions_with_price_stats.csv
```

Paths for `--ridge-whitelist` are resolved from the project root if not absolute. See also [`neo4j/SCHEMA.md`](neo4j/SCHEMA.md).

The ETL includes these rules:
- skips `PricePoint` rows where all price fields are null (`min`, `mean`, `max`)
- keeps only `PricePoint`s that can map to a `Game` (`bgg_id`)
- stores full BGQ review fields (including long text like `body`, `gameplay_overview`, `game_experience`)

### 2) Chunk large price CSV for stable loading
Required for large imports:

```bash
python3 scripts/chunk_neo4j_csvs.py --rows 500000
```

### 3) Run Neo4j locally (Docker)

```bash
docker compose -f neo4j/docker-compose.yml up -d
```

Then open Neo4j Browser at `http://localhost:7474` and login:
- user: `neo4j`
- password: `password`

### 4) Clean rebuild (recommended)
For a true fresh build:

```bash
docker compose -f neo4j/docker-compose.yml down
rm -rf neo4j/data/*
docker compose -f neo4j/docker-compose.yml up -d
```

### 5) Load CSVs into Neo4j
Use the loader runner (includes chunked pricepoint import):

```bash
./neo4j/load/run_all.sh
```

**Reload after a filtered export** (e.g. `--ridge-whitelist`): if the graph already contains nodes from a previous import, **empty the graph** before loading so the KG matches the new CSVs only:

```bash
echo 'MATCH (n) DETACH DELETE n;' | docker exec -i dsci558-neo4j cypher-shell -u neo4j -p password
./neo4j/load/run_all.sh
```

Alternatively, a **full data reset** (new empty store) is step 4) followed by `run_all.sh` as above.

If load fails mid-way, restart and resume from a step:

```bash
docker restart dsci558-neo4j
./neo4j/load/run_all.sh dsci558-neo4j 4
```

### 6) Verify graph counts/links

```bash
cat neo4j/load/08_verify.cypher | docker exec -i dsci558-neo4j cypher-shell -u neo4j -p password
```

### 7) Text embeddings (FAISS + Parquet)

**End-to-end ML steps** (embeddings → per-game features → optional standardization): see [`PIPELINE_README.md`](PIPELINE_README.md).

**Worth / value modeling** (demand head from standardized features, BGO price features, weak BGQ label combiner—flowchart + suggested architectures): see [`docs/WORTH_MODEL.md`](docs/WORTH_MODEL.md).

Embeds `games.csv` descriptions, BGQ `reviews.csv`, and BGG `bgg_reviews*.tsv` from `neo4j/import/` using keys aligned with [`neo4j/SCHEMA.md`](neo4j/SCHEMA.md). Outputs `meta.json`, `index.faiss`, `id_map.parquet`, `vectors.npy`, and `shards/*.parquet` under the chosen output directory.

**Full CLI reference (every flag, inputs, outputs, examples):** [`embeddings/README.md`](embeddings/README.md).

Quick start:

```bash
python scripts/build_embeddings_faiss.py --output embeddings/bge-small-en-v1.5 --index flat
python scripts/query_embeddings_faiss.py --artifacts embeddings/bge-small-en-v1.5 --query "cooperative dungeon crawler" -k 10
```

Join FAISS hits back to Neo4j with [`neo4j/EMBEDDINGS_FAISS_JOINS.md`](neo4j/EMBEDDINGS_FAISS_JOINS.md).

For quick smoke tests without scanning chunked `bgg_reviews` files: `--limit N --skip-bgg-reviews` (see [`embeddings/README.md`](embeddings/README.md)).

To **resume** a long build after a crash, use `--resume` with the same flags and ensure `vectors.npy` exists (omit `--no-vectors-npy`); details in [`embeddings/README.md`](embeddings/README.md).

## Files
*Each webscraping component is currently in their own branch.*
- `bgg_game_scraper.py`: scrapes game info from BoardGameGeek using XML and API token $\rightarrow$ outputs in `games.jsonl`

- `bgg_forums_scraper.py`: scrapes forum threads and articles from BoardGameGeek using XML and API $\rightarrow$ outputs in 5 files separated by forum category
  - `recommendations.jsonl`
  - `gaming-with-kids.jsonl`
  - `games-in-the-classroom.jsonl`
  - `hot-deals.jsonl`
  - `trades.jsonl`

- `bgo_download.py`: scrapes price history from BoardGameOracle $\rightarrow$ outputs in `bgo.zip`

- `bgq_scrapper.py`: scrapes reviews from BoardGameQuest $\rightarrow$ outputs in `reviews.jsonl`
    


