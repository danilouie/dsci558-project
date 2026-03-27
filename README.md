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

python3 scripts/build_neo4j_csvs.py
```

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

If load fails mid-way, restart and resume from a step:

```bash
docker restart dsci558-neo4j
./neo4j/load/run_all.sh dsci558-neo4j 4
```

### 6) Verify graph counts/links

```bash
cat neo4j/load/08_verify.cypher | docker exec -i dsci558-neo4j cypher-shell -u neo4j -p password
```

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
    


