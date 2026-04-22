## Neo4j schema (board game knowledge graph)

### Overlap-only export

The CSV pipeline ([`scripts/build_neo4j_csvs.py`](../scripts/build_neo4j_csvs.py)) accepts **`--overlap-only`**. When set, only games whose **`bgg_id`** appears in **`bgo_key_bgg_map.tsv`** (rows with both a non-empty Oracle `key` and `bgg_id`) are written to `games.csv`, `ranks.csv`, and related edges; `bgo_keys.csv` is restricted to that overlap (`export_mapping` with `price_histories/key_name.tsv` keeps only keys present in the mapping TSV and drops rows whose resolved `bgg_id` is outside the overlap). BGQ **`reviews`**, BGG **`bgg_reviews`** / edges, and user–game **`OWNS` / `WANTS*`** rows are emitted only when that **`bgg_id`** is both in the overlap set and present in **`games.jsonl`** (`valid_bgg_ids`). With **`--overlap-only`**, **`users.csv`** is limited to usernames that appear on at least one of those filtered collection or BGG-review edges (owners with no remaining rows after filtering are omitted). Full exports without the flag still list every collection-file owner, as before. Games listed in the mapping but missing from `games.jsonl` export as no `(:Game)` row until the JSONL includes them.

### Core identifiers
- **Game**: `bgg_id` (string) is the stable unique ID.
- **BGOKey**: `key` (string) is the stable unique ID.
- **PricePoint**: `price_point_id` (string) = `${bgg_id}::${date}` where `date` is `YYYY-MM-DD` (see `export_price_points` in `kg_etl/export_csvs.py`).
- **Review**: `review_id` (string) = BGQ article `url` (stable, unique). **Not** BGG user comments.
- **BggReview**: `bgg_review_id` (string) = `bggrev:` + SHA-256(`bgg_id`, BGG `username`, comment text after outer trim). Deduplicates `game_review_batches/**/page_*.jsonl` and public `comment` on each `user/*_collection.jsonl` (owner = filename prefix before `_collection.jsonl`) when the triple `(bgg_id, username, exact comment)` matches.
- **User**: `username` (string) = BGG username (collection owner + comment authors).
- **Rank**: `rank_id` (string) = `${bgg_id}` (one snapshot from `boardgames_ranks.csv`).

### Labels and properties

#### `(:Game)`
- `bgg_id` (string, unique)
- `name` (string)
- `year` (int)
- `rank` (int)
- `geek_rating` (float)
- `avg_rating` (float)
- `num_voters` (int)
- `is_expansion` (boolean)
- `description` (string)
- `min_players` (int), `max_players` (int)
- `best_min_players` (int), `best_max_players` (int)
- `min_playtime` (int), `max_playtime` (int)
- `min_age` (int)
- `complexity` (float)
- `categories` (list<string>) (imported from pipe-delimited string)
- `mechanisms` (list<string>) (imported from pipe-delimited string)
- Optional rank breakdowns: `abstracts_rank`, `strategygames_rank`, etc.

#### `(:BGOKey)`
- `key` (string, unique)
- `slug` (string)
- `title` (string)
- `detail_url` (string)

#### `(:PricePoint)`
- `price_point_id` (string, unique)
- `bgo_key` (string)
- `bgg_id` (string, optional)
- `date` (date)
- `min_price` (float)
- `mean_price` (float)
- `max_price` (float)
- `source` (string, default `"BGO"`)

#### `(:Review)` (Board Game Quest only)
- `review_id` (string, unique)
- `url` (string)
- `title` (string)
- `author` (string)
- `category` (string)
- `published_at` (datetime)
- `score` (float)
- `game_name_raw` (string)

#### `(:BggReview)` (BGG user comments / ratings)
- `bgg_review_id` (string, unique)
- `comment_text` (string)
- `rating` (float, optional)
- `username` (string, BGG user)
- `sources` (string, pipe-delimited, e.g. `collection|game_review_batches`)
- `source_review_keys` (string, pipe-delimited batch `review_key` values)
- `page` (int, optional; minimum page seen in batches)
- `game_name_raw` (string)

#### `(:User)`
- `username` (string, unique)

#### `(:Rank)`
- `rank_id` (string, unique)
- `rank_value` (int)
- `bayesaverage` (float)
- `average` (float)
- `usersrated` (int)
- `is_expansion` (boolean)
- Optional rank breakdowns: `abstracts_rank`, `strategygames_rank`, etc.

### Relationships
- `(b:BGOKey)-[:MAPS_TO]->(g:Game)`
- `(b:BGOKey)-[:HAS_PRICE_POINT]->(p:PricePoint)`
- `(g:Game)-[:HAS_PRICE_POINT]->(p:PricePoint)`
- `(g:Game)-[:HAS_REVIEW]->(r:Review)` — BGQ articles only
- `(g:Game)-[:HAS_BGG_REVIEW]->(br:BggReview)`
- `(u:User)-[:WROTE]->(br:BggReview)`
- `(u:User)-[:OWNS]->(g:Game)` — optional rel props: `collid`, `num_plays`, `last_modified`, `name`
- `(u:User)-[:WANTS]->(g:Game)` — same optional props when this rel carries row metadata (see ETL note below)
- `(u:User)-[:WANTS_TO_BUY]->(g:Game)`
- `(u:User)-[:WANTS_TO_TRADE]->(g:Game)`
- `(g:Game)-[:HAS_RANK]->(rk:Rank)`

**Collection row metadata on relationships:** When `own` is true, `collid`, `num_plays`, `last_modified`, and `name` are stored on `OWNS`. If the row is not owned, the same fields are stored on the first matching rel in priority order `WANTS` → `WANTS_TO_BUY` → `WANTS_TO_TRADE`; other rels for that row get empty metadata. Collection CSV rows are produced from every `user/*_collection.jsonl` file (unless export is limited via CLI).

### Constraints and indexes (Neo4j 5 syntax)
```cypher
CREATE CONSTRAINT game_bgg_id IF NOT EXISTS FOR (g:Game) REQUIRE g.bgg_id IS UNIQUE;
CREATE CONSTRAINT bgokey_key IF NOT EXISTS FOR (k:BGOKey) REQUIRE k.key IS UNIQUE;
CREATE CONSTRAINT pricepoint_id IF NOT EXISTS FOR (p:PricePoint) REQUIRE p.price_point_id IS UNIQUE;
CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE;
CREATE CONSTRAINT rank_id IF NOT EXISTS FOR (rk:Rank) REQUIRE rk.rank_id IS UNIQUE;
CREATE CONSTRAINT user_username IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE;
CREATE CONSTRAINT bggreview_id IF NOT EXISTS FOR (b:BggReview) REQUIRE b.bgg_review_id IS UNIQUE;

CREATE INDEX game_name IF NOT EXISTS FOR (g:Game) ON (g.name);
CREATE INDEX pricepoint_date IF NOT EXISTS FOR (p:PricePoint) ON (p.date);
CREATE INDEX review_published IF NOT EXISTS FOR (r:Review) ON (r.published_at);
CREATE INDEX bggreview_username IF NOT EXISTS FOR (b:BggReview) ON (b.username);
```
