## Neo4j schema (board game knowledge graph)

### Core identifiers
- **Game**: `bgg_id` (string) is the stable unique ID.
- **BGOKey**: `key` (string) is the stable unique ID.
- **PricePoint**: `price_point_id` (string) = `${bgo_key}::${date}` where `date` is `YYYY-MM-DD`.
- **Review**: `review_id` (string) = BGQ `url` (stable, unique).
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

#### `(:Review)`
- `review_id` (string, unique)
- `url` (string)
- `title` (string)
- `author` (string)
- `category` (string)
- `published_at` (datetime)
- `score` (float)
- `game_name_raw` (string)

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
- `(g:Game)-[:HAS_REVIEW]->(r:Review)`
- `(g:Game)-[:HAS_RANK]->(rk:Rank)`

### Constraints and indexes (Neo4j 5 syntax)
```cypher
CREATE CONSTRAINT game_bgg_id IF NOT EXISTS FOR (g:Game) REQUIRE g.bgg_id IS UNIQUE;
CREATE CONSTRAINT bgokey_key IF NOT EXISTS FOR (k:BGOKey) REQUIRE k.key IS UNIQUE;
CREATE CONSTRAINT pricepoint_id IF NOT EXISTS FOR (p:PricePoint) REQUIRE p.price_point_id IS UNIQUE;
CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE;
CREATE CONSTRAINT rank_id IF NOT EXISTS FOR (rk:Rank) REQUIRE rk.rank_id IS UNIQUE;

CREATE INDEX game_name IF NOT EXISTS FOR (g:Game) ON (g.name);
CREATE INDEX pricepoint_date IF NOT EXISTS FOR (p:PricePoint) ON (p.date);
CREATE INDEX review_published IF NOT EXISTS FOR (r:Review) ON (r.published_at);
```
