# Joining FAISS hits back to Neo4j

Embedding indexes built by `scripts/build_embeddings_faiss.py` align with [`SCHEMA.md`](SCHEMA.md). Each `id_map.parquet` row stores `doc_kind` and the same primary keys used in Neo4j constraints.

Do **not** join on Neo4j internal ids (`elementId`).

## Columns in `id_map.parquet`

| Column | Meaning |
|--------|---------|
| `faiss_id` | Row index into FAISS (0 … N−1); equals row order used at build time |
| `doc_kind` | `game_description`, `bgq_review`, or `bgg_review` |
| `review_id` | Set for BGQ articles — matches `(:Review.review_id)` |
| `bgg_review_id` | Set for BGG comments — matches `(:BggReview.bgg_review_id)` |
| `bgg_id` | Set for game descriptions (`(:Game.bgg_id)`); may be set for reviews when denormalized from `game_bgg_review_edges.csv` |
| `text_sha256` | SHA-256 of embedded text at build time |

## Cypher lookups per hit

After search returns `(doc_kind, review_id, bgg_review_id, bgg_id)`:

### Board Game Quest articles (`doc_kind = bgq_review`)

```cypher
MATCH (r:Review {review_id: $review_id})
RETURN r;
```

With game via graph edge:

```cypher
MATCH (g:Game)-[:HAS_REVIEW]->(r:Review {review_id: $review_id})
RETURN g, r;
```

### BGG user comments (`doc_kind = bgg_review`)

```cypher
MATCH (br:BggReview {bgg_review_id: $bgg_review_id})
RETURN br;
```

With game:

```cypher
MATCH (g:Game)-[:HAS_BGG_REVIEW]->(br:BggReview {bgg_review_id: $bgg_review_id})
RETURN g, br;
```

### Game description (`doc_kind = game_description`)

```cypher
MATCH (g:Game {bgg_id: $bgg_id})
RETURN g.name, g.description;
```

## Query CLI

```bash
python scripts/query_embeddings_faiss.py \
  --artifacts embeddings/your-model-dir \
  --query " cooperative dungeon crawler " \
  -k 10
```
