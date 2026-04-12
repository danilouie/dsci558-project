#!/usr/bin/env bash
# Split bgg_reviews.tsv into small files and LOAD each in a separate cypher-shell invocation
# so transaction memory stays under dbms.memory.transaction.total.max without raising heap.
set -euo pipefail

CONTAINER="${1:-dsci558-neo4j}"
AUTH_USER="${NEO4J_USER:-neo4j}"
AUTH_PASS="${NEO4J_PASSWORD:-password}"
# Data lines per chunk file (not counting header). Override: BGG_REVIEW_CHUNK_LINES=250 ./09b_...
LINES="${BGG_REVIEW_CHUNK_LINES:-500}"

SRC="neo4j/import/bgg_reviews.tsv"
CHUNK_DIR="neo4j/import/bgg_review_chunks"

if [ ! -s "${SRC}" ]; then
  echo "Missing or empty ${SRC}"
  exit 1
fi

echo "==> Splitting ${SRC} into chunks of ${LINES} data lines -> ${CHUNK_DIR}/"
python3 scripts/chunk_bgg_reviews_tsv.py "${SRC}" "${CHUNK_DIR}" "${LINES}"

shopt -s nullglob
chunks=( "${CHUNK_DIR}"/bgg_reviews_*.tsv )
if [ "${#chunks[@]}" -eq 0 ]; then
  echo "No chunk files produced."
  exit 1
fi

for file in "${chunks[@]}"; do
  base="$(basename "$file")"
  echo "==> Loading BggReview chunk ${base}"
  docker exec -i "${CONTAINER}" cypher-shell -u "${AUTH_USER}" -p "${AUTH_PASS}" <<EOF
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///bgg_review_chunks/${base}' AS row FIELDTERMINATOR '\t'
  WITH row WHERE row.bgg_review_id IS NOT NULL AND trim(row.bgg_review_id) <> ''
  MERGE (b:BggReview {bgg_review_id: trim(row.bgg_review_id)})
  SET
    b.comment_text = row.comment_text,
    b.username = row.username,
    b.sources = row.sources,
    b.source_review_keys = row.source_review_keys,
    b.game_name_raw = row.game_name_raw,
    b.rating = CASE WHEN trim(coalesce(row.rating,'')) = '' THEN null ELSE toFloat(trim(row.rating)) END,
    b.page = CASE WHEN trim(coalesce(row.page,'')) = '' THEN null ELSE toInteger(trim(row.page)) END
} IN TRANSACTIONS OF 5 ROWS;
EOF
done

echo "==> All BggReview chunks loaded."
