#!/usr/bin/env bash
# Chunk large edge CSVs + one cypher-shell per chunk to avoid Java heap OOM on multi-million-row LOAD CSV.
#
# Cypher shape: LOAD CSV inside CALL { ... } IN TRANSACTIONS (same as 07_rels_game_price_chunks.sh).
# Nesting lets each batch stream a bounded number of rows; outer LOAD CSV + batched CALL can hit
# ForsetiClient "transaction terminated / no more locks" on large chunks under Neo4j 5.x.
#
# Env (optional):
#   BGG_EDGE_CHUNK_LINES  — data lines per chunk file (default 2500).
#   BGG_REL_TXN_BATCH     — IN TRANSACTIONS OF n ROWS (default 50). Lower if heap pressure.
#
# If imports still abort: raise NEO4J_db_transaction_timeout / heap in neo4j/docker-compose.yml and recreate the container.
# and recreate the container.
set -euo pipefail

CONTAINER="${1:-dsci558-neo4j}"
AUTH_USER="${NEO4J_USER:-neo4j}"
AUTH_PASS="${NEO4J_PASSWORD:-password}"
LINES="${BGG_EDGE_CHUNK_LINES:-2500}"
TXN="${BGG_REL_TXN_BATCH:-100}"

CHUNK_DIR="neo4j/import/bgg_rel_chunks"
mkdir -p "${CHUNK_DIR}"

run_game_bgg_edges() {
  echo "==> Chunking game_bgg_review_edges.csv (${LINES} lines/file)"
  python3 scripts/chunk_csv_lines.py neo4j/import/game_bgg_review_edges.csv "${CHUNK_DIR}" game_bgg_edges_ "${LINES}"
  shopt -s nullglob
  for file in "${CHUNK_DIR}"/game_bgg_edges_*.csv; do
    base="$(basename "$file")"
    echo "==> HAS_BGG_REVIEW ${base}"
    docker exec -i "${CONTAINER}" cypher-shell -u "${AUTH_USER}" -p "${AUTH_PASS}" <<EOF
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///bgg_rel_chunks/${base}' AS row
  MATCH (g:Game {bgg_id: trim(row.bgg_id)})
  MATCH (b:BggReview {bgg_review_id: trim(row.bgg_review_id)})
  MERGE (g)-[:HAS_BGG_REVIEW]->(b)
} IN TRANSACTIONS OF ${TXN} ROWS;
EOF
  done
}

run_user_bgg_edges() {
  echo "==> Chunking user_bgg_review_edges.csv (${LINES} lines/file)"
  python3 scripts/chunk_csv_lines.py neo4j/import/user_bgg_review_edges.csv "${CHUNK_DIR}" user_bgg_edges_ "${LINES}"
  shopt -s nullglob
  for file in "${CHUNK_DIR}"/user_bgg_edges_*.csv; do
    base="$(basename "$file")"
    echo "==> WROTE ${base}"
    docker exec -i "${CONTAINER}" cypher-shell -u "${AUTH_USER}" -p "${AUTH_PASS}" <<EOF
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///bgg_rel_chunks/${base}' AS row
  MATCH (u:User {username: trim(row.username)})
  MATCH (b:BggReview {bgg_review_id: trim(row.bgg_review_id)})
  MERGE (u)-[:WROTE]->(b)
} IN TRANSACTIONS OF ${TXN} ROWS;
EOF
  done
}

run_user_game_file() {
  local src_name="$1"
  local prefix="$2"
  local rel="$3"
  local src="neo4j/import/${src_name}"
  if [ ! -s "${src}" ]; then
    echo "==> Skip empty ${src_name}"
    return 0
  fi
  echo "==> Chunking ${src_name}"
  python3 scripts/chunk_csv_lines.py "${src}" "${CHUNK_DIR}" "${prefix}" "${LINES}"
  shopt -s nullglob
  for file in "${CHUNK_DIR}"/"${prefix}"*.csv; do
    base="$(basename "$file")"
    echo "==> ${rel} ${base}"
    docker exec -i "${CONTAINER}" cypher-shell -u "${AUTH_USER}" -p "${AUTH_PASS}" <<EOF
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///bgg_rel_chunks/${base}' AS row
  MATCH (u:User {username: trim(row.owner_username)})
  MATCH (g:Game {bgg_id: trim(row.bgg_id)})
  MERGE (u)-[r:${rel}]->(g)
  SET
    r.collid = CASE WHEN trim(coalesce(row.collid,'')) = '' THEN null ELSE trim(row.collid) END,
    r.num_plays = CASE WHEN trim(coalesce(row.num_plays,'')) = '' THEN null ELSE toInteger(trim(row.num_plays)) END,
    r.last_modified = CASE WHEN trim(coalesce(row.last_modified,'')) = '' THEN null ELSE trim(row.last_modified) END,
    r.name = CASE WHEN trim(coalesce(row.name,'')) = '' THEN null ELSE trim(row.name) END
} IN TRANSACTIONS OF ${TXN} ROWS;
EOF
  done
}

run_game_bgg_edges
run_user_bgg_edges
run_user_game_file "user_game_owns.csv" "user_game_owns_" "OWNS"
run_user_game_file "user_game_wants.csv" "user_game_wants_" "WANTS"
run_user_game_file "user_game_wants_to_buy.csv" "user_game_wants_to_buy_" "WANTS_TO_BUY"
run_user_game_file "user_game_wants_to_trade.csv" "user_game_wants_to_trade_" "WANTS_TO_TRADE"

echo "==> BGG collection relationship chunks loaded."
