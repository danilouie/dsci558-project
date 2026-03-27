#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${1:-dsci558-neo4j}"
AUTH_USER="${NEO4J_USER:-neo4j}"
AUTH_PASS="${NEO4J_PASSWORD:-password}"
CHUNKS_DIR="${2:-neo4j/import/chunks}"

for file in "${CHUNKS_DIR}"/game_price_edges_*.csv; do
  [ -e "$file" ] || { echo "No game_price_edges chunk files found in ${CHUNKS_DIR}"; exit 1; }
  base="$(basename "$file")"
  echo "==> Loading ${base}"
  docker exec -i "${CONTAINER}" cypher-shell -u "${AUTH_USER}" -p "${AUTH_PASS}" <<EOF
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///chunks/${base}' AS row
  MATCH (g:Game {bgg_id: row.bgg_id})
  MATCH (p:PricePoint {price_point_id: row.price_point_id})
  MERGE (g)-[:HAS_PRICE_POINT]->(p)
} IN TRANSACTIONS OF 100 ROWS;
EOF
done

echo "Game->PricePoint relationship chunks loaded."

