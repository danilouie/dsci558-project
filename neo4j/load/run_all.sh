#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${1:-dsci558-neo4j}"
AUTH_USER="${NEO4J_USER:-neo4j}"
AUTH_PASS="${NEO4J_PASSWORD:-password}"
START_STEP="${2:-1}"

run_file() {
  local file="$1"
  echo "==> Running ${file}"
  cat "${file}" | docker exec -i "${CONTAINER}" cypher-shell -u "${AUTH_USER}" -p "${AUTH_PASS}"
}

run_script() {
  local script="$1"
  echo "==> Running ${script}"
  NEO4J_USER="${AUTH_USER}" NEO4J_PASSWORD="${AUTH_PASS}" "./${script}" "${CONTAINER}"
}

ensure_chunks() {
  if [ ! -d "neo4j/import/chunks" ] || [ -z "$(ls -1 neo4j/import/chunks/price_points_*.csv 2>/dev/null)" ]; then
    echo "==> Creating CSV chunks for large files"
    python3 scripts/chunk_neo4j_csvs.py --rows 100000
  fi
  # If you still see transaction memory errors after changing --rows, remove chunks and re-run step 4:
  #   rm -rf neo4j/import/chunks
}

preflight() {
  local required=(
    "neo4j/import/games.csv"
    "neo4j/import/ranks.csv"
    "neo4j/import/reviews.csv"
    "neo4j/import/price_points.csv"
    "neo4j/import/game_review_edges.csv"
    "neo4j/import/users.csv"
    "neo4j/import/bgg_reviews.tsv"
    "neo4j/import/game_bgg_review_edges.csv"
    "neo4j/import/user_bgg_review_edges.csv"
    "neo4j/import/user_game_owns.csv"
    "neo4j/import/user_game_wants.csv"
    "neo4j/import/user_game_wants_to_buy.csv"
    "neo4j/import/user_game_wants_to_trade.csv"
  )
  for f in "${required[@]}"; do
    if [ ! -s "${f}" ]; then
      echo "Missing/empty required import file: ${f}"
      echo "Run: python3 scripts/build_neo4j_csvs.py"
      exit 1
    fi
  done
}

preflight

if [ "${START_STEP}" -le 1 ]; then run_file "neo4j/load/01_constraints.cypher"; fi
if [ "${START_STEP}" -le 2 ]; then run_file "neo4j/load/02_nodes_games.cypher"; fi
if [ "${START_STEP}" -le 3 ]; then run_file "neo4j/load/03_nodes_small.cypher"; fi
if [ "${START_STEP}" -le 4 ]; then ensure_chunks; run_script "neo4j/load/04_nodes_pricepoints_chunks.sh"; fi
if [ "${START_STEP}" -le 5 ]; then run_file "neo4j/load/05_rels_small.cypher"; fi
if [ "${START_STEP}" -le 6 ]; then
  run_file "neo4j/load/09_nodes_users.cypher"
  run_script "neo4j/load/09b_bgg_reviews_chunks.sh"
fi
if [ "${START_STEP}" -le 7 ]; then run_script "neo4j/load/10_rels_bgg_collection_chunks.sh"; fi
if [ "${START_STEP}" -le 8 ]; then run_file "neo4j/load/08_verify.cypher"; fi

echo "All load steps completed."
