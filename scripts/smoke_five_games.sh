#!/usr/bin/env bash
# End-to-end smoke: embed 5 games (with --store-text), sentiment, per-game features.
# From repo root:  bash scripts/smoke_five_games.sh
# Override:  OUT=... IDS=... bash scripts/smoke_five_games.sh
# Faster embed (skips BGG user comments; still has descriptions + BGQ for those games):
#   SKIP_BGG_REVIEWS=1 bash scripts/smoke_five_games.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Default: top-5 BGG rank games from a typical boardgames_ranks import (edit freely)
IDS="${IDS:-224517,342942,161936,174430,397598}"
OUT="${OUT:-embeddings/smoke-5games-MiniLM}"

EXTRA_FLAGS=()
if [[ "${SKIP_BGG_REVIEWS:-}" == "1" ]]; then
  EXTRA_FLAGS+=(--skip-bgg-reviews)
  echo "(Using --skip-bgg-reviews for a faster smoke run)"
fi

echo "==> Building FAISS + shards (only bgg_id: ${IDS})"
# "${arr[@]}" on an empty array trips `set -u` on older bash (macOS); this form is safe.
python3 scripts/build_embeddings_faiss.py \
  ${EXTRA_FLAGS[@]+"${EXTRA_FLAGS[@]}"} \
  --output "$OUT" \
  --model sentence-transformers/all-MiniLM-L6-v2 \
  --index flat \
  --batch-size 256 \
  --device mps \
  --store-text \
  --only-bgg-ids "$IDS"

SENT_OUT="${SENT_OUT:-game_feature_export/review_sentiment/artifacts/smoke_5games/review_sentiment.parquet}"
mkdir -p "$(dirname "$SENT_OUT")"

echo "==> Review sentiment"
python3 -m game_feature_export.review_sentiment \
  --embedding-root "$OUT" \
  --out "$SENT_OUT" \
  --batch-size 32 \
  --device mps

FEAT_OUT="${FEAT_OUT:-game_feature_export/artifacts/smoke_5games}"
echo "==> Per-game features -> $FEAT_OUT"
python3 -m game_feature_export \
  --embedding-root "$OUT" \
  --neo4j-import neo4j/import \
  --include-sentiment-features \
  --sentiment-parquet "$SENT_OUT" \
  --output-dir "$FEAT_OUT" \
  --good-value-text "This game is excellent value for money." \
  --bad-value-text "This game is overpriced and not worth the price."

echo "Done: $FEAT_OUT/features_per_game.parquet"
