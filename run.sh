#!/usr/bin/env bash

set -euo pipefail

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

# echo "=== Step 1: Build embeddings (FAISS) ==="
# python3 scripts/build_embeddings_faiss.py \
#   --output embeddings/all-MiniLM-L6-v2-full \
#   --resume \
#   --model sentence-transformers/all-MiniLM-L6-v2 \
#   --index flat \
#   --batch-size 256 \
#   --device mps \
#   --store-text \
#   2>&1 | tee "$LOG_DIR/step1_embeddings.log"

echo "=== Step 2: Review sentiment ==="
python3 -m game_feature_export.review_sentiment \
  --embedding-root embeddings/all-MiniLM-L6-v2-full \
  --out game_feature_export/review_sentiment/artifacts/full_run_new/review_sentiment.parquet \
  --sentiment-model distilbert-base-uncased-finetuned-sst-2-english \
  --batch-size 64 \
  --device mps \
  --resume \
  2>&1 | tee "$LOG_DIR/step2_sentiment.log"

echo "=== Step 3: Feature export ==="
python3 -m game_feature_export \
  --embedding-root embeddings/all-MiniLM-L6-v2-full \
  --neo4j-import neo4j/import \
  --include-sentiment-features \
  --sentiment-parquet game_feature_export/review_sentiment/artifacts/full_run/review_sentiment.parquet \
  --output-dir game_feature_export/artifacts/full_run_v1 \
  --good-value-text "This game is excellent value for money." \
  --bad-value-text "This game is overpriced and not worth the price." \
  --concept-encoder-device mps \
  2>&1 | tee "$LOG_DIR/step3_export.log"

echo "=== All steps completed successfully ==="