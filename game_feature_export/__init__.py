"""Isolated per-game feature export (read embeddings; write parquet under game_feature_export/artifacts)."""

from game_feature_export.run import build_per_game_features

__all__ = ["build_per_game_features"]
