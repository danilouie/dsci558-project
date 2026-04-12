from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    root: Path

    @property
    def games_jsonl(self) -> Path:
        return self.root / "games.jsonl"

    @property
    def bgq_reviews_jsonl(self) -> Path:
        return self.root / "bgq_reviews.jsonl"

    @property
    def ranks_csv(self) -> Path:
        return self.root / "boardgames_ranks.csv"

    @property
    def bgo_map_tsv(self) -> Path:
        return self.root / "bgo_key_bgg_map.tsv"

    @property
    def price_histories_dir(self) -> Path:
        return self.root / "price_histories"

    @property
    def price_key_name_tsv(self) -> Path:
        return self.price_histories_dir / "key_name.tsv"

    @property
    def neo4j_import_dir(self) -> Path:
        return self.root / "neo4j" / "import"

    @property
    def game_review_batches_dir(self) -> Path:
        return self.root / "game_review_batches"

    @property
    def user_dir(self) -> Path:
        return self.root / "user"

    @property
    def zrc26_collection_jsonl(self) -> Path:
        """Single-file convenience path; full export uses all `user/*_collection.jsonl`."""
        return self.user_dir / "zrc26_collection.jsonl"


def default_paths() -> ProjectPaths:
    # Assumes this file lives at <repo>/kg_etl/paths.py
    root = Path(__file__).resolve().parents[1]
    return ProjectPaths(root=root)

