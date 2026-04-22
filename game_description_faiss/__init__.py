"""Per-BGG-category FAISS indices over game descriptions (linked by ``bgg_id``).

Import ``game_description_faiss.build`` for the CLI entrypoint to avoid eager
imports of ``embeddings`` / PyTorch when only category helpers are needed.
"""

from game_description_faiss.categories import (
    build_documents_by_category,
    category_slug,
    stable_slug_suffix,
    uncategorized_registry_key,
)

__all__ = [
    "build_documents_by_category",
    "category_slug",
    "stable_slug_suffix",
    "uncategorized_registry_key",
]
