#!/usr/bin/env python3
"""CLI wrapper: query FAISS embedding index (see embeddings.search)."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from embeddings.search import search_cli_main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(search_cli_main())
