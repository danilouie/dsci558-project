#!/usr/bin/env python3
"""Deprecated: use ``build_bgg_id_name_faiss.py`` (single index, no categories)."""

from __future__ import annotations

import sys
from pathlib import Path


def _main() -> int:
    repo = Path(__file__).resolve().parents[1]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    from bgg_id_name_faiss.build import build_cli_main  # noqa: PLC0415

    print(
        "Note: build_bgg_id_name_faiss_by_category.py is deprecated — "
        "the BGG id+name builder no longer splits by category. "
        "Use: python scripts/build_bgg_id_name_faiss.py ...",
        file=sys.stderr,
        flush=True,
    )
    return build_cli_main()


if __name__ == "__main__":
    raise SystemExit(_main())
