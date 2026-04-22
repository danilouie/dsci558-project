#!/usr/bin/env python3
"""CLI wrapper: build per-category game-description FAISS indices."""

from __future__ import annotations

import sys
from pathlib import Path


def _main() -> int:
    repo = Path(__file__).resolve().parents[1]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    from game_description_faiss.build import build_registry_cli_main  # noqa: PLC0415

    return build_registry_cli_main()


if __name__ == "__main__":
    raise SystemExit(_main())
