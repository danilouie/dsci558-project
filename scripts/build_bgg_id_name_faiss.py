#!/usr/bin/env python3
"""CLI wrapper: single FAISS index for BGG id + game name."""

from __future__ import annotations

import sys
from pathlib import Path


def _main() -> int:
    repo = Path(__file__).resolve().parents[1]
    if str(repo) not in sys.path:
        sys.path.insert(0, str(repo))
    from bgg_id_name_faiss.build import build_cli_main  # noqa: PLC0415

    return build_cli_main()


if __name__ == "__main__":
    raise SystemExit(_main())
