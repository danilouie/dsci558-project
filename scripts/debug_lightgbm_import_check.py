#!/usr/bin/env python3
"""One-shot diagnostic for LightGBM + libomp on macOS (debug session instrumentation)."""

from __future__ import annotations

import json
import os
import sys
import time

# region agent log
_LOG_PATH = "/Users/vishalsankarram/dsci558-project/.cursor/debug-2099dc.log"


def _log(hypothesis_id: str, location: str, message: str, data: dict) -> None:
    payload = {
        "sessionId": "2099dc",
        "timestamp": int(time.time() * 1000),
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "runId": "lightgbm-import-debug",
    }
    with open(_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload) + "\n")


# endregion agent log


def main() -> int:
    brew_libomp = "/opt/homebrew/opt/libomp/lib/libomp.dylib"
    intel_brew = "/usr/local/opt/libomp/lib/libomp.dylib"

    _log(
        "H1",
        "debug_lightgbm_import_check.py:brew_paths",
        "check libomp paths",
        {
            "brew_arm_exists": os.path.isfile(brew_libomp),
            "brew_intel_exists": os.path.isfile(intel_brew),
            "DYLD_LIBRARY_PATH": os.environ.get("DYLD_LIBRARY_PATH", ""),
        },
    )

    try:
        import lightgbm  # noqa: F401

        _log(
            "H2",
            "debug_lightgbm_import_check.py:import",
            "lightgbm import ok",
            {"version": getattr(__import__("lightgbm"), "__version__", "?")},
        )
        return 0
    except OSError as e:
        _log(
            "H3",
            "debug_lightgbm_import_check.py:import",
            "lightgbm OSError",
            {"type": type(e).__name__, "args": [str(a) for a in e.args]},
        )
        return 1
    except Exception as e:
        _log(
            "H4",
            "debug_lightgbm_import_check.py:import",
            "lightgbm other error",
            {"type": type(e).__name__, "args": [str(a) for a in e.args]},
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
