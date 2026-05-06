from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKSPACE = ROOT.parent

for candidate in (
    ROOT / "src",
    WORKSPACE / "contracts" / "src",
    WORKSPACE / "subsystem-sdk",
):
    path = str(candidate)
    if candidate.exists() and path not in sys.path:
        sys.path.insert(0, path)
