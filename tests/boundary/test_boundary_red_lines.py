from __future__ import annotations

from pathlib import Path

DENIED = (
    "tushare",
    "data_platform.adapters",
    "stg_",
    "raw_zone",
    "graph_engine",
    "main_core",
)
TEXT_SUFFIXES = {".py", ".toml", ".yml", ".yaml", ""}


def test_source_has_no_live_provider_or_downstream_references() -> None:
    root = Path(__file__).resolve().parents[2]
    scanned = []
    for base in (root / "src", root / ".github", root / "Makefile", root / "pyproject.toml"):
        files = [base] if base.is_file() else sorted(base.rglob("*"))
        for path in files:
            if path.is_file() and path.suffix in TEXT_SUFFIXES:
                scanned.append(path)
                text = path.read_text(encoding="utf-8").lower()
                for denied in DENIED:
                    assert denied not in text, f"{denied!r} leaked into {path}"

    assert scanned
