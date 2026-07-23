from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def fixture_dir() -> Path:
    """Give every test one portable path to repository-owned provider fixtures."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def load_fixture(fixture_dir: Path):
    """Return a small helper for loading named JSON API responses."""

    def _load(name: str) -> dict[str, Any]:
        return json.loads((fixture_dir / name).read_text(encoding="utf-8"))

    return _load
