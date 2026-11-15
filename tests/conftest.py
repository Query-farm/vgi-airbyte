"""Shared fixtures: a worker client, and an isolated cache directory."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from vgi.client import Client

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def client() -> Iterator[Client]:
    """A client that launches the worker as a subprocess, the way DuckDB does."""
    with Client([sys.executable, "-m", "airbyte_worker.worker"]) as c:
        yield c


@pytest.fixture(autouse=True)
def _cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep registry/manifest caches out of the user's cache directory."""
    monkeypatch.setenv("VGI_AIRBYTE_CACHE_DIR", str(tmp_path / "cache"))
