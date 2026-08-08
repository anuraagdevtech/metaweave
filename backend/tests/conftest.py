"""Test-session setup: point the app at a scratch SQLite file (not `:memory:`,
so the same DB is visible across the worker threads TestClient may dispatch
requests to) before any `app.*` module is imported, and clean it up after.
"""
from __future__ import annotations

import os
import pathlib

import pytest

_TEST_DB_PATH = pathlib.Path(__file__).parent / "test_metaweave.db"
os.environ["DATABASE_URL"] = f"sqlite:///{_TEST_DB_PATH}"
os.environ["METAWEAVE_ENV"] = "test"
os.environ.pop("ANTHROPIC_API_KEY", None)  # force the regex fallback path in tests

if _TEST_DB_PATH.exists():
    _TEST_DB_PATH.unlink()


@pytest.fixture(scope="session", autouse=True)
def _cleanup_test_db():
    yield
    if _TEST_DB_PATH.exists():
        _TEST_DB_PATH.unlink()
