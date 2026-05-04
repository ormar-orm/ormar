"""Helpers for selecting which schema tests run on which dialect."""

import os

import pytest

IS_SQLITE = os.getenv("DATABASE_URL", "sqlite:///test.db").startswith("sqlite")

requires_schemas = pytest.mark.skipif(
    IS_SQLITE,
    reason="SQLite does not support per-table schemas",
)
