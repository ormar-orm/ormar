"""
Micro-benchmark for get_column_name_from_alias and related alias functions.
"""

import pytest

from benchmarks.conftest import Author, Book

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("num_lookups", [1000, 10000])
def test_get_column_name_from_alias(benchmark, num_lookups: int) -> None:
    """Benchmark get_column_name_from_alias - O(n) linear scan per call."""
    # Get all column aliases for the model
    aliases = [col.name for col in Author.ormar_config.table.columns]

    def run() -> None:
        for _ in range(num_lookups):
            for alias in aliases:
                Author.get_column_name_from_alias(alias)

    benchmark(run)


@pytest.mark.parametrize("num_lookups", [1000, 10000])
def test_get_column_name_from_alias_book(benchmark, num_lookups: int) -> None:
    """Benchmark on Book model (more fields including FKs)."""
    aliases = [col.name for col in Book.ormar_config.table.columns]

    def run() -> None:
        for _ in range(num_lookups):
            for alias in aliases:
                Book.get_column_name_from_alias(alias)

    benchmark(run)


@pytest.mark.parametrize("num_lookups", [1000, 10000])
def test_translate_columns_to_aliases(benchmark, num_lookups: int) -> None:
    """Benchmark translate_columns_to_aliases - dict key remapping."""

    def run() -> None:
        for _ in range(num_lookups):
            kwargs = {"name": "test", "score": 50, "id": 1}
            Author.translate_columns_to_aliases(kwargs)

    benchmark(run)


@pytest.mark.parametrize("num_lookups", [1000, 10000])
def test_translate_aliases_to_columns(benchmark, num_lookups: int) -> None:
    """Benchmark translate_aliases_to_columns - reverse remapping."""
    # Get aliases
    aliases = {
        field.get_alias(): "value"
        for field_name, field in Author.ormar_config.model_fields.items()
        if field.get_alias()
    }
    if not aliases:
        aliases = {"name": "test", "score": 50, "id": 1}

    def run() -> None:
        for _ in range(num_lookups):
            kwargs = dict(aliases)
            Author.translate_aliases_to_columns(kwargs)

    benchmark(run)
