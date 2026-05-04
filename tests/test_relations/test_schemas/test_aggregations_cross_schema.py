"""Aggregate functions over cross-schema relations.

Exercises ``count`` / ``min`` / ``max`` / ``sum`` / ``avg`` against a related
model that lives in a different schema, including the variant that filters
through the relation. These hit the LIMIT / GROUP BY / ORDER BY paths that
previously broke when a join's source table carried a schema name.
"""

from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

from .dialect import requires_schemas

pytestmark = requires_schemas

base_ormar_config = create_config()


class AggAuthor(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="authors", schema="catalog_people")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class AggBook(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="books", schema="catalog_books")

    id: int = ormar.Integer(primary_key=True)
    author: Optional[AggAuthor] = ormar.ForeignKey(AggAuthor)
    title: str = ormar.String(max_length=100)
    year: int = ormar.Integer()
    ranking: int = ormar.Integer()


class AggReviewer(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="reviewers", schema="catalog_reviewers"
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    books: Optional[list[AggBook]] = ormar.ManyToMany(AggBook)


create_test_database = init_tests(base_ormar_config)


async def seed():  # pragma: no cover
    a1 = await AggAuthor.objects.create(name="Author 1")
    await AggBook.objects.create(title="B1", year=1920, ranking=3, author=a1)
    await AggBook.objects.create(title="B2", year=1930, ranking=1, author=a1)
    await AggBook.objects.create(title="B3", year=1923, ranking=5, author=a1)
    return a1


@pytest.mark.asyncio
async def test_count_across_schema_via_select_related():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        await seed()
        # Three rows on the joined side, distinct authors collapses to 1.
        assert await AggAuthor.objects.select_related("aggbooks").count() == 1
        assert (
            await AggAuthor.objects.select_related("aggbooks").count(distinct=False)
            == 3
        )


@pytest.mark.asyncio
async def test_min_max_across_schema_via_related():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        await seed()
        assert (
            await AggAuthor.objects.select_related("aggbooks").min("aggbooks__year")
            == 1920
        )
        assert (
            await AggAuthor.objects.select_related("aggbooks").max("aggbooks__year")
            == 1930
        )


@pytest.mark.asyncio
async def test_sum_avg_across_schema_via_related():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        await seed()
        assert (
            await AggAuthor.objects.select_related("aggbooks").sum("aggbooks__ranking")
            == 9
        )
        avg = await AggAuthor.objects.select_related("aggbooks").avg(
            "aggbooks__ranking"
        )
        assert float(avg) == 3.0


@pytest.mark.asyncio
async def test_aggregate_with_filter_through_relation():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        await seed()
        result = (
            await AggAuthor.objects.select_related("aggbooks")
            .filter(aggbooks__year__lt=1925)
            .min("aggbooks__year")
        )
        assert result == 1920
        result = (
            await AggAuthor.objects.select_related("aggbooks")
            .filter(aggbooks__year__lt=1925)
            .sum("aggbooks__ranking")
        )
        assert result == 8


@pytest.mark.asyncio
async def test_aggregate_across_m2m_in_third_schema():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        author = await seed()
        reviewer = await AggReviewer.objects.create(name="Reviewer 1")
        # Span three schemas in a single query: reviewer -> through -> book.
        for book in await AggBook.objects.filter(author=author).all():
            await reviewer.books.add(book)

        assert (
            await AggReviewer.objects.select_related("books").max("books__year") == 1930
        )
        assert (
            await AggReviewer.objects.select_related("books").count(distinct=False) == 3
        )
