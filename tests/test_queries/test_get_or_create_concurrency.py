"""Concurrent ``get_or_create`` race-recovery tests for issue #1016.

These tests run under AUTOCOMMIT (no surrounding transaction) so they
exercise the real-world scenario: separate coroutines, each pulling its
own connection from the pool, racing to insert a row.

The project-wide ``force_rollback=True`` config can't be used here:
PostgreSQL aborts the surrounding transaction on the first failed
statement, which makes the recovery ``get`` raise
``InFailedSQLTransactionError`` instead of returning the winner's row.
"""

import asyncio
from typing import Optional

import pytest
import sqlalchemy.exc

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

race_config = create_config()


class RaceLibrary(ormar.Model):
    ormar_config = race_config.copy(tablename="race_libraries")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class RaceBook(ormar.Model):
    ormar_config = race_config.copy(tablename="race_unique_books")

    id: int = ormar.Integer(primary_key=True)
    isbn: str = ormar.String(max_length=20, unique=True)
    title: str = ormar.String(max_length=200)
    library: Optional[RaceLibrary] = ormar.ForeignKey(
        RaceLibrary, nullable=True, related_name="books"
    )


create_test_database = init_tests(race_config)


async def _cleanup() -> None:
    await RaceBook.objects.delete(each=True)
    await RaceLibrary.objects.delete(each=True)


@pytest.mark.asyncio
async def test_get_or_create_recovers_from_concurrent_create():
    """Two concurrent ``get_or_create`` calls race for real: both
    ``SELECT``s return empty, then one ``INSERT`` wins and the other
    raises ``IntegrityError``. The losing call must catch the error,
    retry the ``get``, and return the winner's row with ``created=False``
    rather than leaking the driver error.

    Both calls returning the same pk with exactly one ``created=True``
    proves the recovery path actually fired — if the IntegrityError had
    propagated, ``asyncio.gather`` would have re-raised it.
    """
    async with race_config.database:
        try:
            results = await asyncio.gather(
                RaceBook.objects.get_or_create(isbn="978-0-00-000000-1", title="Race"),
                RaceBook.objects.get_or_create(isbn="978-0-00-000000-1", title="Race"),
            )

            (book1, c1), (book2, c2) = results
            assert book1.pk == book2.pk
            assert book1.isbn == "978-0-00-000000-1"
            assert sorted([c1, c2]) == [False, True]
            assert await RaceBook.objects.count() == 1
        finally:
            await _cleanup()


@pytest.mark.asyncio
async def test_get_or_create_propagates_unrelated_integrity_error():
    """If ``create`` fails with ``IntegrityError`` and the retry ``get``
    still finds nothing matching the caller's filter, the violation
    isn't a race — it's a legitimate constraint conflict (e.g. a unique
    column the caller isn't filtering by). The original ``IntegrityError``
    must propagate instead of being swallowed.
    """
    async with race_config.database:
        try:
            await RaceBook.objects.create(isbn="978-0-00-000000-2", title="Original")
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                await RaceBook.objects.get_or_create(
                    isbn="978-0-00-000000-2", title="Different"
                )
        finally:
            await _cleanup()


@pytest.mark.asyncio
async def test_proxy_get_or_create_recovers_from_concurrent_create():
    """Same real-concurrency race but via the m2m / reverse-fk proxy
    ``QuerysetProxy.get_or_create``, which has its own try/except around
    the get/create pair.
    """
    async with race_config.database:
        try:
            library = await RaceLibrary.objects.create(name="Main")

            results = await asyncio.gather(
                library.books.get_or_create(
                    isbn="978-0-00-000000-3", title="ProxyRace"
                ),
                library.books.get_or_create(
                    isbn="978-0-00-000000-3", title="ProxyRace"
                ),
            )

            (book1, c1), (book2, c2) = results
            assert book1.pk == book2.pk
            assert sorted([c1, c2]) == [False, True]
            assert await RaceBook.objects.count() == 1
        finally:
            await _cleanup()


@pytest.mark.asyncio
async def test_proxy_get_or_create_propagates_unrelated_integrity_error():
    """``QuerysetProxy.get_or_create`` re-raises the original
    ``IntegrityError`` when the retry ``get`` still doesn't match —
    same semantics as ``QuerySet.get_or_create``.
    """
    async with race_config.database:
        try:
            library = await RaceLibrary.objects.create(name="Main")
            await RaceBook.objects.create(
                isbn="978-0-00-000000-4", title="Original", library=library
            )

            with pytest.raises(sqlalchemy.exc.IntegrityError):
                await library.books.get_or_create(
                    isbn="978-0-00-000000-4", title="Different"
                )
        finally:
            await _cleanup()
