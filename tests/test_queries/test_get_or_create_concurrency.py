"""Concurrent ``get_or_create`` race-recovery tests for issue #1016.

The first tests run under AUTOCOMMIT (no surrounding transaction) so they
exercise the real-world scenario: separate coroutines, each pulling its
own connection from the pool, racing to insert a row.

The tests inside ``database.transaction()`` check that the insert runs in a
savepoint: PostgreSQL aborts the whole transaction on a failed statement,
so without it the retried ``get`` raised ``InFailedSQLTransactionError``.
The race there is simulated by creating the winner's row in the same
transaction right before the first ``get`` reports a miss.
"""

import asyncio
from typing import Optional

import pytest
import sqlalchemy.exc

import ormar
from ormar.exceptions import NoMatch
from ormar.queryset import QuerySet
from ormar.relations.querysetproxy import QuerysetProxy
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


def fake_first_miss(monkeypatch, owner, create_winner):
    """Make the first ``owner.get`` call create the winner's row and report
    ``NoMatch``, as if a concurrent caller inserted it right after the miss.
    Later calls go to the original ``get``.
    """
    original_get = owner.get
    calls = []

    async def get(self, *args, **kwargs):
        if not calls:
            calls.append(True)
            await create_winner()
            raise NoMatch()
        return await original_get(self, *args, **kwargs)

    monkeypatch.setattr(owner, "get", get)


@pytest.mark.asyncio
async def test_get_or_create_in_transaction_recovers_from_race(monkeypatch):
    """Inside a transaction the losing insert rolls back only its savepoint,
    so the retried ``get`` returns the winner's row and the transaction stays
    usable.
    """
    async with race_config.database:
        async with race_config.database.transaction(force_rollback=True):
            library = await RaceLibrary.objects.create(name="Main")
            fake_first_miss(
                monkeypatch=monkeypatch,
                owner=QuerySet,
                create_winner=lambda: RaceBook.objects.create(
                    isbn="978-0-00-000001-1", title="Winner"
                ),
            )
            book, created = await RaceBook.objects.get_or_create(
                isbn="978-0-00-000001-1", _defaults={"title": "Loser"}
            )
            assert not created
            assert book.title == "Winner"
            await RaceBook.objects.create(isbn="978-0-00-000001-2", title="After")
            assert await RaceBook.objects.count() == 2
            assert await RaceLibrary.objects.get(pk=library.pk)


@pytest.mark.asyncio
async def test_get_or_create_in_transaction_raises_unrelated_integrity_error():
    """An unrelated constraint conflict inside a transaction raises
    ``IntegrityError``, keeps earlier writes and leaves the transaction
    usable.
    """
    async with race_config.database:
        async with race_config.database.transaction(force_rollback=True):
            library = await RaceLibrary.objects.create(name="Main")
            await RaceBook.objects.create(isbn="978-0-00-000001-3", title="Original")
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                await RaceBook.objects.get_or_create(
                    isbn="978-0-00-000001-3", title="Different"
                )
            await RaceBook.objects.create(isbn="978-0-00-000001-4", title="After")
            assert await RaceBook.objects.count() == 2
            assert await RaceLibrary.objects.get(pk=library.pk)


@pytest.mark.asyncio
async def test_proxy_get_or_create_in_transaction_recovers_from_race(monkeypatch):
    """Same in-transaction race through the relation proxy."""
    async with race_config.database:
        async with race_config.database.transaction(force_rollback=True):
            library = await RaceLibrary.objects.create(name="Main")
            fake_first_miss(
                monkeypatch=monkeypatch,
                owner=QuerysetProxy,
                create_winner=lambda: library.books.create(
                    isbn="978-0-00-000001-5", title="Winner"
                ),
            )
            book, created = await library.books.get_or_create(
                isbn="978-0-00-000001-5", _defaults={"title": "Loser"}
            )
            assert not created
            assert book.title == "Winner"
            await library.books.create(isbn="978-0-00-000001-6", title="After")
            assert await library.books.count() == 2


@pytest.mark.asyncio
async def test_proxy_get_or_create_in_transaction_raises_unrelated_integrity_error():
    """Same unrelated constraint conflict through the relation proxy."""
    async with race_config.database:
        async with race_config.database.transaction(force_rollback=True):
            library = await RaceLibrary.objects.create(name="Main")
            await library.books.create(isbn="978-0-00-000001-7", title="Original")
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                await library.books.get_or_create(
                    isbn="978-0-00-000001-7", title="Different"
                )
            await library.books.create(isbn="978-0-00-000001-8", title="After")
            assert await library.books.count() == 2


@pytest.mark.asyncio
async def test_concurrent_get_or_create_misses_in_one_transaction():
    """Two concurrent misses sharing one transaction take turns with their
    savepoints instead of releasing each other's, so both rows are created.
    """
    async with race_config.database:
        async with race_config.database.transaction(force_rollback=True):
            results = await asyncio.gather(
                RaceBook.objects.get_or_create(isbn="978-0-00-000001-9", title="A"),
                RaceBook.objects.get_or_create(isbn="978-0-00-000002-0", title="B"),
            )
            assert [created for _, created in results] == [True, True]
            assert await RaceBook.objects.count() == 2


@pytest.mark.asyncio
async def test_get_or_create_nested_in_pre_save_signal_in_transaction():
    """A ``get_or_create`` called from a ``pre_save`` signal of another
    ``get_or_create`` in the same transaction runs in a nested savepoint
    instead of waiting for the outer call's lock.
    """

    async def ensure_library(sender, instance, **kwargs):
        instance.library, _ = await RaceLibrary.objects.get_or_create(name="Signal")

    RaceBook.ormar_config.signals.pre_save.connect(ensure_library)
    try:
        async with race_config.database:
            async with race_config.database.transaction(force_rollback=True):
                book, created = await asyncio.wait_for(
                    RaceBook.objects.get_or_create(
                        isbn="978-0-00-000002-1", title="Nested"
                    ),
                    timeout=5,
                )
                assert created
                assert book.library.name == "Signal"
                assert await RaceLibrary.objects.count() == 1
    finally:
        RaceBook.ormar_config.signals.pre_save.disconnect(ensure_library)
