"""``create(on_conflict="ignore")`` outside a rolled back transaction: separate
coroutines race on their own pooled connections, and a transaction that
ignored a conflict still commits."""

import asyncio

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

race_config = create_config()


class RaceTag(ormar.Model):
    ormar_config = race_config.copy(tablename="oc_race_tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50, unique=True)


create_test_database = init_tests(race_config)


@pytest.mark.asyncio
async def test_concurrent_create_ignore_inserts_once():
    async with race_config.database:
        try:
            results = await asyncio.gather(
                *(
                    RaceTag.objects.create(name="race", on_conflict="ignore")
                    for _ in range(20)
                )
            )
            assert len([result for result in results if result is not None]) == 1
            assert await RaceTag.objects.count() == 1
        finally:
            await RaceTag.objects.delete(each=True)


@pytest.mark.asyncio
async def test_transaction_with_ignored_conflict_commits():
    async with race_config.database:
        try:
            async with race_config.database.transaction():
                await RaceTag.objects.create(name="a")
                assert (
                    await RaceTag.objects.create(name="a", on_conflict="ignore") is None
                )
                await RaceTag.objects.create(name="b")
            assert await RaceTag.objects.count() == 2
        finally:
            await RaceTag.objects.delete(each=True)
