"""Concurrent updates with column expressions run without a surrounding
transaction, so each coroutine writes on its own connection and the database
evaluates every ``SET col = col + 1`` against the current row."""

import asyncio

import pytest

import ormar
from ormar import F
from tests.lifespan import init_tests
from tests.settings import create_config

race_config = create_config()


class Counter(ormar.Model):
    ormar_config = race_config.copy(tablename="fx_counters")

    id: int = ormar.Integer(primary_key=True)
    value: int = ormar.Integer(default=0)


create_test_database = init_tests(race_config)


@pytest.mark.asyncio
async def test_concurrent_increments_are_not_lost():
    async with race_config.database:
        counter = await Counter.objects.create()
        try:
            qs = Counter.objects.filter(id=counter.id)
            await asyncio.gather(
                *(qs.update(value=Counter.value + 1) for _ in range(20))
            )
            assert (await qs.get()).value == 20
        finally:
            await Counter.objects.delete(each=True)


@pytest.mark.asyncio
async def test_conditional_decrements_stop_at_zero():
    async with race_config.database:
        counter = await Counter.objects.create(value=3)
        try:
            qs = Counter.objects.filter(id=counter.id, value__gt=0)
            counts = await asyncio.gather(
                *(qs.update(value=F("value") - 1) for _ in range(10))
            )
            assert sum(counts) == 3
            assert (await Counter.objects.get(id=counter.id)).value == 0
        finally:
            await Counter.objects.delete(each=True)
