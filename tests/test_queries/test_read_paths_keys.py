from typing import Optional

import pytest
import pytest_asyncio

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Gadget(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rp_gadgets")

    id: int = ormar.Integer(primary_key=True, name="gadget_id")
    name: str = ormar.String(max_length=50)


class Part(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rp_parts")

    id: int = ormar.Integer(primary_key=True, name="part_id")
    label: str = ormar.String(max_length=50)
    gadget: Optional[Gadget] = ormar.ForeignKey(
        Gadget, name="gadget_ref", related_name="parts"
    )


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def connected():
    async with base_ormar_config.database:
        yield


@pytest.mark.asyncio
async def test_aliased_pk_count_iterate_and_paginate(connected):
    for name in "abc":
        gadget = await Gadget.objects.create(name=name)
        await Part.objects.create(label=f"{name}1", gadget=gadget)
        await Part.objects.create(label=f"{name}2", gadget=gadget)
    assert await Gadget.objects.count() == 3
    assert await Gadget.objects.select_related("parts").count() == 3
    assert [g.name async for g in Gadget.objects.iterate()] == ["a", "b", "c"]
    page = await Gadget.objects.select_related("parts").limit(2).all()
    assert [g.name for g in page] == ["a", "b"]
    assert [len(g.parts) for g in page] == [2, 2]
    assert (await Gadget.objects.last()).name == "c"
    assert (await Gadget.objects.get(pk=page[0].pk)).name == "a"
    only_names = await Gadget.objects.fields("name").all()
    assert [g.pk for g in only_names] == [g.pk for g in await Gadget.objects.all()]
