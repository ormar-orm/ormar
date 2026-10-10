import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="items")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    kind: str = ormar.String(max_length=50)


create_test_database = init_tests(base_ormar_config)


async def _create_items():
    for name, kind in [("a", "x"), ("b", "y"), ("c", "z"), ("d", "x")]:
        await Item.objects.create(name=name, kind=kind)


@pytest.mark.asyncio
async def test_chained_excludes_all_apply():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await _create_items()

            items = await Item.objects.exclude(kind="x").exclude(name="b").all()
            assert [item.name for item in items] == ["c"]

            items = await Item.objects.filter(kind="x").exclude(name="a").all()
            assert [item.name for item in items] == ["d"]

            items = await Item.objects.exclude(kind="x", name="a").all()
            assert [item.name for item in items] == ["b", "c", "d"]


@pytest.mark.asyncio
async def test_chained_excludes_update_and_delete():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await _create_items()

            qs = Item.objects.exclude(kind="x").exclude(name="b")
            assert await qs.update(kind="ZZ") == 1
            assert await Item.objects.filter(kind="ZZ").count() == 1

            assert await Item.objects.exclude(kind="ZZ").exclude(name="b").delete() == 2
            assert await Item.objects.count() == 2
