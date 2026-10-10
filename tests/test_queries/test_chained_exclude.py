import pytest

import ormar
from ormar.exceptions import QueryDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="items")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    kind: str = ormar.String(max_length=50)


create_test_database = init_tests(base_ormar_config)


async def create_items():
    for name, kind in [("a", "x"), ("b", "y"), ("c", "z"), ("d", "x")]:
        await Item.objects.create(name=name, kind=kind)


@pytest.mark.asyncio
async def test_chained_excludes_all_apply():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_items()

            qs = Item.objects.exclude(kind="x").exclude(name="b")
            items = await qs.order_by("id").all()
            assert [item.name for item in items] == ["c"]

            items = await Item.objects.filter(kind="x").exclude(name="a").all()
            assert [item.name for item in items] == ["d"]

            items = await Item.objects.exclude(kind="x", name="a").order_by("id").all()
            assert [item.name for item in items] == ["b", "c", "d"]


@pytest.mark.asyncio
async def test_chained_excludes_update_and_delete():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_items()

            qs = Item.objects.exclude(kind="x").exclude(name="b")
            assert await qs.update(kind="ZZ") == 1
            assert await Item.objects.filter(kind="ZZ").count() == 1

            assert await Item.objects.exclude(kind="ZZ").exclude(name="b").delete() == 2
            assert await Item.objects.count() == 2


@pytest.mark.asyncio
async def test_empty_exclude_is_noop():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_items()

            assert await Item.objects.exclude().count() == 4
            assert await Item.objects.exclude(**{}).count() == 4
            with pytest.raises(QueryDefinitionError):
                await Item.objects.exclude().delete()


@pytest.mark.asyncio
async def test_chained_exclude_with_groups_and_limit():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_items()

            items = (
                await Item.objects.exclude(ormar.or_(kind="x", name="b"))
                .exclude(name="z")
                .order_by("id")
                .all()
            )
            assert [item.name for item in items] == ["c"]

            items = (
                await Item.objects.exclude(name="a")
                .exclude(name="b")
                .order_by("id")
                .limit(1)
                .offset(1)
                .all()
            )
            assert [item.name for item in items] == ["d"]
