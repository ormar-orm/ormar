from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Shelf(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="zk_shelves")

    id: int = ormar.Integer(primary_key=True, autoincrement=False)
    name: str = ormar.String(max_length=50)


class Box(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="zk_boxes")

    id: int = ormar.Integer(primary_key=True, autoincrement=False)
    shelf: Shelf = ormar.ForeignKey(Shelf, nullable=False, related_name="boxes")
    spare: Optional[Shelf] = ormar.ForeignKey(Shelf, related_name="spare_boxes")


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_relations_to_a_zero_key():
    async with base_ormar_config.database:
        shelf = await Shelf.objects.create(id=0, name="zero")
        await Box.objects.create(id=0, shelf=shelf, spare=0)
        box = await Box.objects.select_related(["shelf", "spare"]).get()
        assert box.shelf.name == "zero"
        assert box.spare.name == "zero"
        loaded = await Shelf.objects.prefetch_related("boxes").get(id=0)
        assert [b.id for b in loaded.boxes] == [0]
        nested = await Box.objects.prefetch_related("shelf__spare_boxes").get()
        assert [b.id for b in nested.shelf.spare_boxes] == [0]
