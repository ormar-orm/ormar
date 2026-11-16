import pytest
import pytest_asyncio

import ormar
from ormar.queryset.write_statements import (
    DeleteWrite,
    RowSelectWrite,
    UpdateWrite,
    row_template,
)
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Gadget(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="kw_gadgets")

    id: int = ormar.Integer(primary_key=True, name="gadget_id")
    name: str = ormar.String(max_length=50)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def connected():
    async with base_ormar_config.database:
        yield


@pytest.mark.asyncio
async def test_aliased_pk_write_templates(connected):
    update = row_template(UpdateWrite, model_cls=Gadget, pk=1, values={"name": "b"})
    assert "WHERE kw_gadgets.gadget_id = :ormar_pk" in str(update.statement)
    assert update.params["ormar_pk"] == 1
    delete = row_template(DeleteWrite, model_cls=Gadget, pk=1)
    assert "WHERE kw_gadgets.gadget_id = :ormar_pk" in str(delete.statement)
    select = row_template(RowSelectWrite, model_cls=Gadget, pk=1)
    assert "WHERE kw_gadgets.gadget_id = :ormar_pk" in str(select.statement)


@pytest.mark.asyncio
async def test_aliased_pk_crud_and_bulk_update(connected):
    first = await Gadget.objects.create(name="a")
    second = await Gadget.objects.create(name="b")
    first.name, second.name = "x", "y"
    await Gadget.objects.bulk_update([first, second])
    assert {g.name for g in await Gadget.objects.all()} == {"x", "y"}
    await first.update(name="z")
    await first.load()
    assert first.name == "z"
    other = await Gadget.objects.update_or_create(pk=second.pk, name="w")
    assert other.pk == second.pk and other.name == "w"
    await second.delete()
    assert await Gadget.objects.count() == 1
