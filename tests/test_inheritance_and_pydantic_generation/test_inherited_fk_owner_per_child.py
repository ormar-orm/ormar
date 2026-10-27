from typing import Optional

import pytest
import pytest_asyncio

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Label(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ifo_labels")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Release(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True)

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=50)
    label: Optional[Label] = ormar.ForeignKey(Label)


class Single(Release):
    ormar_config = base_ormar_config.copy(tablename="ifo_singles")


class Compilation(Release):
    ormar_config = base_ormar_config.copy(tablename="ifo_compilations")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def connected():
    async with base_ormar_config.database:
        yield


def test_each_child_owns_its_inherited_fk_field():
    single_field = Single.ormar_config.model_fields["label"]
    compilation_field = Compilation.ormar_config.model_fields["label"]
    assert single_field is not compilation_field
    assert single_field.owner is Single
    assert compilation_field.owner is Compilation


@pytest.mark.asyncio
async def test_join_and_filter_from_first_child_use_its_own_table(connected):
    label = await Label.objects.create(name="indie")
    await Single.objects.create(title="s", label=label)
    await Compilation.objects.create(title="c", label=label)
    single = await Single.objects.select_related("label").get()
    assert single.label.name == "indie"
    assert [
        s.title for s in await Single.objects.filter(label__name="indie").all()
    ] == ["s"]
    loaded = await Label.objects.select_related(["singles", "compilations"]).get()
    assert [s.title for s in loaded.singles] == ["s"]
    assert [c.title for c in loaded.compilations] == ["c"]
