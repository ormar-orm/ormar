"""Two models share a tablename but live in distinct schemas."""

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

from .dialect import requires_schemas

pytestmark = requires_schemas

base_ormar_config = create_config()


class HrPerson(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="people", schema="dept_hr")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class FinancePerson(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="people", schema="dept_fin")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


create_test_database = init_tests(base_ormar_config)


def test_metadata_keys_are_schema_qualified():  # pragma: no cover
    keys = set(base_ormar_config.metadata.tables.keys())
    assert "dept_hr.people" in keys
    assert "dept_fin.people" in keys


@pytest.mark.asyncio
async def test_independent_rows_in_each_schema():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        await HrPerson.objects.create(name="Alice")
        await FinancePerson.objects.create(name="Bob")

        assert (await HrPerson.objects.get()).name == "Alice"
        assert (await FinancePerson.objects.get()).name == "Bob"
