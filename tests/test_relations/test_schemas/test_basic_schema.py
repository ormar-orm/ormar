"""End-to-end CRUD on two models that live in different schemas."""

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

from .dialect import requires_schemas

pytestmark = requires_schemas

base_ormar_config = create_config()


class HrEmployee(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="employees", schema="hr")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class ItEmployee(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="employees", schema="it")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


create_test_database = init_tests(base_ormar_config)


def test_table_objects_carry_their_schema():  # pragma: no cover
    assert HrEmployee.ormar_config.table.schema == "hr"
    assert ItEmployee.ormar_config.table.schema == "it"


@pytest.mark.asyncio
async def test_round_trip_in_each_schema():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await HrEmployee.objects.create(name="Alice")
        bob = await ItEmployee.objects.create(name="Bob")

        fetched_alice = await HrEmployee.objects.get(id=alice.id)
        fetched_bob = await ItEmployee.objects.get(id=bob.id)
        assert fetched_alice.name == "Alice"
        assert fetched_bob.name == "Bob"

        # Updating one schema's row does not touch the other's.
        await fetched_alice.update(name="Alice in HR")
        bob_again = await ItEmployee.objects.get(id=bob.id)
        assert bob_again.name == "Bob"

        await fetched_alice.delete()
        assert await HrEmployee.objects.count() == 0
        assert await ItEmployee.objects.count() == 1
