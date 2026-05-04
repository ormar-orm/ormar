"""Filtering through a relation that crosses schema boundaries."""

from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

from .dialect import requires_schemas

pytestmark = requires_schemas

base_ormar_config = create_config()


class FAUser(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users", schema="accounts")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class FAOrder(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="orders", schema="commerce")

    id: int = ormar.Integer(primary_key=True)
    user: Optional[FAUser] = ormar.ForeignKey(FAUser)
    total: int = ormar.Integer()


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_filter_by_related_field_across_schema():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await FAUser.objects.create(name="alice")
        bob = await FAUser.objects.create(name="bob")
        await FAOrder.objects.create(user=alice, total=100)
        await FAOrder.objects.create(user=bob, total=200)
        await FAOrder.objects.create(user=alice, total=50)

        alice_orders = await FAOrder.objects.filter(user__name="alice").all()
        assert {o.total for o in alice_orders} == {100, 50}
