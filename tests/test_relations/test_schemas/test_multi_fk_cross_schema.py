"""Two foreign keys from one model into the same schema-qualified target."""

from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

from .dialect import requires_schemas

pytestmark = requires_schemas

base_ormar_config = create_config()


class MFUser(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users", schema="core")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class MFTicket(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tickets", schema="support")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    created_by: Optional[MFUser] = ormar.ForeignKey(MFUser, related_name="created")
    updated_by: Optional[MFUser] = ormar.ForeignKey(MFUser, related_name="updated")


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_two_fks_to_same_target_resolve_independently():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await MFUser.objects.create(name="alice")
        bob = await MFUser.objects.create(name="bob")
        await MFTicket.objects.create(title="t1", created_by=alice, updated_by=bob)

        ticket = await MFTicket.objects.select_related(
            ["created_by", "updated_by"]
        ).get()
        assert ticket.created_by.name == "alice"
        assert ticket.updated_by.name == "bob"
