"""Foreign key from a model in schema B to a model in schema A."""

from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

from .dialect import requires_schemas

pytestmark = requires_schemas

base_ormar_config = create_config()


class XSUser(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users", schema="public_app")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class XSAuditEvent(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="events", schema="audit_log")

    id: int = ormar.Integer(primary_key=True)
    actor: Optional[XSUser] = ormar.ForeignKey(XSUser)
    message: str = ormar.String(max_length=200)


create_test_database = init_tests(base_ormar_config)


def test_fk_constraint_uses_schema_qualified_target():  # pragma: no cover
    fks = list(XSAuditEvent.ormar_config.table.c["actor"].foreign_keys)
    assert len(fks) == 1
    target = fks[0].target_fullname
    assert target.startswith("public_app.users.")


@pytest.mark.asyncio
async def test_cross_schema_fk_save_and_select_related():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        actor = await XSUser.objects.create(name="root")
        await XSAuditEvent.objects.create(actor=actor, message="login")

        events = await XSAuditEvent.objects.select_related("actor").all()
        assert len(events) == 1
        assert events[0].actor.name == "root"


@pytest.mark.asyncio
async def test_cross_schema_filter_through_relation():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await XSUser.objects.create(name="alice")
        bob = await XSUser.objects.create(name="bob")
        await XSAuditEvent.objects.create(actor=alice, message="ok")
        await XSAuditEvent.objects.create(actor=bob, message="ok")

        alice_events = await XSAuditEvent.objects.filter(actor__name="alice").all()
        assert len(alice_events) == 1


@pytest.mark.asyncio
async def test_cross_schema_prefetch_forward_fk():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await XSUser.objects.create(name="alice")
        await XSAuditEvent.objects.create(actor=alice, message="login")
        await XSAuditEvent.objects.create(actor=alice, message="logout")

        events = await XSAuditEvent.objects.prefetch_related("actor").all()
        assert len(events) == 2
        assert {e.actor.name for e in events} == {"alice"}


@pytest.mark.asyncio
async def test_cross_schema_prefetch_reverse_fk():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await XSUser.objects.create(name="alice")
        bob = await XSUser.objects.create(name="bob")
        await XSAuditEvent.objects.create(actor=alice, message="m1")
        await XSAuditEvent.objects.create(actor=alice, message="m2")
        await XSAuditEvent.objects.create(actor=bob, message="m3")

        users = (
            await XSUser.objects.prefetch_related("xsauditevents")
            .order_by("name")
            .all()
        )
        assert len(users) == 2
        assert {e.message for e in users[0].xsauditevents} == {"m1", "m2"}
        assert {e.message for e in users[1].xsauditevents} == {"m3"}


@pytest.mark.asyncio
async def test_cross_schema_filter_via_reverse_relation():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await XSUser.objects.create(name="alice")
        bob = await XSUser.objects.create(name="bob")
        await XSAuditEvent.objects.create(actor=alice, message="login")
        await XSAuditEvent.objects.create(actor=bob, message="other")

        users = await XSUser.objects.filter(xsauditevents__message="login").all()
        assert [u.name for u in users] == ["alice"]
