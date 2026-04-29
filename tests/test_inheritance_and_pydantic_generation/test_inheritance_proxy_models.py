"""Tests for proxy model inheritance: child shares the parent table/columns/pk."""

from datetime import datetime
from typing import Optional

import pytest
from pydantic import computed_field

import ormar
from ormar import ModelDefinitionError
from ormar.queryset import QuerySet
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()

ONUPDATE_CALLABLE_RESULT = "callable-on-update"
ONUPDATE_TIMESTAMP = datetime(2030, 6, 15, 12, 0, 0)


def _make_callable_value() -> str:
    return ONUPDATE_CALLABLE_RESULT


def _make_timestamp() -> datetime:
    return ONUPDATE_TIMESTAMP


class Human(ormar.Model):
    """Concrete base model with a real table."""

    ormar_config = base_ormar_config.copy(tablename="humans")

    id: int = ormar.Integer(primary_key=True)
    first_name: str = ormar.String(max_length=50)
    last_name: str = ormar.String(max_length=50)


class CustomQuerySet(QuerySet):
    """Marker queryset to verify proxy can override queryset_class."""


class User(Human):
    """Proxy model adding a method without a separate table."""

    ormar_config = base_ormar_config.copy(proxy=True, queryset_class=CustomQuerySet)

    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"


class Admin(User):
    """Two-level proxy chain — should still resolve back to Human's table."""

    ormar_config = base_ormar_config.copy(proxy=True)

    def label(self) -> str:
        return f"admin:{self.full_name()}"


class HumanWithComputed(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="humans_computed")

    id: int = ormar.Integer(primary_key=True)
    first_name: str = ormar.String(max_length=50)
    last_name: str = ormar.String(max_length=50)


class UserWithComputed(HumanWithComputed):
    ormar_config = base_ormar_config.copy(proxy=True)

    @computed_field
    def display_name(self) -> str:
        return f"{self.first_name} {self.last_name}".upper()


class Audited(ormar.Model):
    """Parent with on_update fields — used to verify on_update fires via proxy."""

    ormar_config = base_ormar_config.copy(tablename="audited")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    revision: int = ormar.Integer(default=0, on_update=99)
    note: str = ormar.String(
        max_length=50, default="initial", on_update=_make_callable_value
    )
    updated_at: Optional[datetime] = ormar.DateTime(
        default=None, nullable=True, on_update=_make_timestamp
    )


class AuditedProxy(Audited):
    """Proxy that should inherit the parent's on_update behavior."""

    ormar_config = base_ormar_config.copy(proxy=True)


create_test_database = init_tests(base_ormar_config)


def test_proxy_shares_parent_table() -> None:
    assert User.ormar_config.table is Human.ormar_config.table
    assert User.ormar_config.tablename == Human.ormar_config.tablename
    assert User.ormar_config.pkname == Human.ormar_config.pkname
    assert User.ormar_config.columns is Human.ormar_config.columns
    assert User.ormar_config.model_fields is Human.ormar_config.model_fields
    assert User.ormar_config.metadata is Human.ormar_config.metadata
    assert User.ormar_config.database is Human.ormar_config.database
    assert User.ormar_config.proxy is True
    assert Human.ormar_config.proxy is False


def test_proxy_signals_are_independent() -> None:
    assert User.ormar_config.signals is not Human.ormar_config.signals


def test_proxy_uses_overridden_queryset_class() -> None:
    assert isinstance(User.objects, CustomQuerySet)
    assert not isinstance(Human.objects, CustomQuerySet)


def test_two_level_proxy_chain_shares_root_table() -> None:
    assert Admin.ormar_config.table is Human.ormar_config.table
    assert Admin.ormar_config.proxy is True


@pytest.mark.asyncio
async def test_proxy_can_query_parent_rows() -> None:
    async with base_ormar_config.database:
        await Human.objects.create(first_name="foo", last_name="bar")

        users = await User.objects.all()
        assert len(users) == 1
        assert isinstance(users[0], User)
        assert users[0].full_name() == "foo bar"

        admins = await Admin.objects.all()
        assert len(admins) == 1
        assert isinstance(admins[0], Admin)
        assert admins[0].label() == "admin:foo bar"


@pytest.mark.asyncio
async def test_proxy_save_persists_to_shared_table() -> None:
    async with base_ormar_config.database:
        await User.objects.create(first_name="alice", last_name="liddell")

        humans = await Human.objects.filter(first_name="alice").all()
        assert len(humans) == 1
        assert humans[0].last_name == "liddell"
        assert isinstance(humans[0], Human)


def test_proxy_cannot_add_new_fields() -> None:
    with pytest.raises(ModelDefinitionError, match="cannot declare new ormar fields"):

        class BadProxy(Human):  # pragma: no cover
            ormar_config = base_ormar_config.copy(proxy=True)

            extra: str = ormar.String(max_length=10)


def test_proxy_cannot_inherit_from_abstract() -> None:
    class AbstractBase(ormar.Model):
        ormar_config = base_ormar_config.copy(abstract=True)

        id: int = ormar.Integer(primary_key=True)
        name: str = ormar.String(max_length=10)

    with pytest.raises(ModelDefinitionError, match="cannot inherit from abstract"):

        class BadProxy(AbstractBase):  # pragma: no cover
            ormar_config = base_ormar_config.copy(proxy=True)


def test_proxy_and_abstract_are_mutually_exclusive() -> None:
    with pytest.raises(ModelDefinitionError, match="cannot be both proxy and abstract"):

        class BadProxy(Human):  # pragma: no cover
            ormar_config = base_ormar_config.copy(proxy=True, abstract=True)


def test_proxy_property_field_extends_parent() -> None:
    user = UserWithComputed(id=1, first_name="alice", last_name="liddell")
    dumped = user.model_dump()
    assert dumped["display_name"] == "ALICE LIDDELL"

    base = HumanWithComputed(id=1, first_name="alice", last_name="liddell")
    assert "display_name" not in base.model_dump()


def test_proxy_copy_preserves_proxy_flag() -> None:
    cfg = base_ormar_config.copy(proxy=True)
    again = cfg.copy()
    assert again.proxy is True


def test_proxy_without_concrete_parent_rejected() -> None:
    with pytest.raises(ModelDefinitionError, match="no concrete ormar parent"):

        class FloatingProxy(ormar.Model):  # pragma: no cover
            ormar_config = base_ormar_config.copy(proxy=True)


def test_proxy_with_multiple_concrete_bases_different_tables_rejected() -> None:
    class TableA(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="proxy_a")

        id: int = ormar.Integer(primary_key=True)
        label: str = ormar.String(max_length=10)

    class TableB(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="proxy_b")

        id: int = ormar.Integer(primary_key=True)
        label: str = ormar.String(max_length=10)

    with pytest.raises(
        ModelDefinitionError,
        match="cannot inherit from multiple concrete ormar models",
    ):

        class CrossProxy(TableA, TableB):  # pragma: no cover
            ormar_config = base_ormar_config.copy(proxy=True)


def test_proxy_inherits_parent_onupdate_field_set() -> None:
    assert AuditedProxy._onupdate_fields == Audited._onupdate_fields
    assert {"revision", "note", "updated_at"} <= AuditedProxy._onupdate_fields


@pytest.mark.asyncio
async def test_proxy_update_applies_parent_static_on_update() -> None:
    async with base_ormar_config.database:
        record = await AuditedProxy.objects.create(name="alpha")
        assert record.revision == 0

        await record.update()

        refreshed = await Audited.objects.get(id=record.id)
        assert refreshed.revision == 99


@pytest.mark.asyncio
async def test_proxy_update_invokes_parent_callable_on_update() -> None:
    async with base_ormar_config.database:
        record = await AuditedProxy.objects.create(name="beta")
        assert record.note == "initial"
        assert record.updated_at is None

        await record.update()

        refreshed = await Audited.objects.get(id=record.id)
        assert refreshed.note == ONUPDATE_CALLABLE_RESULT
        assert refreshed.updated_at == ONUPDATE_TIMESTAMP


@pytest.mark.asyncio
async def test_proxy_update_with_explicit_value_skips_on_update() -> None:
    async with base_ormar_config.database:
        record = await AuditedProxy.objects.create(name="gamma")

        await record.update(revision=7)

        refreshed = await Audited.objects.get(id=record.id)
        assert refreshed.revision == 7
        assert refreshed.note == ONUPDATE_CALLABLE_RESULT


@pytest.mark.asyncio
async def test_parent_update_unaffected_by_proxy_definition() -> None:
    """Defining a proxy must not regress on_update behavior on the parent class."""
    async with base_ormar_config.database:
        record = await Audited.objects.create(name="delta")

        await record.update()

        refreshed = await Audited.objects.get(id=record.id)
        assert refreshed.revision == 99
        assert refreshed.note == ONUPDATE_CALLABLE_RESULT
