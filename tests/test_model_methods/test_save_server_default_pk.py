"""Tests for ``Model.save()`` behavior around ``server_default`` on the pk.

Covers two closely related concerns:

1. N+1 fix (PR #919) — when the pk is the only ``server_default`` field, the
   INSERT's RETURNING clause already provides the pk, so ``save()`` must not
   issue a second SELECT to reload the model.

2. Pk-recovery loud-fail — on backends that cannot return a server-generated
   pk (Oracle MySQL has no RETURNING), ``save()`` must raise
   ``ModelPersistenceError`` rather than silently storing a bogus pk (the old
   behavior was to return ``rowcount`` from the executor, which ``save()``
   then mistook for the pk).
"""

import functools
import uuid
from typing import Any, List

import pytest
from sqlalchemy import text

import ormar
from ormar.databases.query_executor import QueryExecutor
from ormar.exceptions import ModelPersistenceError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()

_IS_MYSQL = "mysql" in base_ormar_config.database.url


class ServerDefaultPk(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="server_default_pk")

    id: int = ormar.Integer(
        primary_key=True, autoincrement=False, server_default=text("100")
    )
    name: str = ormar.String(max_length=100)


class ServerDefaultNonPk(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="server_default_nonpk")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    company: str = ormar.String(max_length=100, server_default="Acme")


class ServerDefaultPkAndNonPk(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="server_default_pk_and_nonpk")

    id: int = ormar.Integer(
        primary_key=True, autoincrement=False, server_default=text("200")
    )
    name: str = ormar.String(max_length=100)
    company: str = ormar.String(max_length=100, server_default="Acme")


SERVER_UUID = uuid.UUID("00000000-0000-0000-0000-000000000042")


class ServerDefaultUuidPk(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="server_default_uuid_pk")

    id: uuid.UUID = ormar.UUID(
        primary_key=True,
        uuid_format="string",
        server_default=text(f"'{SERVER_UUID}'"),
    )
    name: str = ormar.String(max_length=100)


create_test_database = init_tests(base_ormar_config)


class _RecordingMethod:
    """Replaces a ``QueryExecutor`` method, recording each executed statement."""

    def __init__(self, counter: "_StatementCounter", original: Any) -> None:
        self.counter = counter
        self.original = original

    def __get__(self, instance: Any, owner: Any) -> Any:
        return functools.partial(self.call, instance)

    async def call(self, executor: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        self.counter.statements.append(str(query))
        return await self.original(executor, query, *args, **kwargs)


class _StatementCounter:
    """Records every statement ormar executes, on either execution path."""

    METHODS = ("fetch_all", "fetch_one", "fetch_val", "execute")

    def __init__(self) -> None:
        self.statements: List[str] = []

    def __enter__(self) -> "_StatementCounter":
        self._originals = {name: getattr(QueryExecutor, name) for name in self.METHODS}
        for name, original in self._originals.items():
            setattr(QueryExecutor, name, _RecordingMethod(self, original))
        return self

    def __exit__(self, *exc: Any) -> None:
        for name, original in self._originals.items():
            setattr(QueryExecutor, name, original)


def _table_selects(statements: List[str], tablename: str) -> List[str]:
    return [
        s
        for s in statements
        if s.lstrip().upper().startswith("SELECT") and tablename in s
    ]


@pytest.mark.asyncio
@pytest.mark.skipif(
    _IS_MYSQL,
    reason=(
        "Oracle MySQL has no RETURNING clause, so a server_default on a "
        "non-AUTO_INCREMENT pk cannot be recovered — covered by "
        "test_save_raises_when_server_default_pk_cannot_be_recovered instead."
    ),
)
async def test_save_does_not_reload_when_only_pk_has_server_default():  # noqa: E501  # pragma: no cover
    """INSERT returns the server-generated pk, so no SELECT should follow."""
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with _StatementCounter() as counter:
                instance = ServerDefaultPk(name="first")
                await instance.save()

            selects = _table_selects(
                counter.statements, ServerDefaultPk.ormar_config.tablename
            )
            assert instance.pk is not None
            assert selects == [], counter.statements


@pytest.mark.asyncio
async def test_save_still_reloads_when_non_pk_has_server_default():
    """Regression guard: non-pk server defaults must still trigger a reload."""
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with _StatementCounter() as counter:
                instance = ServerDefaultNonPk(id=1, name="first")
                await instance.save()

            selects = _table_selects(
                counter.statements, ServerDefaultNonPk.ormar_config.tablename
            )
            assert instance.company == "Acme"
            assert len(selects) == 1, counter.statements


@pytest.mark.asyncio
@pytest.mark.skipif(
    _IS_MYSQL,
    reason=(
        "Oracle MySQL has no RETURNING clause, so a server_default on a "
        "non-AUTO_INCREMENT pk cannot be recovered — covered by "
        "test_save_raises_when_server_default_pk_cannot_be_recovered instead."
    ),
)
async def test_save_reloads_once_when_both_pk_and_non_pk_have_server_default():  # noqa: E501  # pragma: no cover
    """Mixed case: still need exactly one reload for the non-pk column."""
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with _StatementCounter() as counter:
                instance = ServerDefaultPkAndNonPk(name="first")
                await instance.save()

            selects = _table_selects(
                counter.statements, ServerDefaultPkAndNonPk.ormar_config.tablename
            )
            assert instance.pk is not None
            assert instance.company == "Acme"
            assert len(selects) == 1, counter.statements


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _IS_MYSQL,
    reason=(
        "This loud-fail path only fires on backends that cannot return a "
        "server-generated pk. RETURNING-capable backends (PostgreSQL, "
        "SQLite 3.35+, MariaDB 10.5+) succeed here."
    ),
)
async def test_save_raises_when_server_default_pk_cannot_be_recovered():  # noqa: E501  # pragma: no cover
    """Oracle MySQL: no RETURNING → save() must raise, not silently store a
    bogus pk (the old bug was to coerce rowcount into ``Model.pk``)."""
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            instance = ServerDefaultPk(name="first")
            with pytest.raises(ModelPersistenceError, match="primary key"):
                await instance.save()
            assert instance.pk is None


@pytest.mark.asyncio
@pytest.mark.skipif(
    _IS_MYSQL,
    reason="Oracle MySQL has no RETURNING clause to recover a server default pk.",
)
async def test_save_returns_typed_uuid_pk_from_server_default():  # noqa: E501  # pragma: no cover
    """The pk returned by RETURNING goes through the column type, like on
    SQLAlchemy's execution path: a UUID column yields a ``uuid.UUID``."""
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            instance = await ServerDefaultUuidPk(name="first").save()
            assert instance.pk == SERVER_UUID
            assert isinstance(instance.pk, uuid.UUID)
