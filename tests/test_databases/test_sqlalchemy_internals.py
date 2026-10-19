"""Guards the SQLAlchemy internals that native execution relies on."""

from datetime import datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.mysql.asyncmy import MySQLDialect_asyncmy
from sqlalchemy.dialects.postgresql.asyncpg import (
    AsyncAdapt_asyncpg_connection,
    PGDialect_asyncpg,
)
from sqlalchemy.dialects.sqlite.aiosqlite import SQLiteDialect_aiosqlite
from sqlalchemy.types import TypeEngine

metadata = sa.MetaData()
items = sa.Table(
    "internals_items",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String(50)),
    sa.Column("created", sa.DateTime, default=datetime.now),
)

DIALECTS = [PGDialect_asyncpg, MySQLDialect_asyncmy, SQLiteDialect_aiosqlite]


def compile_with_key(stmt, dialect):
    key = stmt._generate_cache_key()
    return key, stmt._compiler(dialect, cache_key=key)


@pytest.mark.parametrize("dialect_cls", DIALECTS)
def test_sqlalchemy_internals_used_by_native_execution_exist(dialect_cls):
    dialect = dialect_cls()
    select = items.select().where(items.c.id.in_([1, 2]))
    key, compiled = compile_with_key(select, dialect)
    assert hash(key.key) is not None
    params = compiled.construct_params(
        extracted_parameters=key[1], escape_names=True, _no_postcompile=True
    )
    expanded = compiled.construct_expanded_state(params, escape_names=False)
    assert expanded.statement and expanded.positiontup is not None
    assert isinstance(expanded.parameters, dict)
    assert isinstance(expanded.processors, dict)
    assert isinstance(compiled._bind_processors, dict)
    assert [col[0] for col in compiled._result_columns] == ["id", "name", "created"]

    _, insert = compile_with_key(items.insert().values(name="a"), dialect)
    assert [col.name for col in insert.insert_prefetch] == ["created"]
    assert insert._within_exec_param_key_getter(items.c.created) == "created"
    _, update = compile_with_key(items.update().values(name="b"), dialect)
    assert update.update_prefetch == []

    assert callable(TypeEngine._cached_result_processor)
    assert callable(sa.exc.DBAPIError.instance)


def test_asyncpg_adapter_internals_exist():
    assert "_started" in AsyncAdapt_asyncpg_connection.__slots__
    for name in ("_start_transaction", "_handle_exception"):
        assert callable(getattr(AsyncAdapt_asyncpg_connection, name))
    assert "_execute_mutex" in AsyncAdapt_asyncpg_connection.__slots__


def test_sqlalchemy_version_in_pinned_range():
    version = tuple(int(part) for part in sa.__version__.split(".")[:3])
    assert (2, 0, 40) <= version < (2, 1)
