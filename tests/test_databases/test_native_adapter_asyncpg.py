import functools

import pytest
import pytest_asyncio

from ormar.databases.native.adapters import select_adapter
from ormar.databases.native.adapters.asyncpg import AsyncpgAdapter
from ormar.databases.native.compiler import NativeCompiler
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, create_config
from tests.test_databases import native_adapter_cases as cases

pytestmark = pytest.mark.skipif(
    not DATABASE_URL.startswith("postgresql"),
    reason="asyncpg adapter runs on PostgreSQL",
)

base_ormar_config = create_config()
items = cases.make_items_table(base_ormar_config.metadata)
create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def database():
    async with base_ormar_config.database as database:
        yield database


@pytest.fixture()
def adapter(database):
    adapter = select_adapter(database.engine.dialect)
    assert isinstance(adapter, AsyncpgAdapter)
    return adapter


@pytest.mark.asyncio
async def test_fetch_all_matches_sqlalchemy(database, adapter):
    await cases.check_fetch_all_matches_sqlalchemy(database, adapter, items)


@pytest.mark.asyncio
async def test_execute_insert_returns_pk(database, adapter):
    await cases.check_execute_insert_returns_pk(database, adapter, items)


@pytest.mark.asyncio
async def test_execute_insert_with_explicit_pk_returns_it(database, adapter):
    await cases.check_execute_insert_with_explicit_pk_returns_it(
        database, adapter, items
    )


@pytest.mark.asyncio
async def test_execute_update_returns_rowcount(database, adapter):
    await cases.check_execute_update_returns_rowcount(database, adapter, items)


@pytest.mark.asyncio
async def test_execute_many_updates_all_rows(database, adapter):
    await cases.check_execute_many_updates_all_rows(database, adapter, items)


@pytest.mark.asyncio
async def test_stream_yields_all_rows_in_order(database, adapter):
    await cases.check_stream_yields_all_rows_in_order(database, adapter, items)


@pytest.mark.asyncio
async def test_concurrent_statements_on_transaction_connection(database, adapter):
    await cases.check_concurrent_statements_on_transaction_connection(
        database, adapter, items
    )


class PrepareCounter:
    def __init__(self, original):
        self.original = original
        self.calls = 0

    def __get__(self, instance, owner):
        return functools.partial(self.call, instance)

    async def call(self, connection, *args, **kwargs):
        self.calls += 1
        return await self.original(connection, *args, **kwargs)


@pytest.mark.asyncio
async def test_prepare_called_once_per_plan(database, adapter, monkeypatch):
    compiler = NativeCompiler(database.engine.dialect)
    async with database._autocommit_engine.connect() as conn:
        raw = await conn.get_raw_connection()
        connection_cls = type(raw.driver_connection)
        counter = PrepareCounter(connection_cls.prepare)
        monkeypatch.setattr(connection_cls, "prepare", counter)
        for name in ("p1", "p2", "p3"):
            await adapter.fetch_all(
                raw, compiler.bind(items.select().where(items.c.name == name))
            )
    assert counter.calls == 1
