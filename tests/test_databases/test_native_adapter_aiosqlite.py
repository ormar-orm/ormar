import pytest
import pytest_asyncio
from sqlalchemy.dialects.mysql.aiomysql import MySQLDialect_aiomysql

from ormar.databases.native.adapters import select_adapter
from ormar.databases.native.adapters.aiosqlite import AiosqliteAdapter
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, create_config
from tests.test_databases import native_adapter_cases as cases

pytestmark = pytest.mark.skipif(
    not DATABASE_URL.startswith("sqlite"), reason="aiosqlite adapter runs on SQLite"
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
    assert isinstance(adapter, AiosqliteAdapter)
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


def test_select_adapter_unknown_driver_is_none():
    assert select_adapter(MySQLDialect_aiomysql()) is None
