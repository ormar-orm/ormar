import pytest
import pytest_asyncio
import sqlalchemy as sa

from ormar.databases.native.adapters import select_adapter
from ormar.databases.native.adapters.asyncmy import AsyncmyAdapter
from ormar.databases.native.compiler import NativeCompiler
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, MYSQL_DRIVER, create_config
from tests.test_databases import native_adapter_cases as cases

pytestmark = pytest.mark.skipif(
    not (DATABASE_URL.startswith("mysql") and MYSQL_DRIVER == "asyncmy"),
    reason="asyncmy adapter runs on MySQL with asyncmy",
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
    assert isinstance(adapter, AsyncmyAdapter)
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


@pytest.mark.asyncio
async def test_percent_in_values_and_like_matches_sqlalchemy(database, adapter):
    compiler = NativeCompiler(database.engine.dialect)
    async with database._autocommit_engine.connect() as conn:
        raw = await conn.get_raw_connection()
        await conn.execute(
            items.insert(), [{"name": "50%"}, {"name": "a%b"}, {"name": "plain"}]
        )
        statements = [
            items.select().where(items.c.name == "50%"),
            items.select()
            .where(items.c.name.contains("%", autoescape=True))
            .order_by(items.c.id),
            sa.select(sa.literal_column("'100%'").label("literal")),
        ]
        results = []
        for stmt in statements:
            native = await adapter.fetch_all(raw, compiler.bind(stmt))
            assert native == await cases.sqlalchemy_rows(conn, stmt)
            results.append(native)
    assert [row["name"] for row in results[0]] == ["50%"]
    assert [row["name"] for row in results[1]] == ["50%", "a%b"]
    assert results[2] == [{"literal": "100%"}]


@pytest.mark.asyncio
async def test_attribute_error_is_translated_as_disconnect(database, adapter):
    async with database._autocommit_engine.connect() as conn:
        raw = await conn.get_raw_connection()
        error = AttributeError("'NoneType' object has no attribute 'write'")
        assert isinstance(error, adapter.driver_errors)
        translated = adapter.translate_error(error, raw.dbapi_connection)
        assert isinstance(
            translated, database.engine.dialect.loaded_dbapi.InternalError
        )
        assert database.engine.dialect.is_disconnect(
            translated, raw.dbapi_connection, None
        )
