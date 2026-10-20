"""
Exercises QueryExecutor's native branches on a dedicated SQLite database, so
they are covered on every database's test run (including drivers without a
native backend).
"""

import asyncio
import contextlib

import pytest
import pytest_asyncio
import sqlalchemy as sa

from ormar.databases.connection import DatabaseConnection
from ormar.databases.query_executor import QueryExecutor

metadata = sa.MetaData()
rows = sa.Table(
    "native_executor_rows",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String(50)),
)


@pytest_asyncio.fixture()
async def database(tmp_path):
    database = DatabaseConnection(f"sqlite+aiosqlite:///{tmp_path}/native.db")
    async with database:
        async with database.engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        assert database.native_backend is not None
        yield database


@pytest.mark.asyncio
async def test_every_executor_method_runs_natively(database):
    async with database.transaction():
        async with database.get_query_executor() as executor:
            pk = await executor.execute(rows.insert().values(name="a"))
            await executor.execute_many(
                rows.update()
                .where(rows.c.id == sa.bindparam("new_id"))
                .values(name=sa.bindparam("new_name")),
                [{"new_id": pk, "new_name": "b"}],
            )
            query = rows.select().where(rows.c.id == pk)
            assert await executor.fetch_all(query) == [{"id": pk, "name": "b"}]
            assert await executor.fetch_one(query) == {"id": pk, "name": "b"}
            assert await executor.fetch_one(rows.select().where(rows.c.id < 0)) is None
            assert await executor.fetch_val(sa.select(rows.c.name)) == "b"
            assert (
                await executor.fetch_val(sa.select(rows.c.name).where(rows.c.id < 0))
                is None
            )
            assert [row async for row in executor.iterate(query)] == [
                {"id": pk, "name": "b"}
            ]
            assert await executor.execute(rows.insert().values(id=500, name="x")) == 500
            returned = await executor.execute(
                rows.insert().values(name="r").returning(rows.c.id)
            )
            assert (
                await executor.fetch_val(
                    sa.select(rows.c.name).where(rows.c.id == returned)
                )
                == "r"
            )


@pytest.mark.asyncio
async def test_driver_errors_are_translated(database):
    broken = sa.table("missing_table", sa.column("x")).select()
    async with database.get_query_executor() as executor:
        with pytest.raises(sa.exc.OperationalError):
            await executor.fetch_all(broken)
    async with database.get_query_executor(transactional=True) as executor:
        with pytest.raises(sa.exc.OperationalError):
            async for _ in executor.iterate(broken):
                pass  # pragma: no cover


@pytest.mark.asyncio
async def test_hide_parameters_is_respected(tmp_path):
    database = DatabaseConnection(
        f"sqlite+aiosqlite:///{tmp_path}/hidden.db", hide_parameters=True
    )
    async with database:
        async with database.engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        async with database.get_query_executor() as executor:
            await executor.execute(rows.insert().values(id=1, name="first"))
            with pytest.raises(sa.exc.IntegrityError) as single:
                await executor.execute(rows.insert().values(id=1, name="secret-one"))
            with pytest.raises(sa.exc.IntegrityError) as many:
                await executor.execute_many(
                    rows.insert().values(id=sa.bindparam("new_id")),
                    [{"new_id": 1}],
                )
    for error in (single.value, many.value):
        assert "parameters hidden" in str(error)
        assert "secret-one" not in str(error)


async def consume(executor, query):
    return [row async for row in executor.iterate(query)]


@pytest.mark.asyncio
@pytest.mark.parametrize("call", ["fetch_all", "iterate"])
async def test_cancelled_native_call_invalidates_connection(database, call):
    query = rows.select()
    async with database._autocommit_engine.connect() as conn:
        executor = QueryExecutor(conn, database.native_backend)
        work = (
            executor.fetch_all(query)
            if call == "fetch_all"
            else consume(executor, query)
        )
        task = asyncio.ensure_future(work)
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert conn.invalidated


@pytest.mark.asyncio
async def test_plain_text_statements_match_sqlalchemy(database):
    async with database.get_query_executor() as executor:
        rows_out = await executor.fetch_all(sa.text("SELECT 1 AS x"))
        assert [dict(row) for row in rows_out] == [{"x": 1}]
        assert await executor.fetch_val(sa.text("SELECT 2 AS y")) == 2
        await executor.execute(rows.insert().values(id=7, name="t"))
        updated = await executor.execute(
            sa.text("UPDATE native_executor_rows SET name = 'u' WHERE id = 7")
        )
        assert updated == 1


@pytest.mark.asyncio
async def test_ddl_runs_through_sqlalchemy(database):
    extra = sa.Table("native_executor_ddl", sa.MetaData(), sa.Column("id", sa.Integer))
    async with database.get_query_executor() as executor:
        await executor.execute(sa.schema.CreateTable(extra))
        assert await executor.fetch_all(extra.select()) == []


@pytest.mark.asyncio
async def test_insert_returning_no_row_gives_none(database):
    nothing = sa.select(rows.c.id, rows.c.name).where(sa.false())
    stmt = rows.insert().from_select(["id", "name"], nothing).returning(rows.c.id)
    async with database.get_query_executor() as executor:
        assert await executor.execute(stmt) is None


@pytest.mark.asyncio
async def test_rows_accept_column_objects_like_row_mapping(database):
    async with database.get_query_executor() as executor:
        await executor.execute(rows.insert().values(id=11, name="keyed"))
        row = (await executor.fetch_all(rows.select().where(rows.c.id == 11)))[0]
    assert row[rows.c.name] == row["name"] == "keyed"
    assert row[rows.c.id] == 11
    with pytest.raises(KeyError):
        row[sa.column("other")]


@pytest.mark.asyncio
async def test_duplicate_labels_behave_like_sqlalchemy(database):
    stmt = sa.select(rows.c.id, sa.literal(1).label("id"))
    async with database.get_query_executor() as executor:
        await executor.execute(rows.insert().values(id=12, name="dup"))
        found = await executor.fetch_all(stmt.where(rows.c.id == 12))
    with pytest.raises(sa.exc.InvalidRequestError):
        found[0]["id"]


@pytest.mark.asyncio
async def test_execute_many_with_no_rows_matches_sqlalchemy(database):
    async with database.get_query_executor() as executor:
        await executor.execute(rows.insert().values(id=21, name="before"))
        await executor.execute_many(rows.update().values(name="after"), [])
        assert (
            await executor.fetch_val(sa.select(rows.c.name).where(rows.c.id == 21))
            == "after"
        )


@pytest.mark.asyncio
async def test_closing_iterate_early_keeps_connection_usable(database):
    async with database.transaction():
        async with database.get_query_executor() as executor:
            await executor.execute_many(
                rows.insert(), [{"id": 100 + i, "name": f"e{i}"} for i in range(3)]
            )
            stream = executor.iterate(rows.select().order_by(rows.c.id))
            async with contextlib.aclosing(stream):
                async for _ in stream:
                    break
            assert await executor.fetch_val(sa.select(sa.func.count(rows.c.id))) >= 3
