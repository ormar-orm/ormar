"""Shared checks run against every native driver adapter on its own dialect."""

import asyncio
from decimal import Decimal
from typing import Any

import sqlalchemy as sa

from ormar.databases.native.compiler import NativeCompiler


def make_items_table(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "native_adapter_items",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("name", sa.String(50), nullable=False),
        sa.Column("price", sa.Numeric(10, 2), nullable=True),
        sa.Column("meta", sa.JSON, nullable=True),
    )


async def sqlalchemy_rows(conn: Any, stmt: Any) -> list:
    return [dict(row) for row in (await conn.execute(stmt)).mappings().all()]


async def check_fetch_all_matches_sqlalchemy(
    database: Any, adapter: Any, table: sa.Table
) -> None:
    compiler = NativeCompiler(database.engine.dialect)
    async with database._autocommit_engine.connect() as conn:
        raw = await conn.get_raw_connection()
        await conn.execute(
            table.insert(),
            [
                {"name": "a", "price": Decimal("1.50"), "meta": {"k": [1]}},
                {"name": "b", "price": None, "meta": None},
            ],
        )
        stmt = table.select().where(table.c.name.in_(["a", "b"])).order_by(table.c.id)
        native = await adapter.fetch_all(raw, compiler.bind(stmt))
        assert native == await sqlalchemy_rows(conn, stmt)
        assert isinstance(native[0]["price"], Decimal)
        assert native[0]["meta"] == {"k": [1]}


async def check_execute_insert_returns_pk(
    database: Any, adapter: Any, table: sa.Table
) -> None:
    compiler = NativeCompiler(database.engine.dialect)
    async with database._autocommit_engine.connect() as conn:
        raw = await conn.get_raw_connection()
        first = await adapter.execute(
            raw, compiler.bind(table.insert().values(name="x"))
        )
        second = await adapter.execute(
            raw, compiler.bind(table.insert().values(name="y"))
        )
        assert isinstance(first, int)
        assert second == first + 1


async def check_execute_insert_with_explicit_pk_returns_it(
    database: Any, adapter: Any, table: sa.Table
) -> None:
    compiler = NativeCompiler(database.engine.dialect)
    async with database._autocommit_engine.connect() as conn:
        raw = await conn.get_raw_connection()
        stmt = table.insert().values(id=4242, name="explicit")
        assert await adapter.execute(raw, compiler.bind(stmt)) == 4242


async def check_execute_update_returns_rowcount(
    database: Any, adapter: Any, table: sa.Table
) -> None:
    compiler = NativeCompiler(database.engine.dialect)
    async with database._autocommit_engine.connect() as conn:
        raw = await conn.get_raw_connection()
        await conn.execute(table.insert(), [{"name": "u"}, {"name": "u"}])
        stmt = table.update().where(table.c.name == "u").values(name="v")
        assert await adapter.execute(raw, compiler.bind(stmt)) == 2


async def check_execute_many_updates_all_rows(
    database: Any, adapter: Any, table: sa.Table
) -> None:
    compiler = NativeCompiler(database.engine.dialect)
    async with database._autocommit_engine.connect() as conn:
        raw = await conn.get_raw_connection()
        await conn.execute(table.insert(), [{"name": f"m{i}"} for i in range(3)])
        ids = [
            row["id"]
            for row in await sqlalchemy_rows(
                conn, table.select().where(table.c.name.like("m%"))
            )
        ]
        stmt = (
            table.update()
            .where(table.c.id == sa.bindparam("new_id"))
            .values(name=sa.bindparam("new_name"))
        )
        rows = [{"new_id": pk, "new_name": f"renamed-{pk}"} for pk in ids]
        await adapter.execute_many(raw, compiler.bind_many(stmt, rows))
        names = {
            row["name"]
            for row in await sqlalchemy_rows(
                conn, table.select().where(table.c.id.in_(ids))
            )
        }
        assert names == {f"renamed-{pk}" for pk in ids}


async def check_stream_yields_all_rows_in_order(
    database: Any, adapter: Any, table: sa.Table
) -> None:
    compiler = NativeCompiler(database.engine.dialect)
    async with database._autocommit_engine.connect() as conn:
        await conn.execute(table.insert(), [{"name": f"s{i:02d}"} for i in range(25)])
    async with database.transaction():
        conn = database.get_transaction_connection()
        await adapter.start_transaction(conn)
        raw = await conn.get_raw_connection()
        stmt = table.select().where(table.c.name.like("s%")).order_by(table.c.name)
        streamed = [row async for row in adapter.stream(raw, compiler.bind(stmt), 10)]
    assert [row["name"] for row in streamed] == [f"s{i:02d}" for i in range(25)]


async def check_concurrent_statements_on_transaction_connection(
    database: Any, adapter: Any, table: sa.Table
) -> None:
    compiler = NativeCompiler(database.engine.dialect)
    async with database.transaction():
        conn = database.get_transaction_connection()
        await adapter.start_transaction(conn)
        raw = await conn.get_raw_connection()
        await conn.execute(table.insert(), [{"name": f"c{i}"} for i in range(10)])
        results = await asyncio.gather(
            *[
                adapter.fetch_all(
                    raw, compiler.bind(table.select().where(table.c.name == f"c{i}"))
                )
                for i in range(10)
            ]
        )
    assert [rows[0]["name"] for rows in results] == [f"c{i}" for i in range(10)]
