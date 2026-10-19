"""
Native execution adapter for asyncpg.
"""

from typing import Any, AsyncGenerator, Dict, List

from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.ext.asyncio import AsyncConnection

from ormar.databases.native.adapters.base import (
    DriverAdapter,
    execute_mutex,
    first_value,
    inserted_primary_key,
)
from ormar.databases.native.compiler import BoundBatch, BoundStatement, row_to_dict


class AsyncpgAdapter(DriverAdapter):
    """
    Executes bound statements on an asyncpg connection. Statements always go
    through asyncpg's statement cache; ``prepare()`` is only called once per
    plan to read the result column type OIDs.
    """

    def __init__(self, dialect: Dialect) -> None:
        """
        :param dialect: dialect of the connected database
        :type dialect: Dialect
        """
        super().__init__(dialect)
        self.driver_errors = tuple(
            dialect.dbapi._asyncpg_error_translate  # type: ignore[union-attr]
        )

    async def fetch_all(self, raw: Any, bound: BoundStatement) -> List[Dict[str, Any]]:
        """
        Execute a statement and return all rows keyed by result label.

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :return: rows
        :rtype: List[Dict[str, Any]]
        """
        driver = raw.driver_connection
        async with execute_mutex(raw):
            await self.resolve_column_types(driver, bound)
            rows = await driver.fetch(bound.sql, *bound.args)
        return [row_to_dict(bound.plan, row) for row in rows]

    async def execute(self, raw: Any, bound: BoundStatement) -> Any:
        """
        Execute a write statement.

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :return: inserted primary key for INSERT, row count otherwise
        :rtype: Any
        """
        driver = raw.driver_connection
        async with execute_mutex(raw):
            if bound.plan.is_insert and bound.plan.returning:
                await self.resolve_column_types(driver, bound)
                row = await driver.fetchrow(bound.sql, *bound.args)
                return inserted_primary_key(bound, first_value(bound, row), None)
            status = await driver.execute(bound.sql, *bound.args)
        if bound.plan.is_insert:
            return inserted_primary_key(bound, None, None)
        return int(status.rsplit(" ", 1)[-1])

    async def execute_many(self, raw: Any, batch: BoundBatch) -> None:
        """
        Execute a statement once per set of arguments.

        :param raw: pooled raw connection
        :type raw: Any
        :param batch: statement and arguments per row
        :type batch: BoundBatch
        """
        async with execute_mutex(raw):
            await raw.driver_connection.executemany(batch.sql, batch.args_list)

    async def stream(
        self, raw: Any, bound: BoundStatement, batch_size: int
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream rows of a statement through a server-side cursor, fetching
        ``batch_size`` rows at a time. Requires an open transaction. The
        execution lock is held per fetch only, so other statements can run on
        the same connection between batches.

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :param batch_size: rows fetched per round trip
        :type batch_size: int
        :return: rows keyed by result label
        :rtype: AsyncGenerator[Dict[str, Any], None]
        """
        driver = raw.driver_connection
        async with execute_mutex(raw):
            await self.resolve_column_types(driver, bound)
            cursor = await driver.cursor(bound.sql, *bound.args)
        while True:
            async with execute_mutex(raw):
                rows = await cursor.fetch(batch_size)
            if not rows:
                return
            for row in rows:
                yield row_to_dict(bound.plan, row)

    async def start_transaction(self, sa_connection: AsyncConnection) -> None:
        """
        Start the asyncpg transaction SQLAlchemy only starts lazily on its own
        first statement, so natively executed statements run inside it.

        :param sa_connection: connection the transaction was begun on
        :type sa_connection: AsyncConnection
        """
        adapted = (await sa_connection.get_raw_connection()).dbapi_connection
        if not adapted._started:  # type: ignore[union-attr]
            await adapted._start_transaction()  # type: ignore[union-attr]

    def translate_error(
        self, error: BaseException, dbapi_connection: Any
    ) -> BaseException:
        """
        Translate an asyncpg exception into SQLAlchemy's DBAPI-level exception.

        :param error: exception raised by asyncpg
        :type error: BaseException
        :param dbapi_connection: SQLAlchemy's adapted asyncpg connection
        :type dbapi_connection: Any
        :return: DBAPI-level exception
        :rtype: BaseException
        """
        try:
            dbapi_connection._handle_exception(error)
        except Exception as translated:
            return translated
        return error  # pragma: no cover

    async def resolve_column_types(self, driver: Any, bound: BoundStatement) -> None:
        """
        Resolve the plan's result processors from the column type OIDs on its
        first execution.

        :param driver: asyncpg connection
        :type driver: Any
        :param bound: statement being executed
        :type bound: BoundStatement
        """
        if bound.plan.result_processors is None:
            prepared = await driver.prepare(bound.sql)
            self.resolve_processors(
                bound, [attr.type.oid for attr in prepared.get_attributes()]
            )
