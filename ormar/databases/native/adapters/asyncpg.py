"""
Native execution adapter for asyncpg (``postgresql+asyncpg://`` URLs).
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
    Runs bound statements with asyncpg's own API, which takes ``$1``-style SQL
    and positional arguments::

        await driver.fetch("SELECT users.id FROM users WHERE users.id = $1", 5)

    asyncpg prepares and caches statements by SQL text on its own. It does
    not report result column types on ``fetch``, so ``prepare()`` is called
    once per plan only to read them (see ``resolve_column_types``).

    asyncpg's ``executemany`` runs in an implicit transaction when none is
    open, so a failing row leaves no rows inserted (``atomic_execute_many``).
    """

    atomic_execute_many = True

    def __init__(self, dialect: Dialect) -> None:
        """
        :param dialect: dialect of the connected database
        :type dialect: Dialect
        """
        super().__init__(dialect=dialect)
        self.driver_errors = tuple(
            dialect.dbapi._asyncpg_error_translate  # type: ignore[union-attr]
        )

    async def fetch_all(self, raw: Any, bound: BoundStatement) -> List[Dict[str, Any]]:
        """
        Run a statement with ``fetch`` and turn asyncpg's ``Record`` rows into
        rows keyed by result label.

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :return: rows
        :rtype: List[Dict[str, Any]]
        """
        driver = raw.driver_connection
        async with execute_mutex(raw=raw):
            await self.resolve_column_types(driver=driver, bound=bound)
            rows = await driver.fetch(bound.sql, *bound.args)
        return [row_to_dict(plan=bound.plan, values=row) for row in rows]

    async def execute(self, raw: Any, bound: BoundStatement) -> Any:
        """
        Run a write statement. An INSERT with ``RETURNING`` reads the returned pk
        with ``fetchrow``; other statements use ``execute``, whose status string
        carries the row count::

            await driver.execute("DELETE FROM users")  # -> "DELETE 3" -> 3

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :return: inserted primary key for INSERT, row count otherwise
        :rtype: Any
        """
        driver = raw.driver_connection
        async with execute_mutex(raw=raw):
            if bound.plan.is_insert and bound.plan.returning:
                await self.resolve_column_types(driver=driver, bound=bound)
                row = await driver.fetchrow(bound.sql, *bound.args)
                return inserted_primary_key(
                    bound=bound,
                    returned=first_value(bound=bound, row=row),
                    lastrowid=None,
                )
            status = await driver.execute(bound.sql, *bound.args)
        if bound.plan.is_insert:
            return inserted_primary_key(bound=bound, returned=None, lastrowid=None)
        return int(status.rsplit(" ", 1)[-1])

    async def execute_many(self, raw: Any, batch: BoundBatch) -> None:
        """
        Run a statement once per row with asyncpg's ``executemany``, which sends
        all rows in one pipelined call and is atomic on its own.

        :param raw: pooled raw connection
        :type raw: Any
        :param batch: statement and arguments per row
        :type batch: BoundBatch
        """
        async with execute_mutex(raw=raw):
            await raw.driver_connection.executemany(
                command=batch.sql, args=batch.args_list
            )

    async def stream(
        self, raw: Any, bound: BoundStatement, batch_size: int
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream rows through a PostgreSQL server-side cursor, fetching
        ``batch_size`` rows at a time; PostgreSQL only allows such cursors
        inside a transaction, which ``QuerySet.iterate()`` opens.

        The execution lock is held per fetch only, so a query issued inside
        the ``async for`` loop on the same connection can run between
        batches instead of waiting forever for the lock.

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
        async with execute_mutex(raw=raw):
            await self.resolve_column_types(driver=driver, bound=bound)
            cursor = await driver.cursor(bound.sql, *bound.args)
        while True:
            async with execute_mutex(raw=raw):
                rows = await cursor.fetch(n=batch_size)
            if not rows:
                return
            for row in rows:
                yield row_to_dict(plan=bound.plan, values=row)

    async def start_transaction(self, sa_connection: AsyncConnection) -> None:
        """
        Send ``BEGIN`` now for a transaction SQLAlchemy began.

        SQLAlchemy's asyncpg wrapper only marks the transaction as begun and
        sends ``BEGIN`` with its own first statement (``_started`` flag).
        Native statements skip the wrapper, so without this they would run
        and commit on their own, outside the transaction.

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
        Map an asyncpg exception to the DBAPI class SQLAlchemy's asyncpg
        wrapper uses, e.g. ``UniqueViolationError`` to its ``IntegrityError``.
        The wrapper's ``_handle_exception`` raises the mapped exception, so it
        is caught and returned.

        :param error: exception raised by asyncpg
        :type error: BaseException
        :param dbapi_connection: SQLAlchemy's adapted asyncpg connection
        :type dbapi_connection: Any
        :return: DBAPI-level exception
        :rtype: BaseException
        """
        try:
            dbapi_connection._handle_exception(error=error)
        except Exception as translated:
            return translated
        return error  # pragma: no cover

    async def resolve_column_types(self, driver: Any, bound: BoundStatement) -> None:
        """
        On a plan's first execution, read its result column types from
        ``prepare()`` and look up its result processors.

        asyncpg reports types as PostgreSQL type OIDs (e.g. ``1700`` for
        ``NUMERIC``), the same codes SQLAlchemy's own path passes to the
        types, so both paths pick the same processors.

        :param driver: asyncpg connection
        :type driver: Any
        :param bound: statement being executed
        :type bound: BoundStatement
        """
        if bound.plan.result_processors is None:
            prepared = await driver.prepare(query=bound.sql)
            self.resolve_processors(
                bound=bound,
                coltypes=[attr.type.oid for attr in prepared.get_attributes()],
            )
