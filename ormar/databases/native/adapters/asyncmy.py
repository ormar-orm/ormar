"""
Native execution adapter for asyncmy.
"""

from typing import Any, AsyncGenerator, Dict, List

from sqlalchemy.engine.interfaces import Dialect

from ormar.databases.native.adapters.base import (
    DriverAdapter,
    execute_mutex,
    inserted_primary_key,
)
from ormar.databases.native.compiler import BoundBatch, BoundStatement, row_to_dict


class AsyncmyAdapter(DriverAdapter):
    """
    Executes bound statements on an asyncmy connection. Arguments are always
    passed as a tuple (even when empty), so ``%%`` escapes in compiled SQL are
    formatted the same way for every statement.
    """

    def __init__(self, dialect: Dialect) -> None:
        """
        :param dialect: dialect of the connected database
        :type dialect: Dialect
        """
        super().__init__(dialect)
        dbapi = dialect.dbapi
        # asyncmy raises AttributeError when its transport is gone mid-call;
        # SQLAlchemy's adapter turns it into a disconnect InternalError.
        self.driver_errors = (dbapi.Error, AttributeError)  # type: ignore[union-attr]
        self.unbuffered_cursor = dbapi.asyncmy.cursors.SSCursor  # type: ignore

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
        async with execute_mutex(raw), raw.driver_connection.cursor() as cursor:
            await cursor.execute(bound.sql, tuple(bound.args))
            self.resolve_processors(bound, [desc[1] for desc in cursor.description])
            rows = await cursor.fetchall()
        return [row_to_dict(bound.plan, row) for row in rows]

    async def execute(self, raw: Any, bound: BoundStatement) -> Any:
        """
        Execute a write statement. MySQL has no RETURNING, so generated primary
        keys come from ``lastrowid``.

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :return: inserted primary key for INSERT, row count otherwise
        :rtype: Any
        """
        async with execute_mutex(raw), raw.driver_connection.cursor() as cursor:
            await cursor.execute(bound.sql, tuple(bound.args))
            if bound.plan.is_insert:
                return inserted_primary_key(bound, None, cursor.lastrowid)
            return cursor.rowcount

    async def execute_many(self, raw: Any, batch: BoundBatch) -> None:
        """
        Execute a statement once per set of arguments.

        :param raw: pooled raw connection
        :type raw: Any
        :param batch: statement and arguments per row
        :type batch: BoundBatch
        """
        async with execute_mutex(raw), raw.driver_connection.cursor() as cursor:
            await cursor.executemany(batch.sql, [tuple(a) for a in batch.args_list])

    async def stream(
        self, raw: Any, bound: BoundStatement, batch_size: int
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream rows through an unbuffered cursor, fetching ``batch_size`` rows
        at a time. The execution lock is held per fetch only.

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :param batch_size: rows fetched per round trip
        :type batch_size: int
        :return: rows keyed by result label
        :rtype: AsyncGenerator[Dict[str, Any], None]
        """
        cursor = raw.driver_connection.cursor(self.unbuffered_cursor)
        try:
            async with execute_mutex(raw):
                await cursor.execute(bound.sql, tuple(bound.args))
                self.resolve_processors(bound, [desc[1] for desc in cursor.description])
            while True:
                async with execute_mutex(raw):
                    rows = await cursor.fetchmany(batch_size)
                if not rows:
                    return
                for row in rows:
                    yield row_to_dict(bound.plan, row)
        finally:
            async with execute_mutex(raw):
                await cursor.close()

    def translate_error(
        self, error: BaseException, dbapi_connection: Any
    ) -> BaseException:
        """
        Translate an asyncmy exception the way SQLAlchemy's asyncmy adapter
        does (``AttributeError`` becomes a disconnect ``InternalError``).

        :param error: exception raised by asyncmy
        :type error: BaseException
        :param dbapi_connection: SQLAlchemy's adapted asyncmy connection
        :type dbapi_connection: Any
        :return: DBAPI-level exception
        :rtype: BaseException
        """
        try:
            dbapi_connection._handle_exception(error)
        except Exception as translated:
            return translated
        return error  # pragma: no cover
