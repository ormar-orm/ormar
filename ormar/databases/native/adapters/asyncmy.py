"""
Native execution adapter for asyncmy (``mysql+asyncmy://`` URLs).
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
    Runs bound statements on asyncmy cursors, which take ``%s``-style SQL and
    a tuple of arguments::

        await cursor.execute("SELECT users.id FROM users WHERE users.id = %s", (5,))

    Arguments are always passed as a tuple, even when empty. asyncmy only
    applies ``%`` formatting when arguments are given, and SQLAlchemy compiles
    a literal ``%`` as ``%%``, so ``LIKE 'a%%'`` would otherwise reach MySQL
    with the ``%%`` intact.
    """

    def __init__(self, dialect: Dialect) -> None:
        """
        :param dialect: dialect of the connected database
        :type dialect: Dialect
        """
        super().__init__(dialect=dialect)
        dbapi = dialect.dbapi
        # asyncmy raises AttributeError when its transport is gone mid-call;
        # SQLAlchemy's adapter turns it into a disconnect InternalError.
        self.driver_errors = (dbapi.Error, AttributeError)  # type: ignore[union-attr]
        self.unbuffered_cursor = dbapi.asyncmy.cursors.SSCursor  # type: ignore

    async def fetch_all(self, raw: Any, bound: BoundStatement) -> List[Dict[str, Any]]:
        """
        Run a statement on a cursor and return all rows keyed by result label;
        the result column types come from ``cursor.description``.

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :return: rows
        :rtype: List[Dict[str, Any]]
        """
        async with execute_mutex(raw=raw), raw.driver_connection.cursor() as cursor:
            await cursor.execute(query=bound.sql, args=tuple(bound.args))
            self.resolve_processors(
                bound=bound, coltypes=[desc[1] for desc in cursor.description]
            )
            rows = await cursor.fetchall()
        return [row_to_dict(plan=bound.plan, values=row) for row in rows]

    async def execute(self, raw: Any, bound: BoundStatement) -> Any:
        """
        Run a write statement. MySQL has no ``RETURNING``, so a generated primary
        key comes from ``cursor.lastrowid`` and the row count from
        ``cursor.rowcount``.

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :return: inserted primary key for INSERT, row count otherwise
        :rtype: Any
        """
        async with execute_mutex(raw=raw), raw.driver_connection.cursor() as cursor:
            await cursor.execute(query=bound.sql, args=tuple(bound.args))
            if bound.plan.is_insert:
                return inserted_primary_key(
                    bound=bound, returned=None, lastrowid=cursor.lastrowid
                )
            return cursor.rowcount

    async def execute_many(self, raw: Any, batch: BoundBatch) -> None:
        """
        Run a statement once per row with the cursor's ``executemany``.

        :param raw: pooled raw connection
        :type raw: Any
        :param batch: statement and arguments per row
        :type batch: BoundBatch
        """
        async with execute_mutex(raw=raw), raw.driver_connection.cursor() as cursor:
            await cursor.executemany(
                query=batch.sql, args=[tuple(a) for a in batch.args_list]
            )

    async def stream(
        self, raw: Any, bound: BoundStatement, batch_size: int
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Stream rows through an unbuffered cursor (``SSCursor``), which reads
        rows from the network as they are fetched instead of loading the whole
        result first. The cursor is always closed at the end, because MySQL
        cannot run another statement on the connection while it is open.

        The execution lock is held per fetch only (see the asyncpg adapter).

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :param batch_size: rows fetched per round trip
        :type batch_size: int
        :return: rows keyed by result label
        :rtype: AsyncGenerator[Dict[str, Any], None]
        """
        cursor = raw.driver_connection.cursor(cursor=self.unbuffered_cursor)
        try:
            async with execute_mutex(raw=raw):
                await cursor.execute(query=bound.sql, args=tuple(bound.args))
                self.resolve_processors(
                    bound=bound, coltypes=[desc[1] for desc in cursor.description]
                )
            while True:
                async with execute_mutex(raw=raw):
                    rows = await cursor.fetchmany(size=batch_size)
                if not rows:
                    return
                for row in rows:
                    yield row_to_dict(plan=bound.plan, values=row)
        finally:
            async with execute_mutex(raw=raw):
                await cursor.close()

    def translate_error(
        self, error: BaseException, dbapi_connection: Any
    ) -> BaseException:
        """
        Map an asyncmy exception the way SQLAlchemy's asyncmy wrapper does.
        asyncmy raises ``AttributeError`` when its connection is gone in the
        middle of a call; the wrapper turns it into ``InternalError`` so the
        dialect recognises a disconnect and drops the connection.

        :param error: exception raised by asyncmy
        :type error: BaseException
        :param dbapi_connection: SQLAlchemy's adapted asyncmy connection
        :type dbapi_connection: Any
        :return: DBAPI-level exception
        :rtype: BaseException
        """
        try:
            dbapi_connection._handle_exception(error=error)
        except Exception as translated:
            return translated
        return error  # pragma: no cover
