"""
Common interface of the per-driver adapters that run bound statements
directly on a native async driver connection.

Each driver has its own API (asyncpg ``fetch``/``execute`` with ``$1``
placeholders, asyncmy and aiosqlite DBAPI-style cursors), so one adapter per
driver hides those differences behind ``fetch_all``, ``execute``,
``execute_many`` and ``stream``. The executor only talks to this interface.
"""

import abc
import contextlib
from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
)

from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.ext.asyncio import AsyncConnection

from ormar.databases.native.compiler import (
    BoundBatch,
    BoundStatement,
    resolve_result_processors,
    row_to_dict,
)

# rows fetched per round trip by ``QuerySet.iterate()``
ITERATE_BATCH_SIZE = 100


class DriverAdapter(abc.ABC):
    """
    Runs bound statements on one driver and returns results in the shape the
    SQLAlchemy path returns them (rows keyed by label, inserted pk, row count).

    Every method takes ``raw``, the pooled connection that SQLAlchemy's
    connection is using, so native statements share its transaction. It
    exposes two objects::

        # the driver's own connection, e.g. an asyncpg Connection
        raw.driver_connection
        # SQLAlchemy's wrapper around it, with the execution lock and the
        # mapping of driver errors
        raw.dbapi_connection

    ``driver_errors`` lists the exceptions the driver raises, so the executor
    knows which ones to translate into SQLAlchemy exceptions.
    """

    driver_errors: Tuple[Type[BaseException], ...]

    def __init__(self, dialect: Dialect) -> None:
        """
        :param dialect: dialect of the connected database
        :type dialect: Dialect
        """
        self.dialect = dialect

    @abc.abstractmethod
    async def fetch_all(self, raw: Any, bound: BoundStatement) -> List[Dict[str, Any]]:
        """
        Run a statement and return all its rows keyed by result label::

            fetch_all(raw, bind(select(users.c.id, users.c.name)))
            # -> [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :return: rows
        :rtype: List[Dict[str, Any]]
        """

    @abc.abstractmethod
    async def execute(self, raw: Any, bound: BoundStatement) -> Any:
        """
        Run an INSERT, UPDATE or DELETE and return what SQLAlchemy's path would:
        the primary key of an inserted row, otherwise the number of rows changed::

            execute(raw, bind(users.insert().values(name="a")))  # -> 7 (new id)
            execute(raw, bind(users.delete()))  # -> 3 (rows deleted)

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :return: inserted primary key for INSERT, row count otherwise
        :rtype: Any
        """

    @abc.abstractmethod
    async def execute_many(self, raw: Any, batch: BoundBatch) -> None:
        """
        Run one statement once per row of arguments with the driver's
        ``executemany``, used by ``bulk_update``::

            stmt = users.update().where(users.c.id == bindparam("new_id"))
            stmt = stmt.values(name=bindparam("new_name"))
            rows = [{"new_id": 1, "new_name": "a"}, {"new_id": 2, "new_name": "b"}]
            execute_many(raw, bind_many(stmt, rows))

        :param raw: pooled raw connection
        :type raw: Any
        :param batch: statement and arguments per row
        :type batch: BoundBatch
        """

    @abc.abstractmethod
    def stream(
        self, raw: Any, bound: BoundStatement, batch_size: int
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Yield the rows of a statement one by one while fetching ``batch_size``
        rows per round trip, so ``QuerySet.iterate()`` never holds the whole
        result in memory.

        :param raw: pooled raw connection
        :type raw: Any
        :param bound: statement to execute
        :type bound: BoundStatement
        :param batch_size: rows fetched per round trip
        :type batch_size: int
        :return: rows keyed by result label
        :rtype: AsyncGenerator[Dict[str, Any], None]
        """

    async def start_transaction(self, sa_connection: AsyncConnection) -> None:
        """
        Make sure the transaction SQLAlchemy began is really open on the driver
        connection before native statements run on it.

        Some SQLAlchemy adapters only send ``BEGIN`` lazily with their own first
        statement; native statements bypass that, so without this they would run
        outside the transaction. No-op for drivers that start it on their own.

        :param sa_connection: connection the transaction was begun on
        :type sa_connection: AsyncConnection
        """

    def translate_error(
        self, error: BaseException, dbapi_connection: Any
    ) -> BaseException:
        """
        Return the exception SQLAlchemy's driver wrapper would raise for this
        driver error, which ``sqlalchemy.exc.DBAPIError.instance`` then wraps.

        SQLAlchemy maps driver exceptions to standard DBAPI classes before
        wrapping them, so users catch the same exception on both paths::

            asyncpg.UniqueViolationError -> IntegrityError
            # -> raised to the user as sqlalchemy.exc.IntegrityError

        Drivers that already raise DBAPI classes return the error unchanged.

        :param error: exception raised by the driver
        :type error: BaseException
        :param dbapi_connection: SQLAlchemy's adapted DBAPI connection
        :type dbapi_connection: Any
        :return: DBAPI-level exception
        :rtype: BaseException
        """
        return error

    def resolve_processors(self, bound: BoundStatement, coltypes: List[Any]) -> None:
        """
        Look up the plan's result processors on its first execution, once the
        driver has reported the result column types (see
        ``resolve_result_processors`` for what they are). Later executions reuse
        them.

        :param bound: executed statement
        :type bound: BoundStatement
        :param coltypes: driver-reported column types
        :type coltypes: List[Any]
        """
        if bound.plan.result_processors is None:
            resolve_result_processors(
                plan=bound.plan, dialect=self.dialect, coltypes=coltypes
            )


def inserted_primary_key(
    bound: BoundStatement, returned: Any, lastrowid: Optional[int]
) -> Any:
    """
    Return the primary key of an executed INSERT, checked in the order
    SQLAlchemy's ``inserted_primary_key`` uses, so ``save()`` gets the same pk
    on both paths:

    1. the pk the caller bound: ``insert().values(id=3)`` -> 3
    2. the value of ``RETURNING users.id`` (PostgreSQL, SQLite)
    3. the driver's ``lastrowid`` (MySQL auto increment)

    :param bound: executed insert
    :type bound: BoundStatement
    :param returned: first column of the RETURNING row, if any
    :type returned: Any
    :param lastrowid: driver's last inserted row id
    :type lastrowid: Optional[int]
    :return: primary key or None when it cannot be recovered
    :rtype: Any
    """
    key = bound.plan.pk_param_key
    if key is not None and bound.params.get(key) is not None:
        return bound.params[key]
    if bound.plan.returning:
        return returned
    return lastrowid or None


def first_value(bound: BoundStatement, row: Any) -> Any:
    """
    Return the first column of a ``RETURNING`` row (the inserted primary key),
    converted by its result processor, e.g. a UUID primary key generated by
    the database comes back as ``uuid.UUID`` and not as the driver's value.

    :param bound: executed statement, with resolved result processors
    :type bound: BoundStatement
    :param row: returned row, or None when no row came back
    :type row: Any
    :return: converted first value, or None
    :rtype: Any
    """
    if row is None:
        return None
    return next(iter(row_to_dict(plan=bound.plan, values=row).values()))


def execute_mutex(raw: Any) -> AsyncContextManager[Any]:
    """
    Return the lock SQLAlchemy's driver wrapper uses to run one statement at a
    time on a connection, or a no-op when the wrapper has none.

    asyncpg and asyncmy fail when two statements run on one connection at
    once (e.g. two tasks sharing a transaction with ``asyncio.gather``).
    Taking the same lock as SQLAlchemy serializes native statements with each
    other and with statements SQLAlchemy runs::

        async with execute_mutex(raw):
            rows = await raw.driver_connection.fetch(sql, *args)

    aiosqlite has no lock; it runs calls one by one on its own thread.

    :param raw: pooled raw connection
    :type raw: Any
    :return: async lock or a no-op context manager
    :rtype: AsyncContextManager[Any]
    """
    mutex = getattr(raw.dbapi_connection, "_execute_mutex", None)
    return mutex if mutex is not None else contextlib.nullcontext()
