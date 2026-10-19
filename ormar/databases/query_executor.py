"""
QueryExecutor module - executes database queries natively on the driver when a
native backend is available, otherwise through the SQLAlchemy async API.
"""

import contextlib
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    List,
    Mapping,
    NoReturn,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

from sqlalchemy import text, util
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import Executable
from sqlalchemy.sql.compiler import SQLCompiler

from ormar.databases.native.adapters.base import ITERATE_BATCH_SIZE
from ormar.databases.native.backend import NativeBackend
from ormar.databases.native.compiler import (
    BoundBatch,
    BoundStatement,
    is_plannable,
    pk_param_key,
)
from ormar.databases.native.errors import raise_translated

Payload = TypeVar("Payload", BoundStatement, BoundBatch)
Result = TypeVar("Result")


class QueryExecutor:
    """
    Executes database queries. With a native backend, statements run directly on
    the driver connection underneath the SQLAlchemy connection; statements the
    backend cannot run (and every statement without a backend) go through the
    SQLAlchemy async API. Provides a databases-compatible interface.
    """

    def __init__(
        self, connection: AsyncConnection, native: Optional[NativeBackend] = None
    ) -> None:
        """
        Initialize query executor.

        :param connection: SQLAlchemy async connection
        :param native: native backend of the database, if the driver has one
        """
        self._connection = connection
        self._native = native
        self._raw: Any = None

    async def fetch_all(
        self, query: Executable, params: Optional[Mapping[str, Any]] = None
    ) -> List[Any]:
        """
        Execute a query and fetch all rows.

        :param query: SQLAlchemy query expression
        :param params: values for named bind parameters of the query
        :return: List of rows, accessible by result label
        """
        bound = self._bind(query, params)
        if bound is not None:
            return await self._fetch_native(bound)
        result: CursorResult[Any] = await self._connection.execute(query, params)
        return list(result.mappings().all())

    async def fetch_one(
        self, query: Executable, params: Optional[Mapping[str, Any]] = None
    ) -> Optional[Mapping[Any, Any]]:
        """
        Execute a query and fetch one row.

        :param query: SQLAlchemy query expression
        :param params: values for named bind parameters of the query
        :return: Single row or None
        """
        bound = self._bind(query, params)
        if bound is not None:
            rows = await self._fetch_native(bound)
            return rows[0] if rows else None
        result: CursorResult[Any] = await self._connection.execute(query, params)
        row = result.mappings().first()
        return row

    async def fetch_val(
        self,
        query: Executable,
        column: int = 0,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Any]:
        """
        Execute a query and fetch a single scalar value.

        :param query: SQLAlchemy query expression
        :param column: Column index to fetch (default 0)
        :param params: values for named bind parameters of the query
        :return: Scalar value or None
        """
        bound = self._bind(query, params)
        if bound is not None:
            rows = await self._fetch_native(bound)
            return list(rows[0].values())[column] if rows else None
        result: CursorResult[Any] = await self._connection.execute(query, params)
        return result.scalar()

    async def execute(
        self, query: Executable, params: Optional[Mapping[str, Any]] = None
    ) -> Any:
        """
        Execute a query (INSERT, UPDATE, DELETE).

        :param query: SQLAlchemy query expression
        :param params: values for named bind parameters of the query
        :return: For INSERT, the inserted primary key or ``None`` if the backend
            cannot return one (e.g. Oracle MySQL inserting into a
            non-AUTO_INCREMENT pk with a server default — no RETURNING support).
            For UPDATE/DELETE, the row count.
        """
        bound = self._bind(query, params)
        if bound is not None:
            assert self._native is not None
            return await self._run(self._native.adapter.execute, bound, bound.args)

        result: CursorResult[Any] = await self._connection.execute(query, params)

        # For INSERT queries, try to get the inserted primary key via the
        # dialect's best-available mechanism (RETURNING on PostgreSQL / SQLite
        # 3.35+ / MariaDB 10.5+, LAST_INSERT_ID() on MySQL AUTO_INCREMENT).
        # Do NOT fall back to rowcount here: rowcount is not a pk, and
        # returning it would silently corrupt `Model.pk` in `save()`.
        if result.context and result.context.isinsert:
            # a primary key bound by name (statement templates) is returned as
            # bound; SQLAlchemy only looks it up under the column key
            compiled = cast(SQLCompiler, result.context.compiled)
            key = pk_param_key(compiled) if params else None
            if key is not None and params and params.get(key) is not None:
                return params[key]
            if result.inserted_primary_key:
                pk_value = result.inserted_primary_key[0]
                if pk_value is not None:
                    return pk_value

            if hasattr(result, "lastrowid") and result.lastrowid:  # pragma: no cover
                return result.lastrowid

            return None  # pragma: no cover

        return result.rowcount if result.rowcount is not None else 0

    async def execute_many(
        self, query: Union[Executable, str], values: Sequence[Mapping[str, Any]]
    ) -> None:
        """
        Execute a query multiple times with different parameter sets.

        :param query: SQLAlchemy query expression or SQL string
        :param values: Sequence of parameter mappings
        """
        # SQLAlchemy runs a statement given no parameter sets once; leave
        # that case to it so both paths behave the same.
        if self._native is not None and values and is_plannable(query):
            batch = self._native.compiler.bind_many(query, values)
            if not batch.plan.fallback:
                await self._run(
                    self._native.adapter.execute_many, batch, batch.args_list
                )
                return
        exec_query = text(query) if isinstance(query, str) else query
        await self._connection.execute(exec_query, values)

    async def iterate(
        self, query: Executable, params: Optional[Mapping[str, Any]] = None
    ) -> AsyncIterator[Any]:
        """
        Execute a query and iterate over results.

        :param query: SQLAlchemy query expression
        :param params: values for named bind parameters of the query
        :return: Async iterator of rows, accessible by result label
        """
        bound = self._bind(query, params)
        if bound is not None:
            assert self._native is not None
            raw = await self._raw_connection()
            rows = self._native.adapter.stream(raw, bound, ITERATE_BATCH_SIZE)
            async with contextlib.aclosing(rows):
                try:
                    async for row in rows:
                        yield row
                except self._native.adapter.driver_errors as error:
                    await self._translate(error, bound.sql, bound.args)
                except GeneratorExit:
                    raise
                except BaseException as error:
                    await self._reraise_interrupted(error)
            return
        async with self._connection.stream(query, params) as result:
            async for mapping in result.mappings():
                yield mapping

    async def _fetch_native(self, bound: BoundStatement) -> List[Any]:
        """
        Fetch all rows of a bound statement on the driver.

        :param bound: bound statement
        :return: rows keyed by result label
        """
        assert self._native is not None
        return await self._run(self._native.adapter.fetch_all, bound, bound.args)

    def _bind(
        self, query: Executable, params: Optional[Mapping[str, Any]] = None
    ) -> Optional[BoundStatement]:
        """
        Bind the query for native execution.

        :param query: SQLAlchemy query expression
        :param params: values for named bind parameters of the query
        :return: bound statement, or None when it must run through SQLAlchemy
        """
        if self._native is None or not is_plannable(query):
            return None
        bound = self._native.compiler.bind(query, params)
        return None if bound.plan.fallback else bound

    async def _raw_connection(self) -> Any:
        """
        Return the pooled raw connection under the SQLAlchemy connection.

        :return: pooled raw connection
        """
        if self._raw is None:
            # an established connection already holds its pooled connection;
            # only a lazy, not yet connected one needs SQLAlchemy's greenlet
            sync_connection = self._connection.sync_connection
            if sync_connection is not None and (
                sync_connection._dbapi_connection is not None
            ):
                self._raw = sync_connection._dbapi_connection
            else:
                self._raw = await self._connection.get_raw_connection()
        return self._raw

    async def _run(
        self,
        method: Callable[[Any, Payload], Awaitable[Result]],
        payload: Payload,
        args: Sequence[Any],
    ) -> Result:
        """
        Run an adapter method, translating driver errors to SQLAlchemy ones.

        :param method: adapter method to run
        :param payload: bound statement or batch
        :param args: driver arguments, reported in a translated error
        :return: result of the adapter method
        """
        assert self._native is not None
        raw = await self._raw_connection()
        try:
            return await method(raw, payload)
        except self._native.adapter.driver_errors as error:
            await self._translate(
                error, payload.sql, args, ismulti=isinstance(payload, BoundBatch)
            )
            raise  # pragma: no cover
        except BaseException as error:
            await self._reraise_interrupted(error)

    async def _reraise_interrupted(self, error: BaseException) -> NoReturn:
        """
        Re-raise an exception from a native call, first invalidating the
        connection when the call was interrupted (task cancellation,
        KeyboardInterrupt, ...), as SQLAlchemy does, so a connection left
        mid-statement never returns to the pool.

        :param error: exception raised during the call
        """
        if util.is_exit_exception(error):
            await self._connection.invalidate()
        raise error

    async def _translate(
        self,
        error: BaseException,
        sql: str,
        args: Sequence[Any],
        ismulti: bool = False,
    ) -> None:
        """
        Re-raise a driver error as the matching SQLAlchemy exception.

        :param error: exception raised by the driver
        :param sql: executed SQL
        :param args: driver arguments
        :param ismulti: whether the statement ran once per row of a batch
        """
        assert self._native is not None
        await raise_translated(
            error,
            backend=self._native,
            sa_connection=self._connection,
            sql=sql,
            args=args,
            ismulti=ismulti,
        )
