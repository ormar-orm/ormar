"""
DatabaseConnection module - provides async database connection management.
"""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Optional

from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from ormar.databases.native.adapters import select_adapter
from ormar.databases.native.backend import NativeBackend
from ormar.databases.native.compiler import NativeCompiler
from ormar.databases.query_executor import QueryExecutor
from ormar.databases.transaction import Transaction
from ormar.fields.parsers import deserialize_json_column, serialize_json_column

_transaction_connection: ContextVar[Optional[AsyncConnection]] = ContextVar(
    "_transaction_connection", default=None
)

# Backends where switching a pooled connection into AUTOCOMMIT and back costs
# extra round trips on every checkout (MySQL: SET AUTOCOMMIT, SET SESSION
# TRANSACTION ISOLATION LEVEL, COMMIT; aiosqlite: a hop to the driver thread
# per switch). Their engine runs in AUTOCOMMIT instead and only transactional
# connections leave it (see ``Transaction``).
AUTOCOMMIT_ENGINE_BACKENDS = frozenset({"mysql", "mariadb", "sqlite"})


class DatabaseConnection:
    """
    Wrapper around SQLAlchemy AsyncEngine that provides a databases-compatible API.
    """

    def __init__(self, url: str, native_execution: bool = True, **options: Any) -> None:
        """
        Initialize database connection.

        :param url: Database URL with an async driver (e.g., postgresql+asyncpg://)
        :param native_execution: run statements directly on the driver for
            asyncpg, asyncmy and aiosqlite instead of through SQLAlchemy's
            execution layer
        :param options: Additional engine options passed to
            ``create_async_engine``; ``pool_size`` defaults to 10 and
            ``max_overflow`` to 10. ``min_connections`` (default
            ``pool_size``) opens that many pooled connections on
            ``connect()`` (at most ``pool_size``), so the first concurrent
            queries do not wait for new connections; 0 keeps the pool lazy.
            ``json_serializer`` and ``json_deserializer`` of JSON columns
            default to orjson.
        """
        self._force_rollback = options.pop("force_rollback", False)
        self._url = url
        # Set reasonable pool defaults if not provided; overflow connections are
        # closed on checkin, so pool_size should cover the usual concurrency
        if "pool_size" not in options:
            options["pool_size"] = 10
        if "max_overflow" not in options:
            options["max_overflow"] = 10
        options.setdefault("json_serializer", serialize_json_column)
        options.setdefault("json_deserializer", deserialize_json_column)
        self._min_connections = options.pop("min_connections", options["pool_size"])
        self._options = options
        self._engine: Optional[AsyncEngine] = None
        self._autocommit_engine: Optional[AsyncEngine] = None
        self._autocommit_by_default = (
            "isolation_level" not in options
            and make_url(url).get_backend_name() in AUTOCOMMIT_ENGINE_BACKENDS
        )

        self._global_transaction: Optional[Transaction] = None
        self._native_execution = native_execution
        self.native_backend: Optional[NativeBackend] = None

    async def connect(self) -> None:
        """Connect to the database by creating the async engine."""
        if self._engine is None:
            engine_options = (
                {**self._options, "isolation_level": "AUTOCOMMIT"}
                if self._autocommit_by_default
                else self._options
            )
            self._engine = create_async_engine(self._url, **engine_options)
            # Engine (or a view of the same engine/pool) in AUTOCOMMIT mode.
            # Standalone queries use this to avoid a BEGIN/COMMIT round-trip
            # per call, matching legacy `databases`-library semantics. Explicit
            # Transactions keep using ``self._engine`` so BEGIN / SAVEPOINT
            # still work.
            self._autocommit_engine = (
                self._engine
                if self._autocommit_by_default
                else self._engine.execution_options(isolation_level="AUTOCOMMIT")
            )

            self.native_backend = build_native_backend(
                dialect=self._engine.dialect, enabled=self._native_execution
            )

            # Set up SQLite foreign keys pragma if using SQLite
            if self._engine.dialect.name == "sqlite":  # pragma: nocover

                @event.listens_for(self._engine.sync_engine, "connect")
                def set_sqlite_pragma(dbapi_conn: Any, connection_record: Any) -> None:
                    cursor = dbapi_conn.cursor()
                    cursor.execute("PRAGMA foreign_keys=ON")
                    cursor.close()

            await self._open_min_connections()

            if self._force_rollback:
                assert self._global_transaction is None
                self._global_transaction = Transaction(
                    self, force_rollback=self._force_rollback
                )
                await self._global_transaction.__aenter__()

    async def _open_min_connections(self) -> None:
        """
        Open ``min_connections`` pooled connections at once and return them to
        the pool, capped at the pool size (connections beyond it are closed
        when returned).
        """
        assert self._engine is not None
        count = min(self._min_connections, self._options["pool_size"])
        if count > 0:
            async with AsyncExitStack() as stack:
                await asyncio.gather(
                    *(
                        stack.enter_async_context(self._engine.connect())
                        for _ in range(count)
                    )
                )

    async def disconnect(self) -> None:
        """
        Disconnect from the database and dispose of the engine.
        In case of force_rollback, also roll back the global transaction.
        """
        if self._engine is not None:
            if self._force_rollback:
                assert self._global_transaction is not None
                await self._global_transaction.__aexit__()
                self._global_transaction = None

            await self._engine.dispose()
            self._engine = None
            self._autocommit_engine = None
            self.native_backend = None

    @property
    def is_connected(self) -> bool:
        """Check if the engine is connected."""
        return self._engine is not None

    @property
    def engine(self) -> AsyncEngine:
        """Get the async engine."""
        assert self._engine is not None, "DatabaseConnection not connected"
        return self._engine

    @property
    def dialect(self) -> Any:
        """Get the database dialect."""
        return self.engine.dialect

    @property
    def url(self) -> str:
        """Get the database URL."""
        return self._url

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[AsyncConnection]:
        """
        Get a connection from the pool.
        If inside a transaction, returns the transaction connection.
        """
        trans_conn = _transaction_connection.get()
        if trans_conn is not None:
            yield trans_conn
        else:
            async with self.engine.connect() as conn:
                await self.restore_transaction_isolation(conn)
                yield conn

    async def restore_transaction_isolation(self, conn: AsyncConnection) -> None:
        """
        Switch a freshly checked out connection to the server's default isolation
        level when the engine runs in AUTOCOMMIT, so explicit transactions get
        a real BEGIN / COMMIT. The pool resets it back to AUTOCOMMIT on checkin.

        :param conn: connection checked out from the main engine
        :type conn: AsyncConnection
        """
        if self._autocommit_by_default:  # pragma: no cover
            await conn.execution_options(
                isolation_level=conn.dialect.default_isolation_level
            )

    def transaction(self, force_rollback: bool = False) -> Transaction:
        """
        Create a transaction context manager.

        :param force_rollback: If True, always rollback (used for testing)
        """
        return Transaction(self, force_rollback=force_rollback)

    @asynccontextmanager
    async def get_query_executor(
        self, *, transactional: bool = False
    ) -> AsyncIterator[QueryExecutor]:
        """
        Get connection, reusing transaction connection if in transaction.

        :param transactional: If True, open an explicit transaction for the
            scope of the executor. Use for server-side cursors (``iterate``)
            and multi-row ``execute_many`` batches, which need a real
            transaction either because AUTOCOMMIT would commit each row
            separately or because the driver (asyncpg) requires a
            transaction for streaming.
        :type transactional: bool
        :return: QueryExecutor wrapping a connection
        :rtype: QueryExecutor
        """
        trans_conn = self.get_transaction_connection()
        if trans_conn is not None:
            # Inside a transaction - reuse the transaction's connection
            yield QueryExecutor(connection=trans_conn, native=self.native_backend)
        elif transactional:
            async with self.transaction():
                conn = self.get_transaction_connection()
                assert conn is not None
                yield QueryExecutor(connection=conn, native=self.native_backend)
        else:
            # Outside transaction: use AUTOCOMMIT view so each statement
            # commits at the driver level with no extra BEGIN/COMMIT
            # round-trip.
            assert self._autocommit_engine is not None
            async with self._autocommit_engine.connect() as conn:
                yield QueryExecutor(connection=conn, native=self.native_backend)

    @property
    def atomic_execute_many(self) -> bool:
        """
        Whether ``execute_many`` outside a transaction is atomic on its own
        (native asyncpg), so a single batch needs no explicit transaction.

        ``bulk_create`` uses it to skip ``BEGIN``/``COMMIT`` around a lone
        ``executemany``: on asyncpg a failing row already leaves no rows
        inserted. Other drivers give no such guarantee, so a batch there
        still runs in a transaction.

        :return: True when a lone batch needs no transaction
        :rtype: bool
        """
        return (
            self.native_backend is not None
            and self.native_backend.adapter.atomic_execute_many
        )

    async def start_driver_transaction(self, connection: AsyncConnection) -> None:
        """
        Make sure the driver-level transaction is open on a connection
        SQLAlchemy began a transaction on, so natively executed statements
        run inside it.

        :param connection: connection with a begun transaction
        :type connection: AsyncConnection
        """
        if self.native_backend is not None:
            await self.native_backend.adapter.start_transaction(
                sa_connection=connection
            )

    def get_transaction_connection(self) -> Optional[AsyncConnection]:
        """Get the current transaction connection if in a transaction."""
        return _transaction_connection.get()

    def set_transaction_connection(self, conn: Optional[AsyncConnection]) -> None:
        """Set the current transaction connection."""
        _transaction_connection.set(conn)

    async def __aenter__(self) -> "DatabaseConnection":
        """Async context manager entry - connect to a database."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit - disconnect from database."""
        await self.disconnect()


def build_native_backend(dialect: Dialect, enabled: bool) -> Optional[NativeBackend]:
    """
    Build the native backend for the dialect's driver.

    :param dialect: dialect of the connected database
    :type dialect: Dialect
    :param enabled: whether native execution is enabled
    :type enabled: bool
    :return: native backend, or None when disabled or the driver has no adapter
    :rtype: Optional[NativeBackend]
    """
    adapter = select_adapter(dialect=dialect) if enabled else None
    if adapter is None:
        return None
    return NativeBackend(
        dialect=dialect, adapter=adapter, compiler=NativeCompiler(dialect=dialect)
    )
