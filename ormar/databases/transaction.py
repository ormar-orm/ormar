"""
Transaction module - provides transaction management with savepoint support.
"""

from contextvars import ContextVar
from types import TracebackType
from typing import TYPE_CHECKING, Optional, Type

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncTransaction

if TYPE_CHECKING:  # pragma: no cover
    from ormar.databases.connection import DatabaseConnection

_transaction_depth: ContextVar[int] = ContextVar("_transaction_depth", default=0)


class Transaction:
    """
    Transaction context manager with support for nested transactions via savepoints.
    """

    def __init__(
        self,
        database: "DatabaseConnection",
        force_rollback: bool = False,
    ) -> None:
        """
        Initialize transaction.

        :param database: DatabaseConnection instance
        :param force_rollback: If True, always rollback (used for testing)
        """
        self._database = database
        self._force_rollback = force_rollback
        self._connection: Optional[AsyncConnection] = None
        self._transaction: Optional[AsyncTransaction] = None
        self._depth: int = 0

    async def __aenter__(self) -> "Transaction":
        """Enter transaction context."""
        self._depth = _transaction_depth.get()

        # If this is the outermost transaction, get a new connection.
        # This uses the main engine (not the AUTOCOMMIT view used by
        # standalone queries) with its default isolation restored, so
        # ``connection.begin()`` and the nested ``begin_nested()`` savepoints
        # below both work.
        if self._depth == 0:
            self._connection = await self._database.engine.connect().__aenter__()
            await self._database.restore_transaction_isolation(self._connection)
            self._database.set_transaction_connection(self._connection)
            self._transaction = await self._connection.begin()
            # SQLite requires an explicit BEGIN before SAVEPOINTs to prevent
            # RELEASE SAVEPOINT from auto-committing when no outer transaction exists.
            # Issue after conn.begin() to avoid conflicting with SQLAlchemy's autobegin.
            if self._database.engine.dialect.name == "sqlite":  # pragma: nocover
                await self._connection.exec_driver_sql("BEGIN")
        else:
            # Nested transaction - use savepoint
            self._connection = self._database.get_transaction_connection()
            assert self._connection is not None
            self._transaction = await self._connection.begin_nested()

        _transaction_depth.set(self._depth + 1)

        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackType] = None,
    ) -> None:
        """Exit transaction context."""
        try:
            _transaction_depth.set(self._depth)

            # Handle transaction completion
            if self._transaction is not None:
                if exc_type is not None or self._force_rollback:
                    await self._transaction.rollback()
                else:
                    await self._transaction.commit()
        finally:
            if self._depth == 0:
                self._database.set_transaction_connection(None)
                if self._connection is not None:
                    await self._connection.close()
