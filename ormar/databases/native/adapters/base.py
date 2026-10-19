"""
Common interface of the per-driver adapters that execute bound statements
directly on a native async driver connection.
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

ITERATE_BATCH_SIZE = 100


class DriverAdapter(abc.ABC):
    """
    Executes bound statements on one driver. ``raw`` arguments are the pooled
    raw connection returned by ``AsyncConnection.get_raw_connection()``,
    exposing ``driver_connection`` and ``dbapi_connection``.
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
        Execute a statement and return all rows keyed by result label.

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
        Execute a write statement.

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
        Execute a statement once per set of arguments.

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
        Stream rows of a statement, fetching ``batch_size`` rows at a time.

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
        Make sure the driver-level transaction is open once SQLAlchemy began one.

        :param sa_connection: connection the transaction was begun on
        :type sa_connection: AsyncConnection
        """

    def translate_error(
        self, error: BaseException, dbapi_connection: Any
    ) -> BaseException:
        """
        Return the DBAPI-level exception SQLAlchemy would wrap for this driver
        error.

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
        Resolve the plan's result processors on its first execution.

        :param bound: executed statement
        :type bound: BoundStatement
        :param coltypes: driver-reported column types
        :type coltypes: List[Any]
        """
        if bound.plan.result_processors is None:
            resolve_result_processors(bound.plan, self.dialect, coltypes)


def inserted_primary_key(
    bound: BoundStatement, returned: Any, lastrowid: Optional[int]
) -> Any:
    """
    Return the primary key of an executed INSERT the way SQLAlchemy's
    ``inserted_primary_key`` does: the bound value, else the RETURNING value,
    else the driver's last row id.

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
    Return the first column of a RETURNING row, converted by its column type.

    :param bound: executed statement, with resolved result processors
    :type bound: BoundStatement
    :param row: returned row, or None when no row came back
    :type row: Any
    :return: converted first value, or None
    :rtype: Any
    """
    if row is None:
        return None
    return next(iter(row_to_dict(bound.plan, row).values()))


def execute_mutex(raw: Any) -> AsyncContextManager[Any]:
    """
    Return SQLAlchemy's per-connection execution lock if the driver adapter
    has one, so concurrent statements on one connection are serialized.

    :param raw: pooled raw connection
    :type raw: Any
    :return: async lock or a no-op context manager
    :rtype: AsyncContextManager[Any]
    """
    mutex = getattr(raw.dbapi_connection, "_execute_mutex", None)
    return mutex if mutex is not None else contextlib.nullcontext()
