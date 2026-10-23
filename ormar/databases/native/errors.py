"""
Translates native driver exceptions into the SQLAlchemy exceptions the
SQLAlchemy execution path raises.
"""

from typing import Any, NoReturn, Sequence

from sqlalchemy import exc
from sqlalchemy.ext.asyncio import AsyncConnection

from ormar.databases.native.backend import NativeBackend


async def raise_translated(
    error: BaseException,
    *,
    backend: NativeBackend,
    sa_connection: AsyncConnection,
    sql: str,
    args: Sequence[Any],
    ismulti: bool = False,
) -> NoReturn:
    """
    Re-raise a driver exception as the ``sqlalchemy.exc`` exception SQLAlchemy
    would raise for it, so user code catches the same errors on both paths::

        asyncpg.UniqueViolationError
        # -> sqlalchemy.exc.IntegrityError: (...UniqueViolationError) duplicate key
        #    [SQL: INSERT INTO users ...] [parameters: (...)]

    When the dialect recognises the error as a lost connection, the
    connection is invalidated first, so it is closed instead of going back to
    the pool for the next query to fail on.

    :param error: exception raised by the driver
    :type error: BaseException
    :param backend: native backend the statement ran on
    :type backend: NativeBackend
    :param sa_connection: connection the statement ran on
    :type sa_connection: AsyncConnection
    :param sql: executed SQL
    :type sql: str
    :param args: driver arguments of the statement (one list per row for batches)
    :type args: Sequence[Any]
    :param ismulti: whether the statement ran once per row of a batch
    :type ismulti: bool
    :raises sqlalchemy.exc.DBAPIError: always
    """
    raw = await sa_connection.get_raw_connection()
    dbapi_connection = raw.dbapi_connection
    orig = backend.adapter.translate_error(
        error=error, dbapi_connection=dbapi_connection
    )
    invalidated = backend.dialect.is_disconnect(
        e=orig,  # type: ignore[arg-type]
        connection=dbapi_connection,
        cursor=None,
    )
    if invalidated:
        await sa_connection.invalidate()  # pragma: no cover
    raise exc.DBAPIError.instance(
        statement=sql,
        params=list(args),
        orig=orig,  # type: ignore[arg-type]
        dbapi_base_err=backend.dialect.loaded_dbapi.Error,
        hide_parameters=sa_connection.sync_engine.hide_parameters,
        connection_invalidated=invalidated,
        dialect=backend.dialect,
        ismulti=ismulti,
    ) from orig
