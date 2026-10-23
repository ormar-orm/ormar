"""
Per-driver adapters for native statement execution.

Only the drivers listed in ``ADAPTERS`` run natively; any other driver
(e.g. aiomysql, aiopg) keeps running every statement through SQLAlchemy.
"""

from typing import Dict, Optional, Type

from sqlalchemy.engine.interfaces import Dialect

from ormar.databases.native.adapters.aiosqlite import AiosqliteAdapter
from ormar.databases.native.adapters.asyncmy import AsyncmyAdapter
from ormar.databases.native.adapters.asyncpg import AsyncpgAdapter
from ormar.databases.native.adapters.base import DriverAdapter

ADAPTERS: Dict[str, Type[DriverAdapter]] = {
    "aiosqlite": AiosqliteAdapter,
    "asyncmy": AsyncmyAdapter,
    "asyncpg": AsyncpgAdapter,
}


def select_adapter(dialect: Dialect) -> Optional[DriverAdapter]:
    """
    Return the native adapter for the dialect's driver, or None when the driver
    has none and statements run through SQLAlchemy::

        select_adapter(create_async_engine("postgresql+asyncpg://...").dialect)
        # -> AsyncpgAdapter
        select_adapter(create_async_engine("mysql+aiomysql://...").dialect)
        # -> None

    :param dialect: dialect of the connected database
    :type dialect: Dialect
    :return: adapter instance or None
    :rtype: Optional[DriverAdapter]
    """
    adapter_cls = ADAPTERS.get(dialect.driver)
    return adapter_cls(dialect=dialect) if adapter_cls is not None else None


__all__ = ["DriverAdapter", "select_adapter"]
