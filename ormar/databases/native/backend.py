"""
Bundle of everything a database connection needs for native execution.
"""

from dataclasses import dataclass

from sqlalchemy.engine.interfaces import Dialect

from ormar.databases.native.adapters.base import DriverAdapter
from ormar.databases.native.compiler import NativeCompiler


@dataclass
class NativeBackend:
    """
    What a ``DatabaseConnection`` needs to run statements natively, created
    once on ``connect()`` when the driver has an adapter: the dialect, the
    driver adapter that runs statements, and the compiler whose plan cache is
    shared by all queries on that database.
    """

    dialect: Dialect
    adapter: DriverAdapter
    compiler: NativeCompiler
