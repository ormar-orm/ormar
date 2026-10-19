"""
Bundle of everything a database connection needs for native execution.
"""

from dataclasses import dataclass

from sqlalchemy.engine.interfaces import Dialect

from ormar.databases.native.adapters.base import DriverAdapter
from ormar.databases.native.compiler import NativeCompiler


@dataclass
class NativeBackend:
    """Dialect, driver adapter and statement compiler of one database."""

    dialect: Dialect
    adapter: DriverAdapter
    compiler: NativeCompiler
