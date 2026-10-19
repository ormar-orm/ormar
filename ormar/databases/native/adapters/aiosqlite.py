"""
Native execution adapter for aiosqlite.
"""

import sqlite3
from typing import Any, AsyncGenerator, Dict, List

from sqlalchemy.engine.interfaces import Dialect

from ormar.databases.native.adapters.base import (
    DriverAdapter,
    execute_mutex,
    first_value,
    inserted_primary_key,
)
from ormar.databases.native.compiler import BoundBatch, BoundStatement, row_to_dict


class AiosqliteAdapter(DriverAdapter):
    """Executes bound statements on an aiosqlite connection."""

    def __init__(self, dialect: Dialect) -> None:
        """
        :param dialect: dialect of the connected database
        :type dialect: Dialect
        """
        super().__init__(dialect)
        self.driver_errors = (sqlite3.Error,)

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
        async with execute_mutex(raw):
            async with raw.driver_connection.execute(bound.sql, bound.args) as cursor:
                rows = await cursor.fetchall()
        self.resolve_processors(bound, [None] * len(bound.plan.result_keys))
        return [row_to_dict(bound.plan, row) for row in rows]

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
        async with execute_mutex(raw):
            async with raw.driver_connection.execute(bound.sql, bound.args) as cursor:
                returned = None
                if bound.plan.returning:
                    self.resolve_processors(bound, [None] * len(bound.plan.result_keys))
                    returned = first_value(bound, await cursor.fetchone())
                if bound.plan.is_insert:
                    return inserted_primary_key(bound, returned, cursor.lastrowid)
                return cursor.rowcount if cursor.rowcount is not None else 0

    async def execute_many(self, raw: Any, batch: BoundBatch) -> None:
        """
        Execute a statement once per set of arguments.

        :param raw: pooled raw connection
        :type raw: Any
        :param batch: statement and arguments per row
        :type batch: BoundBatch
        """
        async with execute_mutex(raw):
            await raw.driver_connection.executemany(batch.sql, batch.args_list)

    async def stream(
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
        self.resolve_processors(bound, [None] * len(bound.plan.result_keys))
        async with raw.driver_connection.execute(bound.sql, bound.args) as cursor:
            while rows := await cursor.fetchmany(batch_size):
                for row in rows:
                    yield row_to_dict(bound.plan, row)
