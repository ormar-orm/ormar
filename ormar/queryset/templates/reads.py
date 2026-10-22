"""
Statements built around a queryset's select (exists, count, aggregates) and
clean-up of built templates.

The ``*_statement`` helpers are the ``wrap`` callables of
``QuerySet.build_select_template``: they run once, when a cache key is first
seen, and the wrapped statement is what gets cached.
"""

from typing import (
    Any,
    List,
)

import sqlalchemy
from sqlalchemy.sql import Executable, visitors
from sqlalchemy.sql.elements import BindParameter


def exists_statement(select: sqlalchemy.sql.Select) -> Executable:
    """
    Wrap a read select into ``SELECT EXISTS (...)``, the statement run by
    ``QuerySet.exists()``.

    This and the other ``*_statement`` helpers are the ``wrap`` callables of
    ``QuerySet.build_select_template``: they run on the select once, when a
    cache key is first seen, and the wrapped statement is what gets cached.

    :param select: read select of the queryset
    :type select: sqlalchemy.sql.Select
    :return: exists statement
    :rtype: Executable
    """
    return sqlalchemy.exists(select).select()


def count_statement(
    select: sqlalchemy.sql.Select, distinct: bool, pk_column_name: str
) -> Executable:
    """
    Wrap a read select into a row count, the statement run by
    ``QuerySet.count()``. With ``distinct`` the rows are grouped by primary key
    first, so a model joined to several related rows is counted once::

        SELECT count(*) FROM (
            SELECT count(*) FROM (<select>) AS subquery_for_count GROUP BY <pk>
        ) AS subquery_for_group

    :param select: read select of the queryset
    :type select: sqlalchemy.sql.Select
    :param distinct: count distinct primary keys only
    :type distinct: bool
    :param pk_column_name: primary key column alias of the model
    :type pk_column_name: str
    :return: count statement
    :rtype: Executable
    """
    subquery = select.alias(name="subquery_for_count")
    expr = sqlalchemy.func.count().select().select_from(subquery)
    if distinct:
        grouped = expr.group_by(pk_column_name).alias(  # type: ignore[arg-type]
            name="subquery_for_group"
        )
        expr = sqlalchemy.func.count().select().select_from(grouped)
    return expr


def aggregate_statement(
    select: sqlalchemy.sql.Select, func_name: str, select_columns: List[Any]
) -> Executable:
    """
    Wrap a read select into aggregate function columns, the statement run by
    ``QuerySet.max()``, ``min()``, ``sum()`` and ``avg()``::

        SELECT max(subquery_for_max.score) AS score
        FROM (<select>) AS subquery_for_max

    :param select: read select of the queryset
    :type select: sqlalchemy.sql.Select
    :param func_name: aggregate function name
    :type func_name: str
    :param select_columns: aggregate column expressions
    :type select_columns: List[Any]
    :return: aggregate statement
    :rtype: Executable
    """
    subquery = select.alias(name=f"subquery_for_{func_name}")
    return sqlalchemy.select(*select_columns).select_from(subquery)


def release_values(statement: Executable) -> None:
    """
    Drop the values of a template's named slots, so a cached statement does
    not keep the first call's values (possibly large lists or documents) alive
    for as long as it stays cached. Every call binds its own values anyway.

    SQLAlchemy may have cloned the bind parameters while wrapping the select
    (for example into a subquery), so the whole statement is walked instead of
    clearing only the parameters ``SlotBinder`` created.

    :param statement: built template statement
    :type statement: Executable
    """
    for element in visitors.iterate(statement):  # type: ignore[arg-type]
        if isinstance(element, BindParameter) and str(element.key).startswith("ormar_"):
            element.value = None
