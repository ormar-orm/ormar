"""
Statements built around a queryset's select (exists, count, aggregates) and
clean-up of built templates.

The ``*_statement`` helpers are the ``wrap`` callables of
``QuerySet.build_select_template``: they run once, when a cache key is first
seen, and the wrapped statement is what gets cached.
"""

from typing import (
    List,
    NamedTuple,
    Tuple,
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
    select: sqlalchemy.sql.Select, distinct: bool, pk_column_names: Tuple[str, ...]
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
    :param pk_column_names: primary key column aliases of the model
    :type pk_column_names: Tuple[str, ...]
    :return: count statement
    :rtype: Executable
    """
    subquery = select.alias(name="subquery_for_count")
    expr = sqlalchemy.func.count().select().select_from(subquery)
    if distinct:
        grouped = expr.group_by(*pk_column_names).alias(  # type: ignore[arg-type]
            name="subquery_for_group"
        )
        expr = sqlalchemy.func.count().select().select_from(grouped)
    return expr


class AggregateColumn(NamedTuple):
    """
    Column of an aggregate function: its ``label`` in the result row, its
    ``column_name`` in the read select and the labels of the primary key
    columns identifying the row it is read from (``row_key_names``).
    """

    label: str
    column_name: str
    row_key_names: Tuple[str, ...]


def aggregate_statement(
    select: sqlalchemy.sql.Select,
    func_name: str,
    columns: List[AggregateColumn],
    distinct_rows: bool,
) -> Executable:
    """
    Wrap a read select into aggregate function columns, the statement run by
    ``QuerySet.max()``, ``min()``, ``sum()`` and ``avg()``::

        SELECT max(subquery_for_max.score) AS score
        FROM (<select>) AS subquery_for_max

    A one-to-many or many-to-many join repeats the rows of the other tables, so
    with ``distinct_rows`` each column is aggregated over the distinct rows it
    is read from, identified by the primary keys on its relation path, all in
    one statement::

        SELECT (
            SELECT sum(score) FROM (
                SELECT DISTINCT id, t1a2b3c4d5_id, t1a2b3c4d5_score
                FROM (<select>) AS subquery_for_sum_0
            ) AS distinct_rows_0
        ) AS score

    :param select: read select of the queryset
    :type select: sqlalchemy.sql.Select
    :param func_name: aggregate function name
    :type func_name: str
    :param columns: aggregated columns
    :type columns: List[AggregateColumn]
    :param distinct_rows: aggregate each column over its distinct rows
    :type distinct_rows: bool
    :return: aggregate statement
    :rtype: Executable
    """
    func = getattr(sqlalchemy.func, func_name)
    if not distinct_rows:
        subquery = select.alias(name=f"subquery_for_{func_name}")
        return sqlalchemy.select(
            *(
                func(sqlalchemy.column(column.column_name)).label(column.label)
                for column in columns
            )
        ).select_from(subquery)
    aggregates = []
    for index, column in enumerate(columns):
        subquery = select.alias(name=f"subquery_for_{func_name}_{index}")
        row_column_names = dict.fromkeys((*column.row_key_names, column.column_name))
        distinct_rows_query = (
            sqlalchemy.select(
                *(sqlalchemy.column(column_name) for column_name in row_column_names)
            )
            .select_from(subquery)
            .distinct()
            .alias(name=f"distinct_rows_{index}")
        )
        aggregate = (
            sqlalchemy.select(func(sqlalchemy.column(column.column_name)))
            .select_from(distinct_rows_query)
            .scalar_subquery()
        )
        aggregates.append(aggregate.label(column.label))
    return sqlalchemy.select(*aggregates)


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
