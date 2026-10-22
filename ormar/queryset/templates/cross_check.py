"""
Test-suite safety net for statement templates.

With ``ENABLED`` (``ORMAR_TEMPLATE_CROSS_CHECK=1``, set by the test suite)
every cache hit also builds the statement the old way, without templates, and
fails if the two would run different SQL, values or bind types. That catches a
cache key missing something that changes the SQL.
"""

import os
import re
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
    Type,
)

from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import Executable

# Test-suite safety net: every cache hit is also built without templates and
# compared with the cached statement (see ``assert_same_statement``).
ENABLED = os.environ.get("ORMAR_TEMPLATE_CROSS_CHECK") == "1"


# numbered placeholders in rendered SQL: ``$1`` (asyncpg) or ``:1`` (numeric)
NUMBERED = re.compile(r"[$:](\d+)")


def rendered_statement(
    statement: Executable, params: Optional[Dict[str, Any]], dialect: Dialect
) -> Tuple[Any, ...]:
    """
    Render a statement to positional SQL, values and bind types, so statements
    with differently named parameters can be compared.

    A template names its parameters ``ormar_p0``, ``ormar_limit`` and so on,
    while a statement built without templates gets SQLAlchemy's anonymous
    names, so their named SQL never matches. Rendered with positional
    placeholders, the two match whenever they would run the same query::

        cached: "... WHERE authors.name = :ormar_p0", {"ormar_p0": "a"}
        fresh:  "... WHERE authors.name = :name_1", {"name_1": "a"}
        # both -> ("... WHERE authors.name = ?", ["a"], ["String"])

    Dialects with named placeholders (aiopg) are rendered with the SQLite
    dialect instead, which is positional.

    :param statement: statement to render
    :type statement: Executable
    :param params: values for named bind parameters, or None to use the
        values stored on the statement
    :type params: Optional[Dict[str, Any]]
    :param dialect: dialect to render with
    :type dialect: Dialect
    :return: SQL, positional values and bind type classes
    :rtype: Tuple[Any, ...]
    """
    if not dialect.positional:  # pragma: no cover - aiopg only
        dialect = SQLiteDialect()
    compiled = statement.compile(dialect=dialect)  # type: ignore[attr-defined]
    bound = compiled.construct_params(params=params)
    expanded = compiled.construct_expanded_state(params=bound)
    names = list(expanded.positiontup or [])
    sql = expanded.statement
    if dialect.paramstyle.startswith("numeric"):
        # numbered placeholders render a repeated parameter once; list it per
        # occurrence so one shared slot equals two separate parameters
        names = [names[int(number) - 1] for number in NUMBERED.findall(sql)]
        sql = NUMBERED.sub("?", sql)
    values = [expanded.parameters[name] for name in names]
    types = [
        type(compiled.binds[name].type).__name__
        for name in names
        if name in compiled.binds
    ]
    return sql, values, types


def assert_same_statement(
    cached: Executable, fresh: Executable, params: Dict[str, Any], dialect: Dialect
) -> None:
    """
    Fail if a cached template would run different SQL, values or bind types
    than the statement built without templates for the same call.

    Only runs when ``ENABLED`` is on. It catches a cache key that misses
    something that changes the SQL: two querysets that build different SQL
    but share a cache key would reuse the wrong statement.

    :param cached: statement taken from the template cache
    :type cached: Executable
    :param fresh: statement built without named slots for this call
    :type fresh: Executable
    :param params: values bound to the cached statement
    :type params: Dict[str, Any]
    :param dialect: dialect the statements run on
    :type dialect: Dialect
    :raises AssertionError: when the statements differ
    """
    cached_render = rendered_statement(statement=cached, params=params, dialect=dialect)
    fresh_render = rendered_statement(statement=fresh, params=None, dialect=dialect)
    if cached_render != fresh_render:
        raise AssertionError(
            f"Query template mismatch:\ncached: {cached_render}\nfresh:  {fresh_render}"
        )


def cross_check_write(
    model_cls: Type[Any],
    statement: Executable,
    literal: Executable,
    params: Dict[str, Any],
) -> None:
    """
    With ``ENABLED``, compare a write template with the statement built
    with literal values, as without templates. Catches a write cache key that
    misses something, e.g. two column sets sharing one cached insert.

    :param model_cls: written model
    :type model_cls: Type[Any]
    :param statement: template statement
    :type statement: Executable
    :param literal: statement built with literal values
    :type literal: Executable
    :param params: values bound to the template
    :type params: Dict[str, Any]
    :raises AssertionError: when the statements differ
    """
    if ENABLED:
        assert_same_statement(
            cached=statement,
            fresh=literal,
            params=params,
            dialect=model_cls.ormar_config.database.dialect,
        )
