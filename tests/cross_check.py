"""
Test-suite safety net for statement templates and trusted model construction.

``install()`` (called by ``tests/conftest.py``) wraps the functions that build
templates and trusted instances. With ``ENABLED`` on, every call also builds
the result the old way and fails if the two differ:

- a read or write template must run the same SQL, values and bind types as
  the statement built without templates, which catches a cache key missing
  something that changes the SQL
- an instance built straight from a row (``_construct_from_row``) must equal
  the one built with validation
- ``dump_for_save`` must return the same values as ``model_dump()``

Tests that count built statements or constructed instances switch it off with
``monkeypatch.setattr(cross_check, "ENABLED", False)``.
"""

import re
import sys
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

import sqlalchemy
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import Executable

from ormar.models.model_row import ModelRow, RowExtractionPlan
from ormar.queryset import FilterQuery
from ormar.queryset import queryset as queryset_module
from ormar.queryset.queryset import QuerySet
from ormar.queryset.templates import (
    StatementTemplate,
    delete_template,
    insert_template,
    pk_select_template,
    primary_key_column,
    update_template,
)

ENABLED = True

# numbered placeholders in rendered SQL: ``$1`` (asyncpg) or ``:1`` (numeric)
NUMBERED = re.compile(r"[$:](\d+)")

ORIGINAL_SELECT_TEMPLATE = QuerySet.build_select_template
ORIGINAL_WRITE_TEMPLATE = QuerySet.build_write_template
ORIGINAL_CONSTRUCT_ROW = ModelRow.__dict__["construct_row_instance"].__func__
ORIGINAL_INSERT_TEMPLATE = insert_template
ORIGINAL_UPDATE_TEMPLATE = update_template
ORIGINAL_DELETE_TEMPLATE = delete_template
ORIGINAL_PK_SELECT_TEMPLATE = pk_select_template
ORIGINAL_DUMP_FOR_SAVE = queryset_module.dump_for_save


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
    Fail if a template would run different SQL, values or bind types than the
    statement built without templates for the same call.

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


def assert_same_write(
    model_cls: Any, template: StatementTemplate, fresh: Executable
) -> None:
    """
    Fail if a write template differs from the statement built with literal
    values, e.g. two column sets sharing one cached insert.

    :param model_cls: written model
    :type model_cls: Any
    :param template: template statement and its values
    :type template: StatementTemplate
    :param fresh: statement built with literal values
    :type fresh: Executable
    :raises AssertionError: when the statements differ
    """
    assert_same_statement(
        cached=template.statement,
        fresh=fresh,
        params=template.params,
        dialect=model_cls.ormar_config.database.dialect,
    )


def value_types(values: Dict[str, Any]) -> Dict[str, type]:
    """
    Return the type of each value, so values that compare equal across types
    (``1 == 1.0 == True``, str enums and strings) are told apart::

        {"a": 1} == {"a": True}  # -> True
        value_types(values={"a": 1}) == value_types(values={"a": True})  # -> False

    :param values: field values
    :type values: Dict[str, Any]
    :return: type per field name
    :rtype: Dict[str, type]
    """
    return {name: type(value) for name, value in values.items()}


def literal_values(
    table: sqlalchemy.Table, values: Mapping[str, Any]
) -> Dict[Any, Any]:
    """
    Return the written values keyed by column, in the template's column order
    (sorted by name), to build a write with literal values.

    :param table: written table
    :type table: sqlalchemy.Table
    :param values: written values by column name
    :type values: Mapping[str, Any]
    :return: value per column
    :rtype: Dict[Any, Any]
    """
    return {table.c[name]: values[name] for name in sorted(values)}


def checked_select_template(
    self: QuerySet,
    variant: tuple = (),
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    order_bys: Optional[list] = None,
    wrap: Optional[Callable[[Any], Any]] = None,
) -> StatementTemplate:
    """
    ``QuerySet.build_select_template`` compared with the statement built by
    ``build_select_expression`` without templates.

    :param self: queryset
    :type self: QuerySet
    :param variant: how the select is used (plain, count, aggregate, ...)
    :type variant: tuple
    :param limit: number to limit the query
    :type limit: Optional[int]
    :param offset: number to offset by
    :type offset: Optional[int]
    :param order_bys: list of order actions
    :type order_bys: Optional[list]
    :param wrap: turns the built select into the executed statement
    :type wrap: Optional[Callable[[Any], Any]]
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    template = ORIGINAL_SELECT_TEMPLATE(
        self,
        variant=variant,
        limit=limit,
        offset=offset,
        order_bys=order_bys,
        wrap=wrap,
    )
    if ENABLED:
        fresh = self.build_select_expression(
            limit=limit, offset=offset, order_bys=order_bys
        )
        assert_same_statement(
            cached=template.statement,
            fresh=wrap(fresh) if wrap is not None else fresh,
            params=template.params,
            dialect=self.model_config.database.dialect,
        )
    return template


def checked_write_template(
    self: QuerySet, kind: str, values: dict
) -> StatementTemplate:
    """
    ``QuerySet.build_write_template`` compared with the filtered update or
    delete built with literal values.

    :param self: queryset
    :type self: QuerySet
    :param kind: ``"update"`` or ``"delete"``
    :type kind: str
    :param values: updated values by column name (empty for delete)
    :type values: dict
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    template = ORIGINAL_WRITE_TEMPLATE(self, kind=kind, values=values)
    if ENABLED:
        table = self.table
        expr: Any = (
            table.update().values(literal_values(table=table, values=values))
            if kind == "update"
            else table.delete()
        )
        expr = FilterQuery(filter_clauses=self.filter_clauses).apply(expr=expr)
        expr = FilterQuery(filter_clauses=self.exclude_clauses, exclude=True).apply(
            expr=expr
        )
        assert_same_write(model_cls=self.model, template=template, fresh=expr)
    return template


def checked_insert_template(
    model_cls: Any, values: Mapping[str, Any]
) -> StatementTemplate:
    """
    ``insert_template`` compared with the insert built with literal values.

    :param model_cls: inserted model
    :type model_cls: Any
    :param values: values by column name
    :type values: Mapping[str, Any]
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    template = ORIGINAL_INSERT_TEMPLATE(model_cls=model_cls, values=values)
    if ENABLED:
        table = model_cls.ormar_config.table
        fresh = table.insert().values(literal_values(table=table, values=values))
        assert_same_write(model_cls=model_cls, template=template, fresh=fresh)
    return template


def checked_update_template(
    model_cls: Any, values: Mapping[str, Any], pk: Any
) -> StatementTemplate:
    """
    ``update_template`` compared with the update built with literal values.

    :param model_cls: updated model
    :type model_cls: Any
    :param values: values by column name
    :type values: Mapping[str, Any]
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    template = ORIGINAL_UPDATE_TEMPLATE(model_cls=model_cls, values=values, pk=pk)
    if ENABLED:
        table = model_cls.ormar_config.table
        fresh = (
            table.update()
            .values(literal_values(table=table, values=values))
            .where(primary_key_column(model_cls=model_cls) == pk)
        )
        assert_same_write(model_cls=model_cls, template=template, fresh=fresh)
    return template


def checked_delete_template(model_cls: Any, pk: Any) -> StatementTemplate:
    """
    ``delete_template`` compared with the delete built with a literal pk.

    :param model_cls: model
    :type model_cls: Any
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    template = ORIGINAL_DELETE_TEMPLATE(model_cls=model_cls, pk=pk)
    if ENABLED:
        table = model_cls.ormar_config.table
        fresh = table.delete().where(primary_key_column(model_cls=model_cls) == pk)
        assert_same_write(model_cls=model_cls, template=template, fresh=fresh)
    return template


def checked_pk_select_template(model_cls: Any, pk: Any) -> StatementTemplate:
    """
    ``pk_select_template`` compared with the select built with a literal pk.

    :param model_cls: model
    :type model_cls: Any
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    template = ORIGINAL_PK_SELECT_TEMPLATE(model_cls=model_cls, pk=pk)
    if ENABLED:
        table = model_cls.ormar_config.table
        fresh = table.select().where(primary_key_column(model_cls=model_cls) == pk)
        assert_same_write(model_cls=model_cls, template=template, fresh=fresh)
    return template


def checked_construct_row_instance(
    cls: Any, plan: RowExtractionPlan, item: Dict[str, Any]
) -> Any:
    """
    ``ModelRow.construct_row_instance`` where a trusted build must hold the
    same values of the same types and the same set fields as the instance
    built with validation.

    :param cls: model class
    :type cls: Any
    :param plan: extraction plan of the row position
    :type plan: RowExtractionPlan
    :param item: field values of the row
    :type item: Dict[str, Any]
    :raises AssertionError: when trusted and regular builds differ
    :return: model instance
    :rtype: Any
    """
    built = (
        cls._construct_with_excluded(excluded=plan.excluded_field_names, **item)
        if ENABLED and cls.constructs_from_row(item=item)
        else None
    )
    instance = ORIGINAL_CONSTRUCT_ROW(cls, plan=plan, item=item)
    if built is not None and (
        instance.__dict__ != built.__dict__
        or value_types(values=instance.__dict__) != value_types(values=built.__dict__)
        or instance.__pydantic_fields_set__ != built.__pydantic_fields_set__
    ):
        raise AssertionError(
            f"Trusted row construction mismatch for {cls.__name__}: "
            f"{instance.__dict__} != {built.__dict__}"
        )
    return instance


def checked_dump_for_save(obj: Any) -> dict:
    """
    ``dump_for_save`` compared with ``model_dump()``, including the types of
    the values.

    :param obj: model to save
    :type obj: Any
    :raises AssertionError: when the copy differs from ``model_dump()``
    :return: field values
    :rtype: dict
    """
    values = ORIGINAL_DUMP_FOR_SAVE(obj=obj)
    if ENABLED:
        dumped = obj.model_dump()
        if values != dumped or value_types(values=values) != value_types(values=dumped):
            raise AssertionError(
                f"Trusted dump mismatch for {obj.get_name()}: {values} != {dumped}"
            )
    return values


def replace_function(original: Callable[..., Any], checked: Callable[..., Any]) -> None:
    """
    Replace a module-level function in every loaded ormar module that holds
    it, so modules that imported it by name call the checked version too.

    :param original: function to replace
    :type original: Callable[..., Any]
    :param checked: checked version of the function
    :type checked: Callable[..., Any]
    """
    for name, module in list(sys.modules.items()):
        if name.split(".")[0] != "ormar" or module is None:
            continue
        for attribute, value in list(vars(module).items()):
            if value is original:
                setattr(module, attribute, checked)


def install() -> None:
    """
    Wrap the template and trusted construction functions with their checked
    versions for the whole test run.
    """
    QuerySet.build_select_template = checked_select_template  # type: ignore[method-assign,assignment]
    QuerySet.build_write_template = checked_write_template  # type: ignore[method-assign,assignment]
    ModelRow.construct_row_instance = classmethod(checked_construct_row_instance)  # type: ignore[method-assign,assignment]
    for original, checked in (
        (ORIGINAL_INSERT_TEMPLATE, checked_insert_template),
        (ORIGINAL_UPDATE_TEMPLATE, checked_update_template),
        (ORIGINAL_DELETE_TEMPLATE, checked_delete_template),
        (ORIGINAL_PK_SELECT_TEMPLATE, checked_pk_select_template),
        (ORIGINAL_DUMP_FOR_SAVE, checked_dump_for_save),
    ):
        replace_function(original=original, checked=checked)
