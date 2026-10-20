"""
Read statement templates: per-call values become named bind parameters so a
statement built once per query shape can be reused with new values.
"""

import os
import re
import weakref
from collections import OrderedDict
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Union,
)

import sqlalchemy
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import Executable, operators, visitors
from sqlalchemy.sql.elements import BindParameter

from ormar.queryset.actions.filter_action import FILTER_OPERATORS

if TYPE_CHECKING:  # pragma: no cover
    from ormar.models.excludable import ExcludableItems
    from ormar.queryset.actions.filter_action import FilterAction
    from ormar.queryset.actions.order_action import OrderAction
    from ormar.queryset.clause import FilterGroup

LIMIT_SLOT = "ormar_limit"
OFFSET_SLOT = "ormar_offset"
PK_SLOT = "ormar_pk"
NUMBERED = re.compile(r"[$:](\d+)")
TEMPLATE_CACHE_SIZE = 512
# SQLAlchemy operators of ormar's filter operator methods, used to type bound
# values exactly like a plain ``column <op> value`` comparison would.
COMPARISON_OPERATORS = {
    "__eq__": operators.eq,
    "like": operators.like_op,
    "ilike": operators.ilike_op,
    "in_": operators.in_op,
    "__gt__": operators.gt,
    "__ge__": operators.ge,
    "__lt__": operators.lt,
    "__le__": operators.le,
}
# Test-suite safety net: every template hit is also built fresh and compared.
CROSS_CHECK = os.environ.get("ORMAR_TEMPLATE_CROSS_CHECK") == "1"


class StatementTemplate(NamedTuple):
    """A cached statement and the values to bind for one call."""

    statement: Executable
    params: Dict[str, Any]


class TemplateCache:
    """Least-recently-used cache of the statements of one model, by shape."""

    def __init__(self, max_size: int = TEMPLATE_CACHE_SIZE) -> None:
        """
        :param max_size: number of statement shapes kept
        :type max_size: int
        """
        self.max_size = max_size
        self.entries: "OrderedDict[Any, Any]" = OrderedDict()

    def get(self, key: Any) -> Any:
        """
        Return the statement cached for a shape, or None.

        :param key: statement shape
        :type key: Any
        :return: cached statement or None
        :rtype: Any
        """
        statement = self.entries.get(key)
        if statement is not None:
            try:
                self.entries.move_to_end(key)
            except KeyError:  # pragma: no cover - evicted concurrently
                pass
        return statement

    def put(self, key: Any, statement: Any) -> None:
        """
        Cache the statement of a shape, evicting the least recently used one.

        :param key: statement shape
        :type key: Any
        :param statement: statement to cache
        :type statement: Any
        """
        self.entries[key] = statement
        if len(self.entries) > self.max_size:
            self.entries.popitem(last=False)

    def clear(self) -> None:
        """Remove every cached statement."""
        self.entries.clear()


TEMPLATE_CACHES: "weakref.WeakKeyDictionary[type, TemplateCache]" = (
    weakref.WeakKeyDictionary()
)


def template_cache_for(model_cls: Type[Any]) -> TemplateCache:
    """
    Return the statement template cache of a model class.

    :param model_cls: model class
    :type model_cls: Type[Any]
    :return: the model's template cache
    :rtype: TemplateCache
    """
    cache = TEMPLATE_CACHES.get(model_cls)
    if cache is None:
        cache = TEMPLATE_CACHES.setdefault(model_cls, TemplateCache())
    return cache


def template_params(
    actions: List["FilterAction"], limit: Optional[int], offset: Optional[int]
) -> Dict[str, Any]:
    """
    Collect the values a template binds for one call, keyed like the slots
    ``SlotBinder`` names.

    :param actions: filter actions in canonical order
    :type actions: List[FilterAction]
    :param limit: effective limit (applied when not None)
    :type limit: Optional[int]
    :param offset: effective offset (applied when truthy)
    :type offset: Optional[int]
    :return: values per slot name
    :rtype: Dict[str, Any]
    """
    params = {}
    for index, action in enumerate(actions):
        if action.binds_slot():
            params[f"ormar_p{index}"] = action.bound_value()
    if limit is not None:
        params[LIMIT_SLOT] = limit
    if offset:
        params[OFFSET_SLOT] = offset
    return params


def rendered_statement(
    statement: Executable, params: Optional[Dict[str, Any]], dialect: Dialect
) -> Tuple[Any, ...]:
    """
    Render a statement to positional SQL, values and bind types, so statements
    with differently named parameters can be compared.

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


def value_types(values: dict[str, Any]) -> dict[str, type]:
    """
    Return the type of each value, so values that compare equal across types
    (``1 == 1.0 == True``, str enums and strings) are told apart.

    :param values: field values
    :type values: dict[str, Any]
    :return: type per field name
    :rtype: dict[str, type]
    """
    return {name: type(value) for name, value in values.items()}


def iter_actions(
    clauses: List[Union["FilterAction", "FilterGroup"]],
) -> Iterator["FilterAction"]:
    """
    Yield the filter actions of a clause list in the order their SQL is built:
    a group's nested groups first, then its own actions.

    :param clauses: filter actions and filter groups
    :type clauses: List[Union[FilterAction, FilterGroup]]
    :return: filter actions in build order
    :rtype: Iterator[FilterAction]
    """
    for clause in clauses:
        if hasattr(clause, "_nested_groups"):
            yield from iter_actions(clauses=clause._nested_groups)  # type: ignore
            yield from clause.actions  # type: ignore
        else:
            yield clause  # type: ignore


def canonical_actions(
    filter_clauses: List[Any], exclude_clauses: List[Any]
) -> List["FilterAction"]:
    """
    Return every filter action of a queryset in canonical order: filter clauses
    first, then exclude clauses, each in build order.

    :param filter_clauses: queryset filter clauses
    :type filter_clauses: List[Any]
    :param exclude_clauses: queryset exclude clauses
    :type exclude_clauses: List[Any]
    :return: filter actions in canonical order
    :rtype: List[FilterAction]
    """
    return [
        *iter_actions(clauses=filter_clauses),
        *iter_actions(clauses=exclude_clauses),
    ]


class SlotBinder:
    """
    Turns per-call values of a read statement into named bind parameters: a
    filter action's value is named ``ormar_p{n}`` after its position among the
    canonical actions (so a filter applied twice binds one parameter), limit
    and offset get fixed names.
    """

    def __init__(self, actions: List["FilterAction"]) -> None:
        """
        :param actions: filter actions of the queryset in canonical order
        :type actions: List[FilterAction]
        """
        self.slots = {
            id(action): f"ormar_p{index}" for index, action in enumerate(actions)
        }

    def bind(self, action: "FilterAction", value: Any) -> BindParameter:
        """
        Return the named bind parameter for a filter action's value.

        :param action: filter action the value belongs to
        :type action: FilterAction
        :param value: value to bind
        :type value: Any
        :return: named bind parameter
        :rtype: BindParameter
        """
        operator = COMPARISON_OPERATORS[FILTER_OPERATORS[action.operator]]
        return BindParameter(
            key=self.slots[id(action)],
            value=value,
            expanding=action.operator == "in",
            _compared_to_operator=operator,
            _compared_to_type=action.column.type,
        )

    def limit(self, value: int) -> BindParameter:
        """
        Return the named bind parameter for a limit.

        :param value: limit value
        :type value: int
        :return: named bind parameter
        :rtype: BindParameter
        """
        return sqlalchemy.bindparam(
            key=LIMIT_SLOT, value=value, type_=sqlalchemy.Integer()
        )

    def offset(self, value: int) -> BindParameter:
        """
        Return the named bind parameter for an offset.

        :param value: offset value
        :type value: int
        :return: named bind parameter
        :rtype: BindParameter
        """
        return sqlalchemy.bindparam(
            key=OFFSET_SLOT, value=value, type_=sqlalchemy.Integer()
        )


def value_kind(action: "FilterAction") -> Any:
    """
    Return the part of a filter value that changes the SQL text: ``IS NULL`` /
    ``IS NOT NULL`` for ``isnull``, ``IS NULL`` for a ``None`` value, an
    expanding ``IN`` list, or a plain bound value.

    :param action: filter action
    :type action: FilterAction
    :return: hashable value kind
    :rtype: Any
    """
    value = action.bound_value()
    if action.operator == "isnull":
        return ("isnull", bool(value))
    if action.operator == "in":
        return "in"
    if not action.binds_slot():
        return ("literal", value)
    return ("value", type(value))


def action_shape(action: "FilterAction") -> Tuple[Any, ...]:
    """
    Return everything about a filter action that changes the SQL text.

    :param action: filter action
    :type action: FilterAction
    :return: hashable shape of the action
    :rtype: Tuple[Any, ...]
    """
    return (
        action.query_str,
        action.table_prefix,
        action.operator,
        value_kind(action=action),
        action.has_escaped_character,
    )


def clauses_shape(clauses: Iterable[Any]) -> Tuple[Any, ...]:
    """
    Return the shape of a list of filter actions and filter groups, keeping
    the and/or/not nesting.

    :param clauses: filter actions and filter groups
    :type clauses: Iterable[Any]
    :return: hashable shape of the clauses
    :rtype: Tuple[Any, ...]
    """
    return tuple(
        (
            "group",
            clause.filter_type,
            clause.exclude,
            clauses_shape(clauses=clause._nested_groups),
            tuple(action_shape(action=action) for action in clause.actions),
        )
        if hasattr(clause, "_nested_groups")
        else action_shape(action=clause)
        for clause in clauses
    )


def excludable_shape(excludable: "ExcludableItems") -> Tuple[Any, ...]:
    """
    Return the fields included and excluded per model and relation.

    :param excludable: queryset's fields to include and exclude
    :type excludable: ExcludableItems
    :return: hashable shape of the projection
    :rtype: Tuple[Any, ...]
    """
    return tuple(
        (
            key,
            tuple(sorted(item.include, key=repr)),
            tuple(sorted(item.exclude, key=repr)),
            tuple(sorted(item.flatten, key=repr)),
            item.pk_only,
        )
        for key, item in sorted(excludable.items.items())
    )


def order_shape(order_bys: Iterable["OrderAction"]) -> Tuple[Any, ...]:
    """
    Return the shape of the requested orderings.

    :param order_bys: order actions of the query
    :type order_bys: Iterable[OrderAction]
    :return: hashable shape of the ordering
    :rtype: Tuple[Any, ...]
    """
    return tuple(
        (order.query_str, order.direction, order.table_prefix, order.nulls_ordering)
        for order in order_bys
    )


def exists_statement(select: sqlalchemy.sql.Select) -> Executable:
    """
    Wrap a read select into ``SELECT EXISTS (...)``.

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
    Wrap a read select into a row count, optionally of distinct primary keys.

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
    Wrap a read select into aggregate function columns.

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


PK_COLUMNS: "weakref.WeakKeyDictionary[type, sqlalchemy.Column]" = (
    weakref.WeakKeyDictionary()
)


def cached_statement(
    model_cls: Type[Any],
    key: Tuple[Any, ...],
    builder: Callable[..., Executable],
    **builder_kwargs: Any,
) -> Executable:
    """
    Return the statement cached for a write shape, building it on a miss.

    :param model_cls: model the statement writes
    :type model_cls: Type[Any]
    :param key: write shape
    :type key: Tuple[Any, ...]
    :param builder: builds the statement from ``builder_kwargs``
    :type builder: Callable[..., Executable]
    :param builder_kwargs: keyword arguments of the builder
    :type builder_kwargs: Any
    :return: statement for the shape
    :rtype: Executable
    """
    cache = template_cache_for(model_cls=model_cls)
    statement = cache.get(key)
    if statement is None:
        statement = builder(**builder_kwargs)
        cache.put(key=key, statement=statement)
    return statement


def cross_check_write(
    model_cls: Type[Any],
    statement: Executable,
    literal: Executable,
    params: Dict[str, Any],
) -> None:
    """
    With ``CROSS_CHECK``, compare a write template with the statement built
    with literal values, as without templates.

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
    if CROSS_CHECK:
        assert_same_statement(
            cached=statement,
            fresh=literal,
            params=params,
            dialect=model_cls.ormar_config.database.dialect,
        )


def primary_key_column(model_cls: Type[Any]) -> sqlalchemy.Column:
    """
    Return the primary key column of a model's table, resolved once per model.

    :param model_cls: model class
    :type model_cls: Type[Any]
    :return: primary key column
    :rtype: sqlalchemy.Column
    """
    column = PK_COLUMNS.get(model_cls)
    if column is None:
        alias = model_cls.get_column_alias(field_name=model_cls.ormar_config.pkname)
        column = PK_COLUMNS.setdefault(model_cls, model_cls.ormar_config.table.c[alias])
    return column


def primary_key_bind(column: sqlalchemy.Column) -> BindParameter:
    """
    Return the named bind parameter of a primary key filter.

    :param column: primary key column
    :type column: sqlalchemy.Column
    :return: named bind parameter
    :rtype: BindParameter
    """
    return sqlalchemy.bindparam(key=PK_SLOT, type_=column.type)


def value_binds(table: sqlalchemy.Table, names: Tuple[str, ...]) -> Dict[Any, Any]:
    """
    Return named bind parameters for the written columns, ``ormar_v{i}`` in
    the given column order.

    :param table: written table
    :type table: sqlalchemy.Table
    :param names: written column names, sorted
    :type names: Tuple[str, ...]
    :return: bind parameter per column
    :rtype: Dict[Any, Any]
    """
    return {
        table.c[name]: sqlalchemy.bindparam(
            key=f"ormar_v{index}", type_=table.c[name].type
        )
        for index, name in enumerate(names)
    }


def value_params(names: Tuple[str, ...], values: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Return the written values keyed by their bind parameter names.

    :param names: written column names, sorted
    :type names: Tuple[str, ...]
    :param values: written values by column name
    :type values: Mapping[str, Any]
    :return: values by bind parameter name
    :rtype: Dict[str, Any]
    """
    return {f"ormar_v{index}": values[name] for index, name in enumerate(names)}


def literal_values(
    table: sqlalchemy.Table, names: Tuple[str, ...], values: Mapping[str, Any]
) -> Dict[Any, Any]:
    """
    Return the written values keyed by column, in the template's column order.

    :param table: written table
    :type table: sqlalchemy.Table
    :param names: written column names, sorted
    :type names: Tuple[str, ...]
    :param values: written values by column name
    :type values: Mapping[str, Any]
    :return: value per column
    :rtype: Dict[Any, Any]
    """
    return {table.c[name]: values[name] for name in names}


def build_insert(table: sqlalchemy.Table, names: Tuple[str, ...]) -> Executable:
    """
    Build an insert of the given columns with named value binds.

    :param table: written table
    :type table: sqlalchemy.Table
    :param names: written column names, sorted
    :type names: Tuple[str, ...]
    :return: insert statement
    :rtype: Executable
    """
    return table.insert().values(value_binds(table=table, names=names))


def build_update(
    table: sqlalchemy.Table, names: Tuple[str, ...], pk_column: sqlalchemy.Column
) -> Executable:
    """
    Build an update of the given columns of one row, by primary key.

    :param table: written table
    :type table: sqlalchemy.Table
    :param names: updated column names, sorted
    :type names: Tuple[str, ...]
    :param pk_column: primary key column
    :type pk_column: sqlalchemy.Column
    :return: update statement
    :rtype: Executable
    """
    return (
        table.update()
        .values(value_binds(table=table, names=names))
        .where(pk_column == primary_key_bind(column=pk_column))
    )


def build_delete(table: sqlalchemy.Table, pk_column: sqlalchemy.Column) -> Executable:
    """
    Build a delete of one row, by primary key.

    :param table: written table
    :type table: sqlalchemy.Table
    :param pk_column: primary key column
    :type pk_column: sqlalchemy.Column
    :return: delete statement
    :rtype: Executable
    """
    return table.delete().where(pk_column == primary_key_bind(column=pk_column))


def build_pk_select(
    table: sqlalchemy.Table, pk_column: sqlalchemy.Column
) -> Executable:
    """
    Build a select of one row, by primary key.

    :param table: queried table
    :type table: sqlalchemy.Table
    :param pk_column: primary key column
    :type pk_column: sqlalchemy.Column
    :return: select statement
    :rtype: Executable
    """
    return table.select().where(pk_column == primary_key_bind(column=pk_column))


def build_bulk_update(
    table: sqlalchemy.Table, pk_column: sqlalchemy.Column, columns: Tuple[str, ...]
) -> Executable:
    """
    Build the per-row update used by ``bulk_update``: values and the primary
    key come from ``new_<column>`` parameters of each row.

    :param table: updated table
    :type table: sqlalchemy.Table
    :param pk_column: primary key column
    :type pk_column: sqlalchemy.Column
    :param columns: updated column names
    :type columns: Tuple[str, ...]
    :return: update statement
    :rtype: Executable
    """
    return (
        table.update()
        .where(pk_column == sqlalchemy.bindparam(key="new_" + pk_column.name))
        .values({name: sqlalchemy.bindparam(key="new_" + name) for name in columns})
    )


def insert_statement(model_cls: Type[Any], names: Tuple[str, ...]) -> Executable:
    """
    Return the insert of a model for the given sorted column names.

    :param model_cls: inserted model
    :type model_cls: Type[Any]
    :param names: inserted column names, sorted
    :type names: Tuple[str, ...]
    :return: insert statement with ``ormar_v{i}`` binds
    :rtype: Executable
    """
    table = model_cls.ormar_config.table
    return cached_statement(
        model_cls=model_cls,
        key=("insert", names),
        builder=build_insert,
        table=table,
        names=names,
    )


def insert_template(
    model_cls: Type[Any], values: Mapping[str, Any]
) -> StatementTemplate:
    """
    Return the insert of a model for the given column values.

    :param model_cls: inserted model
    :type model_cls: Type[Any]
    :param values: values by column name
    :type values: Mapping[str, Any]
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    names = tuple(sorted(values))
    statement = insert_statement(model_cls=model_cls, names=names)
    params = value_params(names=names, values=values)
    table = model_cls.ormar_config.table
    if CROSS_CHECK:
        literal = table.insert().values(
            literal_values(table=table, names=names, values=values)
        )
        cross_check_write(
            model_cls=model_cls, statement=statement, literal=literal, params=params
        )
    return StatementTemplate(statement=statement, params=params)


def update_template(
    model_cls: Type[Any], values: Mapping[str, Any], pk: Any
) -> StatementTemplate:
    """
    Return the update of one row of a model for the given column values.

    :param model_cls: updated model
    :type model_cls: Type[Any]
    :param values: values by column name
    :type values: Mapping[str, Any]
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    names = tuple(sorted(values))
    table = model_cls.ormar_config.table
    pk_column = primary_key_column(model_cls=model_cls)
    statement = cached_statement(
        model_cls=model_cls,
        key=("update", names),
        builder=build_update,
        table=table,
        names=names,
        pk_column=pk_column,
    )
    params = {**value_params(names=names, values=values), PK_SLOT: pk}
    if CROSS_CHECK:
        literal = (
            table.update()
            .values(literal_values(table=table, names=names, values=values))
            .where(pk_column == pk)
        )
        cross_check_write(
            model_cls=model_cls, statement=statement, literal=literal, params=params
        )
    return StatementTemplate(statement=statement, params=params)


def delete_template(model_cls: Type[Any], pk: Any) -> StatementTemplate:
    """
    Return the delete of one row of a model.

    :param model_cls: model
    :type model_cls: Type[Any]
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    table = model_cls.ormar_config.table
    pk_column = primary_key_column(model_cls=model_cls)
    statement = cached_statement(
        model_cls=model_cls,
        key=("delete",),
        builder=build_delete,
        table=table,
        pk_column=pk_column,
    )
    params = {PK_SLOT: pk}
    if CROSS_CHECK:
        cross_check_write(
            model_cls=model_cls,
            statement=statement,
            literal=table.delete().where(pk_column == pk),
            params=params,
        )
    return StatementTemplate(statement=statement, params=params)


def pk_select_template(model_cls: Type[Any], pk: Any) -> StatementTemplate:
    """
    Return the select of one row of a model by primary key.

    :param model_cls: model
    :type model_cls: Type[Any]
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    table = model_cls.ormar_config.table
    pk_column = primary_key_column(model_cls=model_cls)
    statement = cached_statement(
        model_cls=model_cls,
        key=("select_pk",),
        builder=build_pk_select,
        table=table,
        pk_column=pk_column,
    )
    params = {PK_SLOT: pk}
    if CROSS_CHECK:
        cross_check_write(
            model_cls=model_cls,
            statement=statement,
            literal=table.select().where(pk_column == pk),
            params=params,
        )
    return StatementTemplate(statement=statement, params=params)


def release_values(statement: Executable) -> None:
    """
    Drop the values of a template's named slots (SQLAlchemy may have cloned the
    bind parameters while building), so a cached statement does not keep the
    first call's values alive; every call binds its own values.

    :param statement: built template statement
    :type statement: Executable
    """
    for element in visitors.iterate(statement):  # type: ignore[arg-type]
        if isinstance(element, BindParameter) and str(element.key).startswith("ormar_"):
            element.value = None
