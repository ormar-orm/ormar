"""
Read statement templates: per-call values become named bind parameters so a
statement built once per query shape can be reused with new values.
"""

import os
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
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.sql import Executable
from sqlalchemy.sql.elements import BindParameter

if TYPE_CHECKING:  # pragma: no cover
    from ormar.models.excludable import ExcludableItems
    from ormar.queryset.actions.filter_action import FilterAction
    from ormar.queryset.actions.order_action import OrderAction
    from ormar.queryset.clause import FilterGroup

LIMIT_SLOT = "ormar_limit"
OFFSET_SLOT = "ormar_offset"
PK_SLOT = "ormar_pk"
TEMPLATE_CACHE_SIZE = 512
# Test-suite safety net: every template hit is also built fresh and compared.
CROSS_CHECK = os.environ.get("ORMAR_TEMPLATE_CROSS_CHECK") == "1"


class SelectTemplate(NamedTuple):
    """A cached read statement and the values to bind for one call."""

    statement: Executable
    params: Dict[str, Any]


class TemplateCache:
    """Least-recently-used cache of read statements of one model, by shape."""

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
            self.entries.move_to_end(key)
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


TEMPLATE_CACHES: Dict[type, TemplateCache] = {}


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
        value = action.bound_value()
        if action.operator != "isnull" and value is not None:
            params[f"ormar_p{index}"] = value
    if limit is not None:
        params[LIMIT_SLOT] = limit
    if offset:
        params[OFFSET_SLOT] = offset
    return params


def assert_same_statement(
    cached: Executable, fresh: Executable, params: Dict[str, Any], dialect: Dialect
) -> None:
    """
    Fail if a cached template would run different SQL or values than the
    statement built fresh for the same call.

    :param cached: statement taken from the template cache
    :type cached: Executable
    :param fresh: statement built for this call
    :type fresh: Executable
    :param params: values bound to the cached statement
    :type params: Dict[str, Any]
    :param dialect: dialect the statements run on
    :type dialect: Dialect
    :raises AssertionError: when the statements differ
    """
    cached_sql = cached.compile(dialect=dialect)  # type: ignore[attr-defined]
    fresh_sql = fresh.compile(dialect=dialect)  # type: ignore[attr-defined]
    cached_values = cached_sql.construct_params(params)
    if str(cached_sql) != str(fresh_sql) or cached_values != fresh_sql.params:
        raise AssertionError(
            "Query template mismatch:\n"
            f"cached: {cached_sql} {cached_values}\n"
            f"fresh:  {fresh_sql} {fresh_sql.params}"
        )


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
            yield from iter_actions(clause._nested_groups)  # type: ignore
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
    return [*iter_actions(filter_clauses), *iter_actions(exclude_clauses)]


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
        return sqlalchemy.bindparam(
            self.slots[id(action)],
            value,
            type_=action.column.type,
            expanding=action.operator == "in",
        )

    def limit(self, value: int) -> BindParameter:
        """
        Return the named bind parameter for a limit.

        :param value: limit value
        :type value: int
        :return: named bind parameter
        :rtype: BindParameter
        """
        return sqlalchemy.bindparam(LIMIT_SLOT, value, type_=sqlalchemy.Integer())

    def offset(self, value: int) -> BindParameter:
        """
        Return the named bind parameter for an offset.

        :param value: offset value
        :type value: int
        :return: named bind parameter
        :rtype: BindParameter
        """
        return sqlalchemy.bindparam(OFFSET_SLOT, value, type_=sqlalchemy.Integer())


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
    return "none" if value is None else "value"


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
        value_kind(action),
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
            clauses_shape(clause._nested_groups),
            tuple(action_shape(action) for action in clause.actions),
        )
        if hasattr(clause, "_nested_groups")
        else action_shape(clause)
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
    subquery = select.alias("subquery_for_count")
    expr = sqlalchemy.func.count().select().select_from(subquery)
    if distinct:
        grouped = expr.group_by(pk_column_name).alias(  # type: ignore[arg-type]
            "subquery_for_group"
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
    subquery = select.alias(f"subquery_for_{func_name}")
    return sqlalchemy.select(*select_columns).select_from(subquery)


class WriteTemplate(NamedTuple):
    """A cached write statement and the values to bind for one call."""

    statement: Executable
    params: Dict[str, Any]


def cached_statement(
    model_cls: Type[Any],
    key: Tuple[Any, ...],
    builder: Callable[..., Executable],
    *args: Any,
) -> Executable:
    """
    Return the statement cached for a write shape, building it on a miss.
    With ``CROSS_CHECK`` a hit is rebuilt and compared.

    :param model_cls: model the statement writes
    :type model_cls: Type[Any]
    :param key: write shape
    :type key: Tuple[Any, ...]
    :param builder: builds the statement from ``args``
    :type builder: Callable[..., Executable]
    :param args: arguments of the builder
    :type args: Any
    :raises AssertionError: when the cached statement differs from a fresh one
    :return: statement for the shape
    :rtype: Executable
    """
    cache = template_cache_for(model_cls)
    statement = cache.get(key)
    if statement is None:
        statement = builder(*args)
        cache.put(key, statement)
    elif CROSS_CHECK:
        fresh = builder(*args)
        cached_sql = str(statement.compile())  # type: ignore[attr-defined]
        if cached_sql != str(fresh.compile()):  # type: ignore[attr-defined]
            raise AssertionError(f"Query template mismatch for {key}: {cached_sql}")
    return statement


def primary_key_column(model_cls: Type[Any]) -> sqlalchemy.Column:
    """
    Return the primary key column of a model's table.

    :param model_cls: model class
    :type model_cls: Type[Any]
    :return: primary key column
    :rtype: sqlalchemy.Column
    """
    alias = model_cls.get_column_alias(model_cls.ormar_config.pkname)
    return model_cls.ormar_config.table.c[alias]


def primary_key_bind(column: sqlalchemy.Column) -> BindParameter:
    """
    Return the named bind parameter of a primary key filter.

    :param column: primary key column
    :type column: sqlalchemy.Column
    :return: named bind parameter
    :rtype: BindParameter
    """
    return sqlalchemy.bindparam(PK_SLOT, type_=column.type)


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
        table.c[name]: sqlalchemy.bindparam(f"ormar_v{index}", type_=table.c[name].type)
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
    return table.insert().values(value_binds(table, names))


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
        .values(value_binds(table, names))
        .where(pk_column == primary_key_bind(pk_column))
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
    return table.delete().where(pk_column == primary_key_bind(pk_column))


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
    return table.select().where(pk_column == primary_key_bind(pk_column))


def insert_template(model_cls: Type[Any], values: Mapping[str, Any]) -> WriteTemplate:
    """
    Return the insert of a model for the given column values.

    :param model_cls: inserted model
    :type model_cls: Type[Any]
    :param values: values by column name
    :type values: Mapping[str, Any]
    :return: statement and values to bind
    :rtype: WriteTemplate
    """
    names = tuple(sorted(values))
    table = model_cls.ormar_config.table
    statement = cached_statement(
        model_cls, ("insert", names), build_insert, table, names
    )
    return WriteTemplate(statement, value_params(names, values))


def update_template(
    model_cls: Type[Any], values: Mapping[str, Any], pk: Any
) -> WriteTemplate:
    """
    Return the update of one row of a model for the given column values.

    :param model_cls: updated model
    :type model_cls: Type[Any]
    :param values: values by column name
    :type values: Mapping[str, Any]
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: WriteTemplate
    """
    names = tuple(sorted(values))
    table = model_cls.ormar_config.table
    statement = cached_statement(
        model_cls,
        ("update", names),
        build_update,
        table,
        names,
        primary_key_column(model_cls),
    )
    return WriteTemplate(statement, {**value_params(names, values), PK_SLOT: pk})


def delete_template(model_cls: Type[Any], pk: Any) -> WriteTemplate:
    """
    Return the delete of one row of a model.

    :param model_cls: model
    :type model_cls: Type[Any]
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: WriteTemplate
    """
    statement = cached_statement(
        model_cls,
        ("delete",),
        build_delete,
        model_cls.ormar_config.table,
        primary_key_column(model_cls),
    )
    return WriteTemplate(statement, {PK_SLOT: pk})


def pk_select_template(model_cls: Type[Any], pk: Any) -> WriteTemplate:
    """
    Return the select of one row of a model by primary key.

    :param model_cls: model
    :type model_cls: Type[Any]
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: WriteTemplate
    """
    statement = cached_statement(
        model_cls,
        ("select_pk",),
        build_pk_select,
        model_cls.ormar_config.table,
        primary_key_column(model_cls),
    )
    return WriteTemplate(statement, {PK_SLOT: pk})
