"""
Read statement templates: per-call values become named bind parameters so a
statement built once per query shape can be reused with new values.
"""

import os
from collections import OrderedDict
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
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
