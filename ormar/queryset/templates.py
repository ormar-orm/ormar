"""
Read statement templates: a select built once per query shape and reused with
new values on every later call.

Most of the cost of a simple read is building the statement: ormar walks the
filters, joins, selected fields and ordering, and SQLAlchemy assembles the
expression tree. Two calls that differ only in their values build the same
tree, so ``QuerySet`` builds it once with named bind parameters ("slots") in
place of the values and keeps it in a cache of the model::

    await Author.objects.filter(name="a").limit(5).all()
    # built and cached: SELECT ... WHERE authors.name = :ormar_p0 LIMIT :ormar_limit
    # bound this call:  {"ormar_p0": "a", "ormar_limit": 5}

    await Author.objects.filter(name="b").limit(10).all()
    # same shape, cached statement reused: {"ormar_p0": "b", "ormar_limit": 10}

The cache key (the "shape", see ``QuerySet.template_key``) holds everything
that changes the SQL text and none of the bound values: filtered columns,
operators, and/or/not nesting, ``select_related``, selected fields, ordering
and whether a limit or offset is present. Values SQLAlchemy writes into the
SQL text instead of binding (``None`` as ``IS NULL``, booleans, ``isnull``)
are part of the shape as well.

Running the same statement object every time also lets the native compiler
reuse its plan for it instead of compiling the statement again.

With ``ORMAR_TEMPLATE_CROSS_CHECK=1`` (set by the test suite) every cache hit
also builds the statement the old way and fails if the two would run
different SQL, values or bind types.
"""

import os
import re
import weakref
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

# fixed slot names of LIMIT and OFFSET; filter values are named ``ormar_p{n}``
LIMIT_SLOT = "ormar_limit"
OFFSET_SLOT = "ormar_offset"
# numbered placeholders in rendered SQL: ``$1`` (asyncpg) or ``:1`` (numeric)
NUMBERED = re.compile(r"[$:](\d+)")
# statement shapes kept per model before the least recently used is dropped
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
# Test-suite safety net: every cache hit is also built without templates and
# compared with the cached statement (see ``assert_same_statement``).
CROSS_CHECK = os.environ.get("ORMAR_TEMPLATE_CROSS_CHECK") == "1"


class SelectTemplate(NamedTuple):
    """
    A read statement from the template cache and the values to run it with for
    one call::

        template = queryset.filter(name="a").limit(5).build_select_template()
        template.params  # -> {"ormar_p0": "a", "ormar_limit": 5}
        await executor.fetch_all(template.statement, template.params)
    """

    statement: Executable
    params: Dict[str, Any]


class TemplateCache:
    """
    Least-recently-used cache of one model's statements, keyed by shape.

    The size is bounded so an application that builds queries from many
    different filter combinations cannot grow memory without limit: once
    ``max_size`` shapes are cached, adding one drops the shape used longest ago.
    """

    def __init__(self, max_size: int = TEMPLATE_CACHE_SIZE) -> None:
        """
        :param max_size: number of statement shapes kept
        :type max_size: int
        """
        self.max_size = max_size
        self.entries: "OrderedDict[Any, Any]" = OrderedDict()

    def get(self, key: Any) -> Any:
        """
        Return the statement cached for a shape, or None, and mark it as the
        most recently used.

        Another thread using the same model may evict the entry between reading
        it and marking it, so that case is ignored: the statement is still
        valid for this call.

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
        Cache the statement of a shape. When the cache is full, the shape used
        longest ago is dropped.

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


# one cache per model class; weak keys let model classes created at runtime
# (e.g. inside tests) be garbage collected together with their statements
TEMPLATE_CACHES: "weakref.WeakKeyDictionary[type, TemplateCache]" = (
    weakref.WeakKeyDictionary()
)


def template_cache_for(model_cls: Type[Any]) -> TemplateCache:
    """
    Return the statement template cache of a model class, creating it on the
    model's first read. ``setdefault`` keeps a single cache when two threads
    create it at the same time.

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
    ``SlotBinder`` names::

        Author.objects.filter(name="a", score__gt=5).limit(10)
        # -> {"ormar_p0": "a", "ormar_p1": 5, "ormar_limit": 10}

    Actions whose value is written into the SQL text (see
    ``FilterAction.binds_slot``) get no entry, but still use up their index,
    so the names of the other slots do not depend on those values. The offset
    is only bound when non-zero, matching ``OffsetQuery``, which leaves out
    ``OFFSET 0``.

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

    Only runs when ``CROSS_CHECK`` is on. It catches a shape that misses
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


def iter_actions(
    clauses: List[Union["FilterAction", "FilterGroup"]],
) -> Iterator["FilterAction"]:
    """
    Yield the filter actions of a clause list in the order their SQL is built:
    a group's nested groups first, then its own actions (the order
    ``FilterGroup.get_text_clause`` emits them in)::

        filter(ormar.or_(ormar.and_(score=1, id=2), name="a"))
        # -> score=1, id=2, name="a"

    The order is the same for every queryset of one shape, so an action's
    index here can name its slot.

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
    first, then exclude clauses, each in build order::

        Author.objects.exclude(score=0).filter(name="a")
        # -> name="a" (ormar_p0), score=0 (ormar_p1)

    ``SlotBinder`` (naming the slots while building) and ``template_params``
    (collecting values on every call) both use this list, so a value always
    ends up in the slot built for it.

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
    Turns per-call values of a read statement into named bind parameters while
    the statement is built. A filter action's value is named ``ormar_p{n}``
    after its position among the canonical actions, limit and offset get fixed
    names::

        binder = SlotBinder(actions=[name_action, score_action])
        binder.bind(action=score_action, value=5)  # -> :ormar_p1
        binder.limit(value=10)  # -> :ormar_limit

    Slots are looked up by action object, not by order of use, because one
    action can be emitted twice: with ``select_related`` and a limit, the
    filters go into the pagination subquery and the outer query, and both
    copies bind the same ``ormar_p{n}``, so one value fills both.

    Passed down through ``Query``, ``FilterQuery``, ``LimitQuery`` and
    ``OffsetQuery``; without a binder they bind the plain values as before.
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

        The parameter is built the way SQLAlchemy builds the bind of
        ``column <op> value``: it is compared to the column's type, so the value
        gets the column's bind processing (dates, JSON, enums and so on), and
        an ``in`` filter is expanding, so it renders one placeholder per item of
        the list it is given on each call.

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
        Return the named bind parameter for a limit, typed as an integer like
        the one SQLAlchemy creates for ``select.limit(10)``.

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
        Return the named bind parameter for an offset, typed as an integer like
        the one SQLAlchemy creates for ``select.offset(10)``.

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
    Return the part of a filter value that changes the SQL text::

        filter(name__isnull=True)  # -> ("isnull", True)   IS NULL
        filter(name=None)  # -> ("literal", None)  IS NULL
        filter(active=True)  # -> ("literal", True)  literal true
        filter(id__in=[1, 2])  # -> "in"               IN (expanding)
        filter(name="a")  # -> ("value", str)     = :ormar_p0

    Values written into the SQL text are part of the shape as they are. For a
    bound value only its type is kept, so values of different types never
    share a statement.

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
    Return everything about a filter action that changes the SQL text: the
    field path, the table alias of the joined relation, the operator, the
    kind of value (see ``value_kind``) and whether LIKE needs an ``ESCAPE``
    clause for ``%`` or ``_`` in the value.

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
    the and/or/not nesting, since ``or_(a, b)`` and ``and_(a, b)`` build
    different SQL from the same actions::

        filter(ormar.or_(name="a", score=1))
        # -> (("group", FilterType.OR, False, (), (<name shape>, <score shape>)),)

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
    Return the fields included and excluded per model and relation, which
    decide the selected columns (``fields()``, ``exclude_fields()``,
    ``values()``). Field sets are sorted so the same selection always gives
    the same shape.

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
    Return the shape of the requested orderings: field path, direction, table
    alias and nulls placement of each, in order::

        order_by("-name")  # -> (("-name", "desc", "", None),)

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
    Wrap a read select into ``SELECT EXISTS (...)``, the statement run by
    ``QuerySet.exists()``.

    This and the other ``*_statement`` helpers are the ``wrap`` callables of
    ``QuerySet.build_select_template``: they run on the select once, when a
    shape is first built, and the wrapped statement is what gets cached.

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
