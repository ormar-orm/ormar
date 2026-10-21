"""
Statement templates: a statement built once per query shape and reused with
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

Writes are cached the same way. ``Model.save()``, ``update()``, ``delete()``,
``load()`` and ``upsert()`` run one statement per model and set of written
columns, binding the values as ``ormar_v{i}`` (in sorted column order) and the
primary key as ``ormar_pk``::

    await Author(name="a", score=1).save()
    # INSERT INTO authors (name, score) VALUES (:ormar_v0, :ormar_v1)
    # {"ormar_v0": "a", "ormar_v1": 1}

``QuerySet.update()`` and ``delete()`` bind both filter and value slots, and
``bulk_create`` and ``bulk_update`` reuse one statement per set of columns for
all their rows.

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

# fixed slot names of LIMIT and OFFSET; filter values are named ``ormar_p{n}``
LIMIT_SLOT = "ormar_limit"
OFFSET_SLOT = "ormar_offset"
# slot name of the primary key in single-row writes and ``load()``
PK_SLOT = "ormar_pk"
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


class StatementTemplate(NamedTuple):
    """
    A statement from the template cache and the values to run it with for one
    call::

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


def value_types(values: dict[str, Any]) -> dict[str, type]:
    """
    Return the type of each value, so values that compare equal across types
    (``1 == 1.0 == True``, str enums and strings) are told apart::

        {"a": 1} == {"a": True}  # -> True
        value_types(values={"a": 1}) == value_types(values={"a": True})  # -> False

    The cross checks of trusted row construction and ``dump_for_save`` compare
    both the values and their types.

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


# primary key column per model class, looked up on the first write
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

    A write's shape is small (the kind of statement and the written column
    names), so callers pass the key directly. The statement is only built on
    a miss, from ``builder(**builder_kwargs)``::

        cached_statement(
            model_cls=Author,
            key=("insert", ("name", "score")),
            builder=build_insert,
            table=Author.ormar_config.table,
            names=("name", "score"),
        )

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
    with literal values, as without templates. Catches a write shape that
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
    if CROSS_CHECK:
        assert_same_statement(
            cached=statement,
            fresh=literal,
            params=params,
            dialect=model_cls.ormar_config.database.dialect,
        )


def primary_key_column(model_cls: Type[Any]) -> sqlalchemy.Column:
    """
    Return the primary key column of a model's table, resolved once per model,
    so a write does not look up the column alias of the primary key field
    every time.

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
    Return the named bind parameter of a primary key filter
    (``WHERE authors.id = :ormar_pk``), typed like the column so the key gets
    the column's bind processing (e.g. UUID).

    :param column: primary key column
    :type column: sqlalchemy.Column
    :return: named bind parameter
    :rtype: BindParameter
    """
    return sqlalchemy.bindparam(key=PK_SLOT, type_=column.type)


def value_binds(table: sqlalchemy.Table, names: Tuple[str, ...]) -> Dict[Any, Any]:
    """
    Return named bind parameters for the written columns, ``ormar_v{i}`` in
    the given column order, typed like their columns::

        value_binds(table=authors, names=("name", "score"))
        # -> {authors.c.name: :ormar_v0, authors.c.score: :ormar_v1}

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
    Return the written values keyed by their bind parameter names, matching
    ``value_binds`` for the same ``names``::

        value_params(names=("name", "score"), values={"score": 1, "name": "a"})
        # -> {"ormar_v0": "a", "ormar_v1": 1}

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
    Only the cross check uses it, to build the statement with literal values
    as without templates.

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
    Build an insert of the given columns with named value binds::

        INSERT INTO authors (name, score) VALUES (:ormar_v0, :ormar_v1)

    The names are sorted by the caller, so the same columns given in any
    order share one statement.

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
    Build an update of the given columns of one row, by primary key::

        UPDATE authors SET name=:ormar_v0, score=:ormar_v1
        WHERE authors.id = :ormar_pk

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
    Build a delete of one row, by primary key::

        DELETE FROM authors WHERE authors.id = :ormar_pk

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
    Build a select of all columns of one row, by primary key::

        SELECT authors.id, authors.name, ... FROM authors
        WHERE authors.id = :ormar_pk

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
    key come from ``new_<column>`` parameters of each row, and the driver's
    ``executemany`` runs it once per row::

        UPDATE authors SET name=:new_name WHERE authors.id = :new_id
        # rows: [{"new_id": 1, "new_name": "a"}, {"new_id": 2, "new_name": "b"}]

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
    Return the insert of a model for the given sorted column names, from the
    model's template cache. ``save()`` binds one row to it and
    ``bulk_create`` runs it with ``executemany`` for every row with the same
    columns.

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
    Return the insert of a model for the given column values, used by
    ``save()`` and one-row ``bulk_create``::

        insert_template(model_cls=Author, values={"name": "a", "score": 1})
        # -> INSERT INTO authors (name, score) VALUES (:ormar_v0, :ormar_v1)
        #    {"ormar_v0": "a", "ormar_v1": 1}

    Rows that leave out the primary key or a column with a server default
    write a different set of columns, so each set gets its own statement.

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
    Return the update of one row of a model for the given column values, used
    by ``Model.update()``::

        update_template(model_cls=Author, values={"name": "b"}, pk=1)
        # -> UPDATE authors SET name=:ormar_v0 WHERE authors.id = :ormar_pk
        #    {"ormar_v0": "b", "ormar_pk": 1}

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
    Return the delete of one row of a model, used by ``Model.delete()``::

        delete_template(model_cls=Author, pk=1)
        # -> DELETE FROM authors WHERE authors.id = :ormar_pk, {"ormar_pk": 1}

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
    Return the select of one row of a model by primary key, used by
    ``Model.load()`` and the existence check of ``upsert(force_save=True)``::

        pk_select_template(model_cls=Author, pk=1)
        # -> SELECT ... FROM authors WHERE authors.id = :ormar_pk, {"ormar_pk": 1}

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
