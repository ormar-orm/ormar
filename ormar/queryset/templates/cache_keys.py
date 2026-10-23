"""
Cache keys of statement templates: hashable values holding everything about
a queryset that changes its SQL text and none of its bound values.

Two querysets with equal keys must build the same SQL, or one would run the
other's statement. ``QuerySet.select_cache_key`` combines these keys for
reads, ``QuerySet.build_write_template`` for filtered updates and deletes::

    Author.objects.filter(name="a")  # same key as filter(name="b")
    Author.objects.filter(name=None)  # different key: renders IS NULL
"""

from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Tuple,
)

if TYPE_CHECKING:  # pragma: no cover
    from ormar.models.excludable import ExcludableItems
    from ormar.queryset.actions.filter_action import FilterAction
    from ormar.queryset.actions.order_action import OrderAction


def value_cache_key(action: "FilterAction") -> Any:
    """
    Return the part of a filter value that changes the SQL text::

        filter(name__isnull=True)  # -> ("isnull", True)   IS NULL
        filter(name=None)  # -> ("literal", None)  IS NULL
        filter(active=True)  # -> ("literal", True)  literal true
        filter(id__in=[1, 2])  # -> "in"               IN (expanding)
        filter(name="a")  # -> ("value", str)     = :ormar_p0

    Values written into the SQL text are part of the key as they are. For a
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


def action_cache_key(action: "FilterAction") -> Tuple[Any, ...]:
    """
    Return everything about a filter action that changes the SQL text: the
    field path, the table alias of the joined relation, the operator, the
    kind of value (see ``value_cache_key``) and whether LIKE needs an ``ESCAPE``
    clause for ``%`` or ``_`` in the value.

    :param action: filter action
    :type action: FilterAction
    :return: hashable cache key of the action
    :rtype: Tuple[Any, ...]
    """
    return (
        action.query_str,
        action.table_prefix,
        action.operator,
        value_cache_key(action=action),
        action.has_escaped_character,
    )


def clauses_cache_key(clauses: Iterable[Any]) -> Tuple[Any, ...]:
    """
    Return the cache key of a list of filter actions and filter groups, keeping
    the and/or/not nesting, since ``or_(a, b)`` and ``and_(a, b)`` build
    different SQL from the same actions::

        filter(ormar.or_(name="a", score=1))
        # -> (("group", FilterType.OR, False, (), (<name key>, <score key>)),)

    :param clauses: filter actions and filter groups
    :type clauses: Iterable[Any]
    :return: hashable cache key of the clauses
    :rtype: Tuple[Any, ...]
    """
    return tuple(
        (
            "group",
            clause.filter_type,
            clause.exclude,
            clauses_cache_key(clauses=clause._nested_groups),
            tuple(action_cache_key(action=action) for action in clause.actions),
        )
        if hasattr(clause, "_nested_groups")
        else action_cache_key(action=clause)
        for clause in clauses
    )


def excludable_cache_key(excludable: "ExcludableItems") -> Tuple[Any, ...]:
    """
    Return the fields included and excluded per model and relation, which
    decide the selected columns (``fields()``, ``exclude_fields()``,
    ``values()``). Field sets are sorted so the same selection always gives
    the same key.

    :param excludable: queryset's fields to include and exclude
    :type excludable: ExcludableItems
    :return: hashable cache key of the projection
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


def order_cache_key(order_bys: Iterable["OrderAction"]) -> Tuple[Any, ...]:
    """
    Return the cache key of the requested orderings: field path, direction, table
    alias and nulls placement of each, in order::

        order_by("-name")  # -> (("-name", "desc", "", None),)

    :param order_bys: order actions of the query
    :type order_bys: Iterable[OrderAction]
    :return: hashable cache key of the ordering
    :rtype: Tuple[Any, ...]
    """
    return tuple(
        (order.query_str, order.direction, order.table_prefix, order.nulls_ordering)
        for order in order_bys
    )
