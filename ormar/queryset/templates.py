"""
Read statement templates: per-call values become named bind parameters so a
statement built once per query shape can be reused with new values.
"""

from typing import TYPE_CHECKING, Any, Iterable, Iterator, List, Tuple, Union

import sqlalchemy
from sqlalchemy.sql.elements import BindParameter

if TYPE_CHECKING:  # pragma: no cover
    from ormar.models.excludable import ExcludableItems
    from ormar.queryset.actions.filter_action import FilterAction
    from ormar.queryset.actions.order_action import OrderAction
    from ormar.queryset.clause import FilterGroup

LIMIT_SLOT = "ormar_limit"
OFFSET_SLOT = "ormar_offset"


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
