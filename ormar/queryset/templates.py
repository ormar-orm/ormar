"""
Read statement templates: per-call values become named bind parameters so a
statement built once per query shape can be reused with new values.
"""

from typing import TYPE_CHECKING, Any, Iterator, List, Union

import sqlalchemy
from sqlalchemy.sql.elements import BindParameter

if TYPE_CHECKING:  # pragma: no cover
    from ormar.queryset.actions.filter_action import FilterAction
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
