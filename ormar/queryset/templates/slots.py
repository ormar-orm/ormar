"""
Slots: named bind parameters that take the per-call values of a statement
template.

While a template is built, ``SlotBinder`` puts slots in place of the values
(filters as ``ormar_p{n}``, limit and offset as ``ormar_limit`` and
``ormar_offset``). Writes bind values as ``ormar_v{i}`` and the primary key as
``ormar_pk``. On every call the values are collected under the same names::

    SELECT ... WHERE authors.name = :ormar_p0 LIMIT :ormar_limit
    # {"ormar_p0": "a", "ormar_limit": 5}
"""

from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import sqlalchemy
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.sqltypes import TupleType

from ormar.queryset.actions.filter_action import FILTER_OPERATORS

if TYPE_CHECKING:  # pragma: no cover
    from ormar.queryset.actions.filter_action import FilterAction
    from ormar.queryset.clause import FilterGroup


# fixed slot names of LIMIT and OFFSET; filter values are named ``ormar_p{n}``
LIMIT_SLOT = "ormar_limit"


OFFSET_SLOT = "ormar_offset"


# slot name of the primary key in single-row writes and ``load()``
PK_SLOT = "ormar_pk"


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


def iter_actions(
    clauses: List[Union["FilterAction", "FilterGroup"]],
) -> Iterator["FilterAction"]:
    """
    Yield the filter actions of a clause list in the order their SQL is built:
    a group's nested groups first, then its own actions (the order
    ``FilterGroup.get_text_clause`` emits them in)::

        filter(ormar.or_(ormar.and_(score=1, id=2), name="a"))
        # -> score=1, id=2, name="a"

    The order is the same for every queryset of one cache key, so an action's
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

    def bind_key_in(
        self, action: "FilterAction", value: Any, column_types: List[Any]
    ) -> BindParameter:
        """
        Return the expanding bind parameter of a row-value ``IN`` over several
        key columns, typed as a tuple of the column types so each part gets its
        column's bind processing::

            (orders.tenant_id, orders.number) IN (:ormar_p0)
            # rendered per call as IN ((?, ?), (?, ?))

        :param action: the key ``IN`` filter action
        :type action: FilterAction
        :param value: key values, one tuple of parts per key
        :type value: Any
        :param column_types: types of the key columns, in key order
        :type column_types: List[Any]
        :return: named expanding bind parameter
        :rtype: BindParameter
        """
        return BindParameter(
            key=self.slots[id(action)],
            value=value,
            expanding=True,
            type_=TupleType(*column_types),
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
