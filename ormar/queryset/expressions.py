"""
Column expressions: arithmetic over columns evaluated by the database, used as
filter values and as values of ``QuerySet.update()``::

    Slot.objects.filter(taken__lt=F("capacity"))
    Slot.objects.filter(id=1).update(taken=Slot.taken + 1)

``Model.field`` accessors are expressions themselves, ``F("path")`` names a
field by its path. Expression classes define only dunders and underscored
names, so they never shadow a field reached through a ``FieldAccessor``.
"""

import itertools
import operator
from typing import TYPE_CHECKING, Any, Callable, Iterator, Optional

import sqlalchemy
from sqlalchemy.sql.elements import BindParameter

from ormar.exceptions import QueryDefinitionError

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model


class Expression:
    """
    Base of column expressions, combined with ``+``, ``-`` and ``*``. The
    literal operand has to be on the right side (``F("a") + 1``).
    """

    def _node(self) -> "Expression":
        """
        :return: the expression as a node of an expression tree
        :rtype: Expression
        """
        return self

    def _combine(self, op: Callable[[Any, Any], Any], other: Any) -> "Combined":
        """
        :param op: ``operator.add``, ``operator.sub`` or ``operator.mul``
        :type op: Callable[[Any, Any], Any]
        :param other: right operand, an expression or a literal
        :type other: Any
        :raises QueryDefinitionError: if the operand is None or a bool
        :return: the combined expression
        :rtype: Combined
        """
        if other is None or isinstance(other, bool):
            raise QueryDefinitionError("Expressions take no None or bool operands")
        if isinstance(other, Expression):
            other = other._node()
        return Combined(left=self._node(), op=op, right=other)

    def __add__(self, other: Any) -> "Combined":
        """SQL ``expression + other``"""
        return self._combine(op=operator.add, other=other)

    def __sub__(self, other: Any) -> "Combined":
        """SQL ``expression - other``"""
        return self._combine(op=operator.sub, other=other)

    def __mul__(self, other: Any) -> "Combined":
        """SQL ``expression * other``"""
        return self._combine(op=operator.mul, other=other)


class F(Expression):
    """
    Field of the model a filter or an update applies to, by its path:
    ``F("capacity")``, or ``F("owner__limit")`` when compared with another field
    of the same related model (``filter(owner__score__gt=F("owner__limit"))``).
    """

    def __init__(self, path: str) -> None:
        """
        :param path: field name, prefixed by its relation path if any
        :type path: str
        """
        self._path = path


class Combined(Expression):
    """Two operands combined with an arithmetic operator."""

    def __init__(
        self, left: Expression, op: Callable[[Any, Any], Any], right: Any
    ) -> None:
        """
        :param left: left operand
        :type left: Expression
        :param op: arithmetic operator
        :type op: Callable[[Any, Any], Any]
        :param right: right operand, an expression or a literal
        :type right: Any
        """
        self._left = left
        self._op = op
        self._right = right


def expression_leaves(node: Any) -> Iterator[Any]:
    """
    Yield the fields (``F``) and literals of an expression, left to right.

    :param node: expression node
    :type node: Any
    :return: leaves in order
    :rtype: Iterator[Any]
    """
    if isinstance(node, Combined):
        yield from expression_leaves(node=node._left)
        yield from expression_leaves(node=node._right)
    else:
        yield node


def expression_fields(node: Any, model: type["Model"], prefix: list[str]) -> list[str]:
    """
    Return the names of the fields an expression reads, checking that each one
    is a non-relation field of ``model`` reached through ``prefix``, the
    relation path of the compared field (``[]`` in updates).

    :param node: expression node
    :type node: Any
    :param model: model owning the fields
    :type model: type[Model]
    :param prefix: relation path every field path has to start with
    :type prefix: list[str]
    :raises QueryDefinitionError: if a field is missing, is a relation or is
        on another model
    :return: field names
    :rtype: list[str]
    """
    names = []
    for leaf in expression_leaves(node=node):
        if isinstance(leaf, F):
            *parts, name = leaf._path.split("__")
            field = model.ormar_config.model_fields.get(name)
            if parts != prefix or field is None or field.is_relation:
                raise QueryDefinitionError(
                    f"F('{leaf._path}') has to name a non-relation field of the "
                    f"model it is compared with or written to."
                )
            names.append(name)
    return names


def expression_key(node: Any) -> Any:
    """
    Return the shape of an expression for template cache keys: operators,
    fields and the types of the literals, which decide how they are bound::

        F("a") + 1  # -> (operator.add, ("f", "a"), int)

    :param node: expression node
    :type node: Any
    :return: hashable shape
    :rtype: Any
    """
    if isinstance(node, Combined):
        return (
            node._op,
            expression_key(node=node._left),
            expression_key(node=node._right),
        )
    return ("f", node._path) if isinstance(node, F) else type(node)


def expression_params(value: Any, slot: str) -> dict[str, Any]:
    """
    Return the values bound for a slot: the value itself, or for an expression
    its literals named ``{slot}_{k}`` (as ``expression_clause`` names them).

    :param value: bound value or expression
    :type value: Any
    :param slot: name of the slot
    :type slot: str
    :return: values by bind parameter name
    :rtype: dict[str, Any]
    """
    if not isinstance(value, Expression):
        return {slot: value}
    literals = [
        leaf for leaf in expression_leaves(node=value) if not isinstance(leaf, F)
    ]
    return {f"{slot}_{index}": literal for index, literal in enumerate(literals)}


def expression_clause(
    node: Any,
    model: type["Model"],
    table: sqlalchemy.Table,
    slot: Optional[str],
    index: Optional[Iterator[int]] = None,
) -> Any:
    """
    Build the SQL of an expression over the columns of ``table``. With a
    ``slot`` the literals become named bind parameters ``{slot}_{k}``, typed
    like SQLAlchemy types the literal of ``column + literal``, without one
    they stay plain values.

    :param node: expression node
    :type node: Any
    :param model: model owning the fields
    :type model: type[Model]
    :param table: table (or its alias) the columns are taken from
    :type table: sqlalchemy.Table
    :param slot: slot name of a statement template, if built for one
    :type slot: Optional[str]
    :param index: counter of the literals already named
    :type index: Optional[Iterator[int]]
    :return: SQL expression
    :rtype: Any
    """
    if isinstance(node, F):
        return table.c[model.get_column_alias(node._path.split("__")[-1])]
    index = index if index is not None else itertools.count()
    left = expression_clause(
        node=node._left, model=model, table=table, slot=slot, index=index
    )
    right = node._right
    if isinstance(right, Expression):
        right = expression_clause(
            node=right, model=model, table=table, slot=slot, index=index
        )
    elif slot is not None:
        right = BindParameter(
            key=f"{slot}_{next(index)}",
            value=right,
            _compared_to_operator=node._op,
            _compared_to_type=left.type,
        )
    return node._op(left, right)
