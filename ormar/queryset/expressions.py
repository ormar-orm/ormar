"""
Column expressions: arithmetic and functions over columns evaluated by the
database, used as filter values and as values of ``QuerySet.update()``::

    Slot.objects.filter(taken__lt=F("capacity"))
    Slot.objects.filter(id=1).update(taken=Slot.taken + 1)
    Member.objects.filter(id=1).update(read_id=Greatest(F("read_id"), 10))
    Player.objects.update(each=True, score=Random())

``Model.field`` accessors are expressions themselves, ``F("path")`` names a
field by its path. Expression classes define only dunders and underscored
names, so they never shadow a field reached through a ``FieldAccessor``.
"""

import itertools
import operator
from typing import TYPE_CHECKING, Any, Callable, Iterator, Mapping, Optional

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.functions import GenericFunction, ReturnTypeFromArgs, coalesce
from sqlalchemy.types import Float

from ormar.exceptions import QueryDefinitionError

if TYPE_CHECKING:  # pragma: no cover
    from ormar.queryset.actions.query_action import QueryAction


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
    Field by its path from the queried model: ``F("capacity")``, or in filters
    also a field of a related model (``filter(taken__lt=F("owner__limit"))``).
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


class Function(Expression):
    """
    SQL function of two or more arguments, at least one of them an expression;
    the others may be literals.
    """

    _sql_name = ""

    def __init__(self, *arguments: Any) -> None:
        """
        :param arguments: expressions and literals
        :type arguments: Any
        :raises QueryDefinitionError: if there are less than two arguments, none
            of them is an expression or a literal is None or a bool
        """
        name = type(self).__name__
        if len(arguments) < 2:
            raise QueryDefinitionError(f"{name}() takes at least two arguments")
        if not any(isinstance(argument, Expression) for argument in arguments):
            raise QueryDefinitionError(
                f"{name}() needs a field or an expression among its arguments"
            )
        if any(
            argument is None or isinstance(argument, bool) for argument in arguments
        ):
            raise QueryDefinitionError(f"{name}() takes no None or bool arguments")
        self._arguments = tuple(
            argument._node() if isinstance(argument, Expression) else argument
            for argument in arguments
        )


class Greatest(Function):
    """
    The largest of the arguments. PostgreSQL ignores ``NULL`` arguments,
    MySQL and SQLite return ``NULL`` when any argument is ``NULL``.
    """

    _sql_name = "greatest"


class Least(Function):
    """
    The smallest of the arguments. PostgreSQL ignores ``NULL`` arguments,
    MySQL and SQLite return ``NULL`` when any argument is ``NULL``.
    """

    _sql_name = "least"


class Coalesce(Function):
    """The first argument that is not ``NULL``."""

    _sql_name = "coalesce"


class Random(Expression):
    """
    Random float from 0 (inclusive) to 1 (exclusive), drawn by the database
    for each row: ``update(each=True, score=Random())``, ``Random() * 100``.
    """


class GreatestOf(ReturnTypeFromArgs):
    """SQL ``greatest()``, the scalar ``max()`` on SQLite."""

    name = "greatest"
    inherit_cache = True
    _register = False


class LeastOf(ReturnTypeFromArgs):
    """SQL ``least()``, the scalar ``min()`` on SQLite."""

    name = "least"
    inherit_cache = True
    _register = False


@compiles(GreatestOf, "sqlite")
def compile_greatest_on_sqlite(
    element: GreatestOf, compiler: SQLCompiler, **kwargs: Any
) -> str:
    """
    SQLite has no ``greatest()``, its ``max()`` of several arguments is the same
    function.
    """
    return f"max({compiler.process(element.clauses, **kwargs)})"


@compiles(LeastOf, "sqlite")
def compile_least_on_sqlite(
    element: LeastOf, compiler: SQLCompiler, **kwargs: Any
) -> str:
    """
    SQLite has no ``least()``, its ``min()`` of several arguments is the same
    function.
    """
    return f"min({compiler.process(element.clauses, **kwargs)})"


class RandomFloat(GenericFunction):
    """
    SQL ``random()`` returning a float in ``[0, 1)``: ``rand()`` on MySQL, the
    low 53 bits of SQLite's 64-bit integer ``random()`` scaled into the range.
    """

    name = "random"
    type = Float()
    inherit_cache = True
    _register = False


@compiles(RandomFloat, "mysql")
def compile_random_on_mysql(
    element: RandomFloat, compiler: SQLCompiler, **kwargs: Any
) -> str:
    """MySQL names the function ``rand()``."""
    return "rand()"


@compiles(RandomFloat, "sqlite")
def compile_random_on_sqlite(
    element: RandomFloat, compiler: SQLCompiler, **kwargs: Any
) -> str:
    """
    SQLite's ``random()`` is a 64-bit integer; its remainder by 2**53 is exact
    as a double, so the absolute value divided by 2**53 stays below 1.
    """
    return "(abs(random() % 9007199254740992) / 9007199254740992.0)"


SQL_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "greatest": GreatestOf,
    "least": LeastOf,
    "coalesce": coalesce,
}


def expression_leaves(node: Any) -> Iterator[Any]:
    """
    Yield the fields (``F``) and literals of an expression, left to right;
    ``Random()`` is neither.

    :param node: expression node
    :type node: Any
    :return: leaves in order
    :rtype: Iterator[Any]
    """
    if isinstance(node, Combined):
        yield from expression_leaves(node=node._left)
        yield from expression_leaves(node=node._right)
    elif isinstance(node, Function):
        for argument in node._arguments:
            yield from expression_leaves(node=argument)
    elif not isinstance(node, Random):
        yield node


def expression_key(node: Any) -> Any:
    """
    Return the shape of an expression for template cache keys: operators,
    fields and the types of the literals, which decide how they are bound::

        F("a") + 1  # -> (operator.add, ("f", "a"), int)
        Greatest(F("a"), 0)  # -> ("greatest", (("f", "a"), int))
        Random()  # -> ("random",)

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
    if isinstance(node, Function):
        return (
            node._sql_name,
            tuple(expression_key(node=argument) for argument in node._arguments),
        )
    if isinstance(node, Random):
        return ("random",)
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
    columns: Mapping[str, "QueryAction"],
    slot: Optional[str],
    index: Optional[Iterator[int]] = None,
) -> Any:
    """
    Build the SQL of an expression, reading each field from the aliased column
    of its action in ``columns``. With a ``slot`` the literals become bind
    parameters named ``{slot}_{k}`` and typed from the left operand and the
    literal (``release_values`` drops the literal afterwards), without one
    they stay plain values.

    :param node: expression node
    :type node: Any
    :param columns: resolved field actions by field path
        (see ``expression_columns``)
    :type columns: Mapping[str, QueryAction]
    :param slot: slot name of a statement template, if built for one
    :type slot: Optional[str]
    :param index: counter of the literals already named
    :type index: Optional[Iterator[int]]
    :return: SQL expression
    :rtype: Any
    """
    if isinstance(node, F):
        return columns[node._path].aliased_column
    if isinstance(node, Random):
        return RandomFloat()
    index = index or itertools.count()
    if isinstance(node, Function):
        return function_clause(node=node, columns=columns, slot=slot, index=index)
    left = expression_clause(node=node._left, columns=columns, slot=slot, index=index)
    right = node._right
    if isinstance(right, Expression):
        right = expression_clause(node=right, columns=columns, slot=slot, index=index)
    elif slot is not None:
        right = BindParameter(
            key=f"{slot}_{next(index)}",
            value=right,
            _compared_to_operator=node._op,
            _compared_to_type=left.type,
        )
    return node._op(left, right)


def function_clause(
    node: Function,
    columns: Mapping[str, "QueryAction"],
    slot: Optional[str],
    index: Iterator[int],
) -> Any:
    """
    Build the SQL of a function. Literal arguments are bound like the literals
    of ``expression_clause``: named ``{slot}_{k}`` in order and typed from the
    first expression argument and the literal, so ``Greatest(F("count"), 2.5)``
    binds a float.

    :param node: function node
    :type node: Function
    :param columns: resolved field actions by field path
    :type columns: Mapping[str, QueryAction]
    :param slot: slot name of a statement template, if built for one
    :type slot: Optional[str]
    :param index: counter of the literals already named
    :type index: Iterator[int]
    :return: SQL function call
    :rtype: Any
    """
    # literals are named in argument order, but typed after the first
    # expression argument, which may come later
    expression_clauses: dict[int, Any] = {}
    literal_keys: dict[int, Optional[str]] = {}
    for position, argument in enumerate(node._arguments):
        if isinstance(argument, Expression):
            expression_clauses[position] = expression_clause(
                node=argument, columns=columns, slot=slot, index=index
            )
        else:
            literal_keys[position] = f"{slot}_{next(index)}" if slot else None
    reference_type = next(iter(expression_clauses.values())).type
    arguments = [
        expression_clauses[position]
        if position in expression_clauses
        else BindParameter(
            key=literal_keys[position],
            value=argument,
            unique=literal_keys[position] is None,
            _compared_to_operator=sql_operators.eq,
            _compared_to_type=reference_type,
        )
        for position, argument in enumerate(node._arguments)
    ]
    return SQL_FUNCTIONS[node._sql_name](*arguments)
