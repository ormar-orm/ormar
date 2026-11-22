"""
Column expressions: arithmetic and functions over columns evaluated by the
database, used as filter values and as values of ``QuerySet.update()``::

    Slot.objects.filter(taken__lt=F("capacity"))
    Slot.objects.filter(id=1).update(taken=Slot.taken + 1)
    Member.objects.filter(id=1).update(read_id=Greatest(F("read_id"), 10))
    Member.objects.filter(name=Func("lower", F("nick")))
    Player.objects.update(each=True, score=Random())

and in ``update_values`` of upserts, where ``Inserted("path")`` is the value
the insert tried to write::

    Level.objects.create(
        ...,
        on_conflict="update",
        update_values={
            "completed": F("completed") + 1,
            "read_id": Greatest(F("read_id"), Inserted("read_id")),
        },
    )

``F()``, arithmetic and functions can also be the filtered side, compared
with ``==``, ``!=``, ``<``, ``<=``, ``>``, ``>=`` or the lookup methods of
``Comparable``, which return filter groups like ``ormar.and_()``::

    Member.objects.filter(Func("lower", F("nick")) == "ann")
    Member.objects.filter(Func("lower", F("team__name")).icontains("red"))
    Slot.objects.exclude((F("capacity") - F("taken")) < 2)

``Model.field`` accessors are expressions themselves, ``F("path")`` names a
field by its path. ``Expression`` defines only dunders and underscored names,
so it never shadows a field reached through a ``FieldAccessor``, which has
lookup methods of its own instead of ``Comparable``.
"""

import copy
import itertools
import operator
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterator,
    Mapping,
    NamedTuple,
    Optional,
    cast,
)

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.elements import BindParameter
from sqlalchemy.sql.functions import Function as SQLFunction
from sqlalchemy.sql.functions import GenericFunction, ReturnTypeFromArgs, coalesce
from sqlalchemy.types import Float, TypeEngine
from sqlalchemy.types import to_instance as type_instance

import ormar  # noqa: I100, I202
from ormar.exceptions import QueryDefinitionError

if TYPE_CHECKING:  # pragma: no cover
    from ormar.queryset.actions.query_action import QueryAction
    from ormar.queryset.clause import FilterGroup


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


class ExpressionFilter(NamedTuple):
    """
    Comparison of an expression with a value, resolved by ``FilterGroup`` into
    an ``ExpressionFilterAction``.

    :ivar lhs: the filtered expression
    :ivar operator: filter operator (``exact``, ``icontains``, ...)
    :ivar value: compared value, a literal or an expression
    """

    lhs: Expression
    operator: str
    value: Any


class Comparable:
    """
    Comparisons and lookups of ``F``, arithmetic and functions, each returning
    a ``FilterGroup`` taken by ``filter()``, ``exclude()``, ``get()`` and
    composed with ``ormar.and_()``, ``ormar.or_()`` and ``~``::

        Func("lower", F("name")) == "abc"
        Greatest(F("home"), F("away")) >= 3
        Func("lower", F("town")).icontains("war")
        Coalesce(F("nick"), F("name")).in_(["ann", "bob"])

    Values are literals or expressions; ``like`` lookups (``iexact``,
    ``contains``, ``startswith``, ...) take strings, escaped and wrapped in
    ``%`` like in ``filter(name__icontains=...)``, or expressions, wrapped in
    ``%`` in SQL with the wildcards of their string literals escaped
    (``in_()`` takes a list of literals only)::

        Func("unaccent", F("town")).icontains(Func("unaccent", "krakow"))
    """

    __hash__ = object.__hash__

    def _compare(self, operator: str, value: Any) -> "FilterGroup":
        """
        :param operator: filter operator
        :type operator: str
        :param value: compared value
        :type value: Any
        :return: group holding the comparison
        :rtype: FilterGroup
        """
        return ormar.queryset.clause.FilterGroup(
            _expression_filters=[
                ExpressionFilter(
                    lhs=cast(Expression, self), operator=operator, value=value
                )
            ]
        )

    def __eq__(self, other: Any) -> "FilterGroup":  # type: ignore[override]
        """SQL ``expression = other`` (``IS NULL`` for None)"""
        return self._compare(operator="exact", value=other)

    def __ne__(self, other: Any) -> "FilterGroup":  # type: ignore[override]
        """SQL ``expression != other`` (``IS NOT NULL`` for None)"""
        return self._compare(operator="ne", value=other)

    def __lt__(self, other: Any) -> "FilterGroup":
        """SQL ``expression < other``"""
        return self._compare(operator="lt", value=other)

    def __le__(self, other: Any) -> "FilterGroup":
        """SQL ``expression <= other``"""
        return self._compare(operator="lte", value=other)

    def __gt__(self, other: Any) -> "FilterGroup":
        """SQL ``expression > other``"""
        return self._compare(operator="gt", value=other)

    def __ge__(self, other: Any) -> "FilterGroup":
        """SQL ``expression >= other``"""
        return self._compare(operator="gte", value=other)

    def in_(self, other: Any) -> "FilterGroup":
        """SQL ``expression IN (<values>)``"""
        return self._compare(operator="in", value=other)

    def iexact(self, other: Any) -> "FilterGroup":
        """SQL ``expression = other`` case-insensitive"""
        return self._compare(operator="iexact", value=other)

    def contains(self, other: Any) -> "FilterGroup":
        """SQL ``expression LIKE '%other%'``"""
        return self._compare(operator="contains", value=other)

    def icontains(self, other: Any) -> "FilterGroup":
        """SQL ``expression LIKE '%other%'`` case-insensitive"""
        return self._compare(operator="icontains", value=other)

    def startswith(self, other: Any) -> "FilterGroup":
        """SQL ``expression LIKE 'other%'``"""
        return self._compare(operator="startswith", value=other)

    def istartswith(self, other: Any) -> "FilterGroup":
        """SQL ``expression LIKE 'other%'`` case-insensitive"""
        return self._compare(operator="istartswith", value=other)

    def endswith(self, other: Any) -> "FilterGroup":
        """SQL ``expression LIKE '%other'``"""
        return self._compare(operator="endswith", value=other)

    def iendswith(self, other: Any) -> "FilterGroup":
        """SQL ``expression LIKE '%other'`` case-insensitive"""
        return self._compare(operator="iendswith", value=other)

    def isnull(self, other: bool) -> "FilterGroup":
        """SQL ``expression IS NULL`` (``IS NOT NULL`` for False)"""
        return self._compare(operator="isnull", value=other)


class F(Comparable, Expression):
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


class Inserted(Expression):
    """
    Value a conflicting insert tried to write into a field, only in
    ``update_values`` of ``create()`` / ``bulk_create()`` with
    ``on_conflict="update"``: ``excluded.<column>`` on PostgreSQL and SQLite,
    ``VALUES(<column>)`` on MySQL. ``F()`` there is the existing row's column.
    """

    def __init__(self, path: str) -> None:
        """
        :param path: name of a non-relation field of the inserted model
        :type path: str
        """
        self._path = path


class Combined(Comparable, Expression):
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


class Function(Comparable, Expression):
    """
    SQL function of expressions and literals. ``Greatest``, ``Least`` and
    ``Coalesce`` take two or more arguments, at least one of them an
    expression; ``Func`` takes any number.
    """

    _sql_name = ""
    _type: Optional[TypeEngine] = None

    def __init__(self, *arguments: Any) -> None:
        """
        :param arguments: expressions and literals
        :type arguments: Any
        :raises QueryDefinitionError: if the arguments break the arity rules of
            the function or a literal is None or a bool
        """
        self._check_arity(arguments=arguments)
        name = type(self).__name__
        if any(
            argument is None or isinstance(argument, bool) for argument in arguments
        ):
            raise QueryDefinitionError(f"{name}() takes no None or bool arguments")
        self._arguments = tuple(
            argument._node() if isinstance(argument, Expression) else argument
            for argument in arguments
        )

    def _check_arity(self, arguments: tuple[Any, ...]) -> None:
        """
        Require two or more arguments, at least one of them an expression.

        :param arguments: expressions and literals
        :type arguments: tuple[Any, ...]
        :raises QueryDefinitionError: if there are less than two arguments or
            none of them is an expression
        """
        name = type(self).__name__
        if len(arguments) < 2:
            raise QueryDefinitionError(f"{name}() takes at least two arguments")
        if not any(isinstance(argument, Expression) for argument in arguments):
            raise QueryDefinitionError(
                f"{name}() needs a field or an expression among its arguments"
            )


FUNCTION_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*")


class Func(Function):
    """
    Any SQL function by its name, of any number of arguments, expressions or
    literals: ``Func("lower", F("name"))``, ``Func("replace", F("code"), "-",
    "")``, ``Func("now")``. A dotted name calls a function in a schema
    (``Func("extensions.unaccent", F("town"))``).

    Literal arguments are typed by their own value, or, when no argument is an
    expression, after the column the function is compared with or written
    to. The function returns ``type_`` if given, else the type of the first
    expression argument, else the type of that column.
    """

    def __init__(self, name: str, *arguments: Any, type_: Optional[Any] = None) -> None:
        """
        :param name: SQL name of the function, optionally schema qualified
        :type name: str
        :param arguments: expressions and literals
        :type arguments: Any
        :param type_: SQLAlchemy type (class or instance) the function returns
        :type type_: Optional[Any]
        :raises QueryDefinitionError: if the name is not a plain or dotted SQL
            identifier or a literal is None or a bool
        """
        if not isinstance(name, str) or not FUNCTION_NAME.fullmatch(name):
            raise QueryDefinitionError(
                f"Func() takes an SQL function name made of letters, digits and "
                f"underscores, optionally schema qualified with dots, got {name!r}"
            )
        self._sql_name = name
        self._type = type_instance(type_) if type_ is not None else None
        super().__init__(*arguments)

    def _check_arity(self, arguments: tuple[Any, ...]) -> None:
        """
        Accept any number of arguments, literal only ones included.

        :param arguments: expressions and literals
        :type arguments: tuple[Any, ...]
        """


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
    Yield the fields (``F``, ``Inserted``) and literals of an expression, left
    to right; ``Random()`` is neither.

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


def replace_literals(node: Any, replacement: Callable[[Any], Any]) -> Any:
    """
    Return a copy of an expression with each literal replaced, fields and
    ``Random()`` kept::

        replace_literals(node=F("a") + 1, replacement=str)  # -> F("a") + "1"

    :param node: expression node
    :type node: Any
    :param replacement: maps a literal to the one written instead
    :type replacement: Callable[[Any], Any]
    :return: expression node with the literals replaced
    :rtype: Any
    """
    if isinstance(node, Combined):
        return Combined(
            left=replace_literals(node=node._left, replacement=replacement),
            op=node._op,
            right=replace_literals(node=node._right, replacement=replacement),
        )
    if isinstance(node, Function):
        function = copy.copy(node)
        function._arguments = tuple(
            replace_literals(node=argument, replacement=replacement)
            for argument in node._arguments
        )
        return function
    if isinstance(node, Expression):
        return node
    return replacement(node)


def expression_key(node: Any) -> Any:
    """
    Return the shape of an expression for template cache keys: operators,
    fields and the types of the literals, which decide how they are bound::

        F("a") + 1  # -> (operator.add, ("f", "a"), int)
        Greatest(F("a"), 0)  # -> ("greatest", (("f", "a"), int))
        Func("lower", F("a"))  # -> ("lower", (("f", "a"),))
        Func("length", F("a"), type_=Integer)
        # -> ("length", (("f", "a"),), (Integer, "Integer()"))
        Random()  # -> ("random",)
        Inserted("a")  # -> ("inserted", "a")

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
        function_key = (
            node._sql_name,
            tuple(expression_key(node=argument) for argument in node._arguments),
        )
        if node._type is None:
            return function_key
        return (*function_key, (type(node._type), repr(node._type)))
    if isinstance(node, Random):
        return ("random",)
    if isinstance(node, Inserted):
        return ("inserted", node._path)
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
    return slot_literals(literals=expression_literals(node=value), slot=slot)


def expression_literals(node: Any) -> list[Any]:
    """
    :param node: expression node
    :type node: Any
    :return: literals of the expression in the order ``expression_clause``
        names them
    :rtype: list[Any]
    """
    return [
        leaf
        for leaf in expression_leaves(node=node)
        if not isinstance(leaf, (F, Inserted))
    ]


def slot_literals(literals: list[Any], slot: str) -> dict[str, Any]:
    """
    :param literals: bound literals in build order
    :type literals: list[Any]
    :param slot: name of the slot
    :type slot: str
    :return: literals named ``{slot}_{k}``
    :rtype: dict[str, Any]
    """
    return {f"{slot}_{index}": literal for index, literal in enumerate(literals)}


def expression_clause(
    node: Any,
    columns: Mapping[str, "QueryAction"],
    slot: Optional[str],
    index: Optional[Iterator[int]] = None,
    inserted: Any = None,
    reference_type: Optional[TypeEngine] = None,
) -> Any:
    """
    Build the SQL of an expression, reading each field from the aliased column
    of its action in ``columns``. With a ``slot`` the literals become bind
    parameters named ``{slot}_{k}`` and typed from the left operand and the
    literal (``release_values`` drops the literal afterwards), without one
    they stay plain values.

    ``Inserted`` fields are read from ``inserted``, the row a conflicting
    insert tried to write (only upserts resolve them, see
    ``expression_columns``).

    ``reference_type`` is the type of the column the expression is compared
    with or written to; it types the literals of functions without an
    expression argument (``Func("upper", "abc")``).

    :param node: expression node
    :type node: Any
    :param columns: resolved field actions by field path
        (see ``expression_columns``)
    :type columns: Mapping[str, QueryAction]
    :param slot: slot name of a statement template, if built for one
    :type slot: Optional[str]
    :param index: counter of the literals already named
    :type index: Optional[Iterator[int]]
    :param inserted: columns of the inserted row (``excluded`` or MySQL
        ``inserted``) of an upsert
    :type inserted: Any
    :param reference_type: type of the compared or written column
    :type reference_type: Optional[TypeEngine]
    :return: SQL expression
    :rtype: Any
    """
    if isinstance(node, F):
        return columns[node._path].aliased_column
    if isinstance(node, Inserted):
        return inserted[columns[node._path].column.name]
    if isinstance(node, Random):
        return RandomFloat()
    index = index or itertools.count()
    if isinstance(node, Function):
        return function_clause(
            node=node,
            columns=columns,
            slot=slot,
            index=index,
            inserted=inserted,
            reference_type=reference_type,
        )
    left = expression_clause(
        node=node._left,
        columns=columns,
        slot=slot,
        index=index,
        inserted=inserted,
        reference_type=reference_type,
    )
    right = node._right
    if isinstance(right, Expression):
        right = expression_clause(
            node=right,
            columns=columns,
            slot=slot,
            index=index,
            inserted=inserted,
            reference_type=reference_type,
        )
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
    inserted: Any = None,
    reference_type: Optional[TypeEngine] = None,
) -> Any:
    """
    Build the SQL of a function. Literal arguments are bound like the literals
    of ``expression_clause``: named ``{slot}_{k}`` in order and typed from the
    first expression argument and the literal, so ``Greatest(F("count"), 2.5)``
    binds a float; without an expression argument from ``reference_type``.
    ``Func`` binds literals next to an expression argument with the type of
    their own value, as its arguments need not share a type
    (``Func("replace", F("uid"), "-", "")`` on a UUID column).

    Functions of ``SQL_FUNCTIONS`` keep their dialect specific SQL, any other
    name is called as is. Either returns the ``type_`` of a ``Func`` if set,
    else the generic one returns the type of the first expression argument or
    ``reference_type``.

    :param node: function node
    :type node: Function
    :param columns: resolved field actions by field path
    :type columns: Mapping[str, QueryAction]
    :param slot: slot name of a statement template, if built for one
    :type slot: Optional[str]
    :param index: counter of the literals already named
    :type index: Iterator[int]
    :param inserted: columns of the inserted row of an upsert
    :type inserted: Any
    :param reference_type: type of the compared or written column
    :type reference_type: Optional[TypeEngine]
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
                node=argument,
                columns=columns,
                slot=slot,
                index=index,
                inserted=inserted,
                reference_type=reference_type,
            )
        else:
            literal_keys[position] = f"{slot}_{next(index)}" if slot else None
    argument_type = next(
        (clause.type for clause in expression_clauses.values()), reference_type
    )
    literal_type = (
        None if isinstance(node, Func) and expression_clauses else argument_type
    )
    arguments = [
        expression_clauses[position]
        if position in expression_clauses
        else BindParameter(
            key=literal_keys[position],
            value=argument,
            unique=literal_keys[position] is None,
            _compared_to_operator=sql_operators.eq,
            _compared_to_type=literal_type,
        )
        for position, argument in enumerate(node._arguments)
    ]
    dialect_function = SQL_FUNCTIONS.get(node._sql_name.lower())
    if dialect_function is not None:
        if node._type is None:
            return dialect_function(*arguments)
        return dialect_function(*arguments, type_=node._type)
    *packagenames, name = node._sql_name.split(".")
    return SQLFunction(
        name,
        *arguments,
        type_=node._type if node._type is not None else argument_type,
        packagenames=tuple(packagenames),
    )
