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

``Case`` picks the value of the first ``When`` whose condition, written
like the arguments of ``filter()``, holds::

    order = [7, 3, 5]
    Item.objects.filter(id__in=order).update(
        rank=Case(*(When(id=pk, then=rank) for rank, pk in enumerate(order, 1)))
    )

``F("data")["key"]`` reads a value at a path in a JSON field, cast to the
type it is compared as or written into::

    Book.objects.filter(F("meta")["tags"][0].as_string() == "sea")
    Book.objects.filter(Book.meta["pages"].as_integer() > 100)

``Model.field`` accessors are expressions themselves, ``F("path")`` names a
field by its path. ``Expression`` defines only dunders and underscored names,
so it never shadows a field reached through a ``FieldAccessor``, which has
lookup methods of its own instead of ``Comparable``.
"""

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
    Sequence,
    Union,
    cast,
)

import sqlalchemy
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.elements import BinaryExpression, BindParameter, ColumnElement
from sqlalchemy.sql.functions import Function as SQLFunction
from sqlalchemy.sql.functions import GenericFunction, ReturnTypeFromArgs, coalesce
from sqlalchemy.sql.visitors import InternalTraversal
from sqlalchemy.types import JSON, Float, NullType, TypeEngine
from sqlalchemy.types import to_instance as type_instance

import ormar  # noqa: I100, I202
from ormar.exceptions import QueryDefinitionError

if TYPE_CHECKING:  # pragma: no cover
    from ormar.queryset.actions.filter_action import FilterAction
    from ormar.queryset.actions.query_action import QueryAction
    from ormar.queryset.clause import FilterGroup


class Expression:
    """
    Base of column expressions, combined with ``+``, ``-`` and ``*``. The
    literal operand has to be on the right side (``F("a") + 1``).
    """

    # ``__getitem__`` of JSON paths would make expressions iterable through
    # the sequence protocol, endlessly: ``list(F("a"))`` and ``"x" in F("a")``
    # raise TypeError instead
    __iter__ = None

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
    ``%`` in SQL with the wildcards of their value escaped, so it matches
    literally (``in_()`` takes a list of literals only)::

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

    def __getitem__(self, key: Union[str, int]) -> "JsonPath":
        """
        :param key: object key or array index in a JSON field
        :type key: Union[str, int]
        :return: the value at the key, to cast with ``as_string()``,
            ``as_integer()``, ``as_float()`` or ``as_boolean()``
        :rtype: JsonPath
        """
        return JsonPath(field=self, keys=(check_json_key(key=key),))


# keys that the JSON path syntax of every dialect reads literally: SQLite and
# MySQL bind them inside ``$."key"``, PostgreSQL nested paths as ``{a, b}``,
# and MySQL rejects control characters in paths
JSON_KEY_FORBIDDEN = re.compile(r'["\\{},\x00-\x1f]')


# SQLAlchemy casts of JSON index operations by ``JsonPath`` cast name
JSON_CASTS: dict[str, Callable[[Any], Any]] = {
    "string": operator.methodcaller("as_string"),
    "integer": operator.methodcaller("as_integer"),
    "float": operator.methodcaller("as_float"),
    "boolean": operator.methodcaller("as_boolean"),
}


def check_json_key(key: Any) -> Union[str, int]:
    """
    :param key: object key or array index of a JSON path
    :type key: Any
    :raises QueryDefinitionError: if the key is not a string or a
        non-negative int, or a string the JSON path syntax of some dialect
        does not read literally
    :return: the key
    :rtype: Union[str, int]
    """
    if isinstance(key, int) and not isinstance(key, bool) and key >= 0:
        return key
    if (
        isinstance(key, str)
        and key
        and key == key.strip()
        and key.lower() != "null"
        and not JSON_KEY_FORBIDDEN.search(key)
    ):
        return key
    raise QueryDefinitionError(
        f"JSON path keys are non-negative array indexes or object keys without "
        f'", \\, {{, }}, commas, control characters or surrounding whitespace, '
        f"and other than 'null', got {key!r}"
    )


class JsonPath(Comparable, Expression):
    """
    Value at a path of object keys and array indexes in an ``ormar.JSON``
    field, ``F("data")["a"][0]`` or ``Model.data["a"][0]``, compared or
    written after a cast: ``as_string()``, ``as_integer()``, ``as_float()``
    or ``as_boolean()``. A missing key or a JSON ``null`` reads as ``NULL``.
    """

    def __init__(
        self,
        field: F,
        keys: tuple[Union[str, int], ...],
        cast_name: Optional[str] = None,
    ) -> None:
        """
        :param field: the JSON field
        :type field: F
        :param keys: object keys and array indexes from the field's root
        :type keys: tuple[Union[str, int], ...]
        :param cast_name: name of the cast in ``JSON_CASTS``, None before one
        :type cast_name: Optional[str]
        """
        self._field = field
        self._path = field._path
        self._keys = keys
        self._cast_name = cast_name

    def _index(self) -> Union[str, int, tuple[Union[str, int], ...]]:
        """
        :return: the value bound as the path: a single key, or the keys of a
            longer path
        :rtype: Union[str, int, tuple[Union[str, int], ...]]
        """
        return self._keys[0] if len(self._keys) == 1 else self._keys

    def __getitem__(self, key: Union[str, int]) -> "JsonPath":
        """
        :param key: object key or array index below the current path
        :type key: Union[str, int]
        :raises QueryDefinitionError: if the path is already cast
        :return: the value at the longer path
        :rtype: JsonPath
        """
        if self._cast_name is not None:
            raise QueryDefinitionError("Index a JSON path before casting it")
        return JsonPath(field=self._field, keys=(*self._keys, check_json_key(key=key)))

    def _cast(self, cast_name: str) -> "JsonPath":
        """
        :param cast_name: name of the cast in ``JSON_CASTS``
        :type cast_name: str
        :raises QueryDefinitionError: if the path is already cast
        :return: the path read as the cast type
        :rtype: JsonPath
        """
        if self._cast_name is not None:
            raise QueryDefinitionError("A JSON path takes one cast")
        return JsonPath(field=self._field, keys=self._keys, cast_name=cast_name)

    def as_string(self) -> "JsonPath":
        """:return: the value as SQL text"""
        return self._cast(cast_name="string")

    def as_integer(self) -> "JsonPath":
        """:return: the value as an SQL integer"""
        return self._cast(cast_name="integer")

    def as_float(self) -> "JsonPath":
        """:return: the value as an SQL float"""
        return self._cast(cast_name="float")

    def as_boolean(self) -> "JsonPath":
        """:return: the value as an SQL boolean"""
        return self._cast(cast_name="boolean")


class Inserted(Comparable, Expression):
    """
    Value a conflicting insert tried to write into a field, only in
    ``update_values`` of ``create()`` / ``bulk_create()`` with
    ``on_conflict="update"``: ``excluded.<column>`` on PostgreSQL and SQLite,
    ``VALUES(<column>)`` on MySQL. ``F()`` there is the existing row's column.
    Its comparisons are conditions of ``When`` there
    (``When(Inserted("version") > F("version"), then=...)``).
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


class When:
    """
    Branch of a ``Case``: its value ``then`` where the condition holds. The
    condition takes the arguments of ``filter()``: keyword filters with
    operators, ``ormar.and_()`` / ``ormar.or_()`` groups, ``Model.field == v``
    and comparisons of expressions::

        When(id=7, then=1)
        When(ormar.or_(score__gt=10, team__name="red"), then=F("bonus") + 1)
        When(Func("lower", F("name")) == "ann", then=None)

    In filters conditions read the model's own fields and the fields of models
    reached through foreign keys, not reverse or many to many relations; in
    ``update()`` and upserts own fields only, in upserts also ``Inserted``
    fields. A field named ``then`` is filtered as ``then__exact``.
    """

    def __init__(self, *groups: "FilterGroup", then: Any, **kwargs: Any) -> None:
        """
        :param groups: filter groups of the condition
        :type groups: FilterGroup
        :param then: value of the branch, a literal (None, True and False
            are written into the SQL) or an expression
        :type then: Any
        :param kwargs: keyword filters of the condition
        :type kwargs: Any
        :raises QueryDefinitionError: if there is no condition
        """
        if not groups and not kwargs:
            raise QueryDefinitionError("When() needs a condition")
        self.condition = ormar.queryset.clause.FilterGroup(*groups, **kwargs)
        self.then = then._node() if isinstance(then, Expression) else then
        # set on the copies resolved for a query (see ``resolve_expression``)
        self.number = 0
        self.actions: list["FilterAction"] = []

    def slot_prefix(self, slot: str) -> str:
        """
        :param slot: slot name of the expression
        :type slot: str
        :return: prefix of the slot names of the condition's filter values,
            ``{slot}_w{number}_``
        :rtype: str
        """
        return f"{slot}_w{self.number}_"


class Case(Comparable, Expression):
    """
    SQL ``CASE``: the ``then`` of the first ``When`` whose condition holds,
    else ``default`` (``NULL`` when not given)::

        Case(When(score__gte=10, then="high"), When(score__gte=5, then="mid"))
        Case(When(team__name="red", then=F("score") * 2), default=F("score"))

    Literal values are typed after the column the ``Case`` is written to or
    compared with, else after its first expression value. Every number of
    branches builds its own statement, kept in the model's statement cache.
    """

    def __init__(self, *whens: When, default: Any = None) -> None:
        """
        :param whens: branches in the order they are checked
        :type whens: When
        :param default: value where no condition holds, a literal or an
            expression
        :type default: Any
        :raises QueryDefinitionError: if there is no branch or an argument is
            not a ``When``
        """
        if not whens or not all(isinstance(when, When) for when in whens):
            raise QueryDefinitionError(
                "Case() takes one or more When() branches as positional arguments"
            )
        self._whens = list(whens)
        self._default = default._node() if isinstance(default, Expression) else default

    def _values(self) -> list[Any]:
        """
        :return: the values of the branches, then the default
        :rtype: list[Any]
        """
        return [*(when.then for when in self._whens), self._default]


def is_null_case(node: Any) -> bool:
    """
    :param node: expression node
    :type node: Any
    :return: whether the node is a ``Case`` whose values are all None
    :rtype: bool
    """
    return isinstance(node, Case) and all(value is None for value in node._values())


def fields_read(node: Any) -> list[str]:
    """
    Return the model's own fields an expression reads by ``F``, the fields its
    ``When`` conditions filter and read included, for the check that an
    update reads no column it sets. ``Inserted`` fields and fields of related
    models are not columns of the updated row.

    :param node: resolved expression node or a value
    :type node: Any
    :return: field names
    :rtype: list[str]
    """
    names = [
        leaf._path
        for leaf in expression_leaves(node=node)
        if isinstance(leaf, (F, JsonPath)) and "__" not in leaf._path
    ]
    for action in expression_conditions(node=node):
        names += action.fields_read()
    return names


def is_sql_constant(value: Any) -> bool:
    """
    :param value: value of a ``Case`` branch
    :type value: Any
    :return: whether the value is None or a bool, written into the SQL as
        ``NULL``, ``true`` or ``false`` instead of bound
    :rtype: bool
    """
    return value is None or isinstance(value, bool)


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


class DialectClause(ColumnElement[Any]):
    """
    Clause rendered as ``sqlite`` on SQLite, ``mysql`` on MySQL and
    ``default`` elsewhere, all of the type of ``default``.
    """

    inherit_cache = True
    _traverse_internals = [
        ("default", InternalTraversal.dp_clauseelement),
        ("sqlite", InternalTraversal.dp_clauseelement),
        ("mysql", InternalTraversal.dp_clauseelement),
    ]

    def __init__(
        self, default: Any, sqlite: Optional[Any] = None, mysql: Optional[Any] = None
    ) -> None:
        """
        :param default: clause of the other dialects
        :type default: Any
        :param sqlite: clause on SQLite, ``default`` if None
        :type sqlite: Optional[Any]
        :param mysql: clause on MySQL, ``default`` if None
        :type mysql: Optional[Any]
        """
        self.default = default
        self.sqlite = sqlite if sqlite is not None else default
        self.mysql = mysql if mysql is not None else default
        self.type = default.type


@compiles(DialectClause)
def compile_dialect_clause(
    element: DialectClause, compiler: SQLCompiler, **kwargs: Any
) -> str:
    """Other dialects render the default clause."""
    return compiler.process(obj=element.default, **kwargs)


@compiles(DialectClause, "sqlite")
def compile_dialect_clause_on_sqlite(
    element: DialectClause, compiler: SQLCompiler, **kwargs: Any
) -> str:
    """SQLite renders its own clause."""
    return compiler.process(obj=element.sqlite, **kwargs)


@compiles(DialectClause, "mysql")
def compile_dialect_clause_on_mysql(
    element: DialectClause, compiler: SQLCompiler, **kwargs: Any
) -> str:
    """MySQL renders its own clause."""
    return compiler.process(obj=element.mysql, **kwargs)


# SQLAlchemy 2.1 casts JSON index operations on SQLite itself, 2.0 renders a
# bare ``JSON_EXTRACT``, which returns the stored JSON type
SQLITE_CASTS_JSON = tuple(
    int(part) for part in sqlalchemy.__version__.split(".")[:2]
) >= (2, 1)

# the JSON index operation of a path by the number of its keys
JSON_INDEX_OPERATORS = {
    True: sql_operators.json_getitem_op,
    False: sql_operators.json_path_getitem_op,
}


# ``Case`` values written into the SQL instead of bound
SQL_CONSTANTS: dict[Optional[bool], Callable[[], Any]] = {
    None: sqlalchemy.null,
    True: sqlalchemy.true,
    False: sqlalchemy.false,
}


SQL_FUNCTIONS: dict[str, Callable[..., Any]] = {
    "greatest": GreatestOf,
    "least": LeastOf,
    "coalesce": coalesce,
}


def expression_leaves(node: Any) -> Iterator[Any]:
    """
    Yield the fields (``F``, ``Inserted``, ``JsonPath``) and literals of an
    expression, left to right; ``Random()`` is neither.

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
    elif isinstance(node, Case):
        for value in node._values():
            if not is_sql_constant(value=value):
                yield from expression_leaves(node=value)
    elif not isinstance(node, Random):
        yield node


def expression_whens(node: Any) -> Iterator[When]:
    """
    Yield the ``When`` branches of the ``Case`` expressions in an expression,
    nested ones included.

    :param node: expression node or a value
    :type node: Any
    :return: branches
    :rtype: Iterator[When]
    """
    if isinstance(node, Combined):
        yield from expression_whens(node=node._left)
        yield from expression_whens(node=node._right)
    elif isinstance(node, Function):
        for argument in node._arguments:
            yield from expression_whens(node=argument)
    elif isinstance(node, Case):
        for when in node._whens:
            yield when
            yield from expression_whens(node=when.then)
        yield from expression_whens(node=node._default)


def expression_conditions(node: Any) -> Iterator["FilterAction"]:
    """
    Yield the filter actions of the ``When`` conditions in a resolved
    expression (see ``resolve_expression``), whose relation paths are joined
    like the fields the expression reads.

    :param node: resolved expression node or a value
    :type node: Any
    :return: filter actions of the conditions
    :rtype: Iterator[FilterAction]
    """
    for when in expression_whens(node=node):
        yield from when.actions


def condition_params(node: Any, slot: str) -> dict[str, Any]:
    """
    Return the values bound by the ``When`` conditions of a resolved
    expression, named like ``case_clause`` names them::

        Case(When(id=7, then=1))  # slot ormar_v0 -> {"ormar_v0_w0_0": 7}

    :param node: resolved expression node or a value
    :type node: Any
    :param slot: slot name of the expression
    :type slot: str
    :return: values by bind parameter name
    :rtype: dict[str, Any]
    """
    params: dict[str, Any] = {}
    for when in expression_whens(node=node):
        params.update(
            ormar.queryset.templates.action_params(
                actions=when.actions, prefix=when.slot_prefix(slot=slot)
            )
        )
    return params


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
        F("a")["b"][0].as_string()  # -> ("json", "a", (str, int), "string")
        Case(When(id=7, then=1), default=None)
        # -> ("case", ((<condition key>, int),), ("constant", None))

    The conditions of a ``Case`` are keyed like filter clauses (see
    ``clauses_cache_key``), so the expression has to be resolved.

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
    if isinstance(node, Case):
        return (
            "case",
            tuple(
                (
                    ormar.queryset.templates.clauses_cache_key(
                        clauses=[when.condition]
                    ),
                    case_value_key(value=when.then),
                )
                for when in node._whens
            ),
            case_value_key(value=node._default),
        )
    if isinstance(node, Random):
        return ("random",)
    if isinstance(node, Inserted):
        return ("inserted", node._path)
    if isinstance(node, JsonPath):
        key_types = tuple(type(key) for key in node._keys)
        return ("json", node._path, key_types, node._cast_name)
    return ("f", node._path) if isinstance(node, F) else type(node)


def case_value_key(value: Any) -> Any:
    """
    :param value: value of a ``Case`` branch
    :type value: Any
    :return: the value itself if written into the SQL (None, True, False),
        else its ``expression_key``
    :rtype: Any
    """
    if is_sql_constant(value=value):
        return ("constant", value)
    return expression_key(node=value)


def expression_params(value: Any, slot: str) -> dict[str, Any]:
    """
    Return the values bound for a slot: the value itself, or for an expression
    its literals named ``{slot}_{k}`` (as ``expression_clause`` names them)
    and the values of its ``When`` conditions (see ``condition_params``).

    :param value: bound value or expression
    :type value: Any
    :param slot: name of the slot
    :type slot: str
    :return: values by bind parameter name
    :rtype: dict[str, Any]
    """
    if not isinstance(value, Expression):
        return {slot: value}
    return {
        **slot_literals(literals=expression_literals(node=value), slot=slot),
        **condition_params(node=value, slot=slot),
    }


def expression_literals(node: Any) -> list[Any]:
    """
    :param node: expression node
    :type node: Any
    :return: literals of the expression in the order ``expression_clause``
        names them
    :rtype: list[Any]
    """
    return [
        leaf._index() if isinstance(leaf, JsonPath) else leaf
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
    if isinstance(node, JsonPath):
        return json_path_clause(
            node=node,
            column=columns[node._path].aliased_column,
            key=f"{slot}_{next(index)}" if slot is not None else None,
        )
    if isinstance(node, Case):
        return case_clause(
            node=node,
            columns=columns,
            slot=slot,
            index=index,
            inserted=inserted,
            reference_type=reference_type,
        )
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
    expression_clauses, literal_keys = operand_clauses(
        operands=node._arguments,
        columns=columns,
        slot=slot,
        index=index,
        inserted=inserted,
        reference_type=reference_type,
    )
    argument_type = next(
        (clause.type for clause in expression_clauses.values()), reference_type
    )
    literal_type = (
        None if isinstance(node, Func) and expression_clauses else argument_type
    )
    arguments = [
        expression_clauses[position]
        if position in expression_clauses
        else literal_bind(
            key=literal_keys[position], value=argument, compared_type=literal_type
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


def case_clause(
    node: Case,
    columns: Mapping[str, "QueryAction"],
    slot: Optional[str],
    index: Iterator[int],
    inserted: Any = None,
    reference_type: Optional[TypeEngine] = None,
) -> Any:
    """
    Build the SQL ``CASE`` of a resolved ``Case`` (see ``resolve_expression``).

    The conditions are built like filters, their values named by a
    ``SlotBinder`` with the prefix ``{slot}_w{number}_`` of each branch.
    Literal values are named ``{slot}_{k}`` like the literals of
    ``expression_clause``, in the order of the branches and the default, and
    typed after ``reference_type``, else after the first expression value.
    None, True and False are written into the SQL; when all values are None
    the ``NULL`` is cast to ``reference_type``, as PostgreSQL reads an untyped
    ``CASE`` of ``NULL`` as text.

    :param node: resolved case node
    :type node: Case
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
    :return: SQL case
    :rtype: Any
    """
    conditions = [
        when.condition.get_text_clause(
            binder=ormar.queryset.templates.SlotBinder(
                actions=when.actions, prefix=when.slot_prefix(slot=slot)
            )
            if slot is not None
            else None,
            inserted=inserted,
        )
        for when in node._whens
    ]
    values = node._values()
    expression_clauses, literal_keys = operand_clauses(
        operands=values,
        columns=columns,
        slot=slot,
        index=index,
        inserted=inserted,
        reference_type=reference_type,
    )
    literal_type = (
        reference_type
        if reference_type is not None
        else next((clause.type for clause in expression_clauses.values()), None)
    )
    null_type = (
        literal_type
        if is_null_case(node=node) and not isinstance(literal_type, NullType)
        else None
    )
    value_clauses = [
        expression_clauses[position]
        if position in expression_clauses
        else sqlalchemy.cast(expression=sqlalchemy.null(), type_=null_type)
        if null_type is not None
        else SQL_CONSTANTS[value]()
        if is_sql_constant(value=value)
        else literal_bind(
            key=literal_keys[position], value=value, compared_type=literal_type
        )
        for position, value in enumerate(values)
    ]
    *then_clauses, default_clause = value_clauses
    return sqlalchemy.case(*zip(conditions, then_clauses), else_=default_clause)


def operand_clauses(
    operands: Sequence[Any],
    columns: Mapping[str, "QueryAction"],
    slot: Optional[str],
    index: Iterator[int],
    inserted: Any = None,
    reference_type: Optional[TypeEngine] = None,
) -> tuple[dict[int, Any], dict[int, Optional[str]]]:
    """
    Build the expression operands of a function or ``Case`` and name its bound
    literals ``{slot}_{k}``, both in operand order. The literals are bound
    afterwards (see ``literal_bind``), typed after the first expression
    operand, which may come later. None and bools are not bound.

    :param operands: arguments or values, expressions and literals
    :type operands: Sequence[Any]
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
    :return: SQL of the expression operands and names of the literal ones,
        by position (None names without a slot)
    :rtype: tuple[dict[int, Any], dict[int, Optional[str]]]
    """
    expression_clauses: dict[int, Any] = {}
    literal_keys: dict[int, Optional[str]] = {}
    for position, operand in enumerate(operands):
        if isinstance(operand, Expression):
            expression_clauses[position] = expression_clause(
                node=operand,
                columns=columns,
                slot=slot,
                index=index,
                inserted=inserted,
                reference_type=reference_type,
            )
        elif not is_sql_constant(value=operand):
            literal_keys[position] = f"{slot}_{next(index)}" if slot else None
    return expression_clauses, literal_keys


def literal_bind(key: Optional[str], value: Any, compared_type: Any) -> BindParameter:
    """
    :param key: name of the parameter, None for an anonymous one
    :type key: Optional[str]
    :param value: literal operand
    :type value: Any
    :param compared_type: type the literal is bound with, None for the type
        of its own value
    :type compared_type: Any
    :return: bind parameter of a literal operand
    :rtype: BindParameter
    """
    return BindParameter(
        key=key,
        value=value,
        unique=key is None,
        _compared_to_operator=sql_operators.eq,
        _compared_to_type=compared_type,
    )


def json_path_clause(node: JsonPath, column: Any, key: Optional[str]) -> Any:
    """
    Build the SQL of a JSON path with SQLAlchemy's JSON index operations, a
    single key as ``->>`` on PostgreSQL, a longer path as ``#>>``, and
    ``JSON_EXTRACT`` (with ``JSON_UNQUOTE`` for strings on MySQL) elsewhere,
    each cast to the type of the path's cast, on SQLite also with SQLAlchemy
    2.0. A boolean is ``NULL`` for a missing key on MySQL too, where
    SQLAlchemy's ``as_boolean()`` reads it as false.

    The path is bound as one parameter named ``key`` like the literals of
    ``expression_clause``, so statement templates and the user variables of
    MySQL upserts bind it like any value; anonymous without a key.

    :param node: JSON path with a cast (see ``expression_columns``)
    :type node: JsonPath
    :param column: the JSON column in the aliased table of its join
    :type column: Any
    :param key: name of the bound path, None for an anonymous one
    :type key: Optional[str]
    :return: SQL of the cast value
    :rtype: Any
    """
    single = len(node._keys) == 1
    path_type = (
        (
            JSON.JSONIntIndexType
            if isinstance(node._keys[0], int)
            else JSON.JSONStrIndexType
        )
        if single
        else JSON.JSONPathType
    )
    path = BindParameter(
        key=key, value=node._index(), type_=path_type(), unique=key is None
    )
    element = BinaryExpression(
        left=column,
        right=path,
        operator=JSON_INDEX_OPERATORS[single],
        type_=column.type,
    )
    value = JSON_CASTS[cast(str, node._cast_name)](element)
    return DialectClause(
        default=value,
        sqlite=None
        if SQLITE_CASTS_JSON
        else sqlalchemy.cast(expression=value, type_=value.type),
        mysql=(
            sqlalchemy.case((element.is_not(other=None), value))
            if node._cast_name == "boolean"
            else None
        ),
    )
