import itertools
from typing import TYPE_CHECKING, Any, Optional, cast

import sqlalchemy
from sqlalchemy import TextClause
from sqlalchemy.sql import operators as sql_operators
from sqlalchemy.sql.elements import BindParameter

import ormar  # noqa: I100, I202
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.query_action import QueryAction
from ormar.queryset.actions.select_action import SelectAction, expression_columns
from ormar.queryset.expressions import (
    Combined,
    Expression,
    Func,
    Function,
    expression_clause,
    expression_key,
    expression_literals,
    expression_params,
    slot_literals,
)

if TYPE_CHECKING:  # pragma: nocover
    from ormar import ForeignKeyField, Model
    from ormar.queryset.templates import SlotBinder

FILTER_OPERATORS = {
    "exact": "__eq__",
    "ne": "__ne__",
    "iexact": "ilike",
    "contains": "like",
    "icontains": "ilike",
    "startswith": "like",
    "istartswith": "ilike",
    "endswith": "like",
    "iendswith": "ilike",
    "isnull": "is_",
    "in": "in_",
    "gt": "__gt__",
    "gte": "__ge__",
    "lt": "__lt__",
    "lte": "__le__",
}
METHODS_TO_OPERATORS = {
    "__eq__": "exact",
    "__ne__": "ne",
    "__mod__": "contains",
    "__gt__": "gt",
    "__ge__": "gte",
    "__lt__": "lt",
    "__le__": "lte",
    "iexact": "iexact",
    "contains": "contains",
    "icontains": "icontains",
    "startswith": "startswith",
    "istartswith": "istartswith",
    "endswith": "endswith",
    "iendswith": "iendswith",
    "isnull": "isnull",
    "in": "in",
}
# SQLAlchemy operators of ormar's filter operator methods, used to type bound
# values exactly like a plain ``column <op> value`` comparison would.
COMPARISON_OPERATORS = {
    "__eq__": sql_operators.eq,
    "__ne__": sql_operators.ne,
    "like": sql_operators.like_op,
    "ilike": sql_operators.ilike_op,
    "in_": sql_operators.in_op,
    "__gt__": sql_operators.gt,
    "__ge__": sql_operators.ge,
    "__lt__": sql_operators.lt,
    "__le__": sql_operators.le,
}
ESCAPE_CHARACTERS = ["%", "_"]
# escape character of plain string values, escaped in Python
VALUE_ESCAPE = "\\"
# escape character of expression values, escaped in SQL; unlike a backslash
# it is no escape character in MySQL string literals
EXPRESSION_ESCAPE = "!"
# operators taking an expression value; ``in`` takes a list and ``isnull`` a
# bool, so neither has an expression form
EXPRESSION_VALUE_OPERATORS = (
    "exact",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "iexact",
    "contains",
    "icontains",
    "startswith",
    "istartswith",
    "endswith",
    "iendswith",
)
LIKE_OPERATORS = (
    "contains",
    "icontains",
    "startswith",
    "istartswith",
    "endswith",
    "iendswith",
)


def has_like_wildcards(value: Any) -> bool:
    """
    :param value: filter value
    :type value: Any
    :return: whether the value is a string holding ``%`` or ``_``
    :rtype: bool
    """
    return isinstance(value, str) and any(
        character in value for character in ESCAPE_CHARACTERS
    )


def escape_like(value: str) -> str:
    """
    Escape the ``LIKE`` wildcards ``%`` and ``_`` with a backslash, matched
    by the ``ESCAPE '\\'`` clause ``comparison_clause`` adds.

    :param value: searched text
    :type value: str
    :return: text matching itself in ``LIKE``
    :rtype: str
    """
    for character in ESCAPE_CHARACTERS:
        value = value.replace(character, f"\\{character}")
    return value


def like_pattern(value: Any, operator: str) -> Any:
    """
    Wrap a searched text in ``%`` for a ``LIKE`` lookup. The text of an SQL
    expression is wrapped in SQL, concatenated with ``||`` (``concat()`` on
    MySQL)::

        like_pattern(value="ab", operator="icontains")  # -> "%ab%"
        like_pattern(value="ab", operator="startswith")  # -> "ab%"
        like_pattern(value=func.lower(name), operator="endswith")
        # -> '%' || lower(name)

    :param value: searched text or SQL text expression, already escaped
    :type value: Any
    :param operator: ``contains``, ``startswith`` or ``endswith`` lookup
    :type operator: str
    :return: ``LIKE`` pattern, a string or an SQL expression
    :rtype: Any
    """
    prefix = "%" if "start" not in operator else ""
    suffix = "%" if "end" not in operator else ""
    if isinstance(value, str):
        return f"{prefix}{value}{suffix}"
    wildcard = sql_string(text="%")
    if prefix:
        value = wildcard.concat(value)
    return value.concat(wildcard) if suffix else value


def sql_string(text: str) -> Any:
    """
    :param text: text without quotes
    :type text: str
    :return: the text as an SQL string literal written into the statement,
        not bound, so it takes no parameter of a statement template
    :rtype: Any
    """
    return sqlalchemy.literal_column(text=f"'{text}'", type_=sqlalchemy.String())


def escape_like_clause(value: Any) -> Any:
    """
    Escape the ``LIKE`` wildcards of an SQL expression's value in SQL, so it
    matches literally, with the ``ESCAPE '!'`` clause ``comparison_clause``
    adds. A value that is not text is cast to text first::

        escape_like_clause(value=func.lower(name))
        # -> replace(replace(replace(lower(name), '!', '!!'), '%', '!%'),
        #            '_', '!_')

    :param value: SQL expression
    :type value: Any
    :return: SQL text expression matching the value in ``LIKE``
    :rtype: Any
    """
    if not isinstance(value.type, sqlalchemy.String):
        value = sqlalchemy.cast(expression=value, type_=sqlalchemy.String())
    for character in (EXPRESSION_ESCAPE, *ESCAPE_CHARACTERS):
        value = sqlalchemy.func.replace(
            value,
            sql_string(text=character),
            sql_string(text=f"{EXPRESSION_ESCAPE}{character}"),
            type_=sqlalchemy.String(),
        )
    return value


def like_value_clause(value: Any, operator: str) -> Any:
    """
    Turn an SQL expression's value into the right side of a filter: for
    ``iexact`` and the ``LIKE`` lookups escaped (see ``escape_like_clause``),
    the lookups also wrapped in ``%`` (see ``like_pattern``); for the other
    operators the value itself.

    :param value: SQL expression
    :type value: Any
    :param operator: filter operator
    :type operator: str
    :return: right side of the condition
    :rtype: Any
    """
    if operator != "iexact" and operator not in LIKE_OPERATORS:
        return value
    value = escape_like_clause(value=value)
    if operator == "iexact":
        return value
    return like_pattern(value=value, operator=operator)


def comparison_bind(
    key: Optional[str], value: Any, operator: str, compared_type: Any
) -> BindParameter:
    """
    Build the bind parameter of a compared value the way SQLAlchemy builds
    the one of ``column <op> value``: typed after ``compared_type`` (its bind
    processing for dates, JSON, enums and so on) and expanding for ``in``,
    rendering one placeholder per item of the list given on each call.

    :param key: name of the parameter, None for an anonymous one
    :type key: Optional[str]
    :param value: compared value
    :type value: Any
    :param operator: filter operator
    :type operator: str
    :param compared_type: type the value is compared to, None for the type of
        the value itself
    :type compared_type: Any
    :return: bind parameter
    :rtype: BindParameter
    """
    return BindParameter(
        key=key,
        value=value,
        unique=key is None,
        expanding=operator == "in",
        _compared_to_operator=COMPARISON_OPERATORS[FILTER_OPERATORS[operator]],
        _compared_to_type=compared_type,
    )


def comparison_clause(
    left: Any, operator: str, value: Any, escape: Optional[str]
) -> Any:
    """
    Compare an SQL expression with a value by a filter operator.

    :param left: filtered column or expression
    :type left: Any
    :param operator: filter operator
    :type operator: str
    :param value: compared value or bind parameter; for ``isnull`` whether the
        expression is null
    :type value: Any
    :param escape: ``ESCAPE`` character of the ``LIKE`` wildcards in the
        value, None when nothing is escaped
    :type escape: Optional[str]
    :return: SQL condition
    :rtype: Any
    """
    if operator == "isnull":
        return left.is_(None) if value else left.isnot(None)
    clause = getattr(left, FILTER_OPERATORS[operator])(value)
    if escape is not None:
        clause.modifiers["escape"] = escape
    return clause


def split_filter_key(key: str) -> tuple[str, list[str]]:
    """
    Split a filter key into its operator and the path of the filtered field.
    A trailing operator name is always the operator, so a field named like an
    operator is filtered with an explicit one::

        split_filter_key(key="album__name__ne")  # -> ("ne", ["album", "name"])
        split_filter_key(key="gt__exact")  # field gt -> ("exact", ["gt"])

    :param key: filter key
    :type key: str
    :raises QueryDefinitionError: if the key is only an operator
    :return: operator (``exact`` when none is given) and the field path parts
    :rtype: tuple[str, list[str]]
    """
    parts = key.split("__")
    if parts[-1] not in FILTER_OPERATORS:
        return "exact", parts
    if len(parts) == 1:
        raise QueryDefinitionError(
            f"Filter key '{key}' is an operator without a field, "
            f"filter a field named '{key}' with '{key}__exact'"
        )
    return parts[-1], parts[:-1]


class FilterAction(QueryAction):
    """
    Represents a single WHERE predicate on a queryset.

    Created for each ``key=value`` (or ``key__op=value``) passed to
    ``filter(...)`` / ``exclude(...)`` and to filtering shortcuts like
    ``get(...)`` / ``first(...)``.
    """

    def __init__(self, filter_str: str, value: Any, model_cls: type["Model"]) -> None:
        super().__init__(query_str=filter_str, model_cls=model_cls)
        self.expression_columns: dict[str, SelectAction] = {}
        if isinstance(value, Expression):
            value = value._node()
            if self.operator not in EXPRESSION_VALUE_OPERATORS:
                raise QueryDefinitionError(
                    f"Expression values take comparison and like filters, not "
                    f"'{self.operator}': in takes a list of values, isnull a bool"
                )
            self.expression_columns = expression_columns(node=value, model=model_cls)
        self.filter_value = value
        self._escape_characters_in_clause()

    def path_actions(self) -> list[QueryAction]:
        """
        :return: the action and the fields its expression reads, each resolving
            one relation path to a joined table
        :rtype: list[QueryAction]
        """
        return [self, *self.expression_columns.values()]

    def update_select_related(self, select_related: list[str]) -> list[str]:
        """
        Add the joins of the filtered field and of the fields its expression
        reads (see ``QueryAction.update_select_related``).

        :param select_related: list of relation join strings
        :type select_related: list[str]
        :return: list of relation joins with implied joins from filter added
        :rtype: list[str]
        """
        select_related = super().update_select_related(select_related=select_related)
        for column in self.expression_columns.values():
            select_related = column.update_select_related(select_related=select_related)
        return select_related

    def _split_value_into_parts(self, query_str: str) -> None:
        self.operator, path = split_filter_key(key=query_str)
        self.field_name = path[-1]
        self.related_parts = path[:-1]

    def _escape_characters_in_clause(self) -> None:
        """
        Escapes the special characters ["%", "_"] if needed.
        Adds `%` for `like` queries.

        ``iexact`` is compiled to ``ilike`` too, so its value is escaped the
        same way, but it is not wrapped in ``%``::

            filter(name__iexact="a_c")  # matches "A_C", not "abc"

        An expression value is escaped and wrapped in ``%`` in SQL when the
        clause is built (see ``like_value_clause``), always with
        ``ESCAPE '!'``.

        :raises QueryDefinitionError: if contains or icontains is used with
        ormar model instance
        """
        self.has_escaped_character = False
        if self.operator != "iexact" and self.operator not in LIKE_OPERATORS:
            return
        if self.operator in LIKE_OPERATORS and isinstance(
            self.filter_value, ormar.Model
        ):
            raise QueryDefinitionError(
                "You cannot use contains and icontains with instance of the Model"
            )
        if isinstance(self.filter_value, Expression):
            self.has_escaped_character = True
            return
        self.has_escaped_character = has_like_wildcards(value=self.filter_value)
        if self.has_escaped_character:
            self.filter_value = escape_like(value=self.filter_value)
        if self.operator in LIKE_OPERATORS:
            self.filter_value = like_pattern(
                value=self.filter_value, operator=self.operator
            )

    def escape_character(self) -> Optional[str]:
        """
        :return: ``ESCAPE`` character of the condition, ``!`` for expression
            values escaped in SQL, a backslash for strings escaped in Python,
            None when nothing is escaped
        :rtype: Optional[str]
        """
        if not self.has_escaped_character:
            return None
        if isinstance(self.filter_value, Expression):
            return EXPRESSION_ESCAPE
        return VALUE_ESCAPE

    def bound_value(self) -> Any:
        """
        Return the value bound to the query: an ormar Model is replaced by its
        value it is referenced by (its primary key or the foreign key's
        ``to_field``), so ``filter(author=author)`` binds ``author.pk``.

        ``filter_value`` itself is left untouched, so the action can be read
        again on every call to collect the value for a cached template.

        :return: value to bind
        :rtype: Any
        """
        if isinstance(self.filter_value, ormar.Model):
            field = self.target_model.ormar_config.model_fields.get(self.field_name)
            if (
                field is not None
                and field.is_relation
                and not field.virtual
                and not field.is_multi
            ):
                relation_field = cast("ForeignKeyField", field)
                return relation_field.target_value_of(related=self.filter_value)
            return self.filter_value.pk
        return self.filter_value

    def binds_slot(self) -> bool:
        """
        Return whether the value is bound as a parameter: SQLAlchemy renders
        ``None`` (``IS NULL``), booleans and ``isnull`` checks into the SQL text.

        Only values that are bound can get a named slot in a statement
        template. The others stay in the SQL text and become part of the
        template's cache key instead (see ``templates.value_cache_key``)::

            filter(name="a")  # True  -> name = :ormar_p0
            filter(name=None)  # False -> name IS NULL

        :return: True when the value is sent as a bound parameter
        :rtype: bool
        """
        value = self.bound_value()
        return (
            self.operator != "isnull"
            and value is not None
            and not isinstance(value, bool)
        )

    def cache_key(self) -> tuple[Any, ...]:
        """
        Return the filtered side of the action for template cache keys (see
        ``templates.action_cache_key``): the field path and the table alias of
        its joined relation.

        :return: hashable key of the filtered side
        :rtype: tuple[Any, ...]
        """
        return (self.query_str, self.table_prefix)

    def slot_params(self, slot: str) -> dict[str, Any]:
        """
        Return the values the action binds in a template, keyed like the
        parameters ``get_text_clause`` names for ``slot``::

            filter(name="a")  # -> {"ormar_p0": "a"}
            filter(score__lt=F("limit") + 1)  # -> {"ormar_p0_0": 1}
            filter(name=None)  # -> {}

        :param slot: slot name of the action
        :type slot: str
        :return: values by bind parameter name
        :rtype: dict[str, Any]
        """
        if not self.binds_slot():
            return {}
        return expression_params(value=self.bound_value(), slot=slot)

    def get_text_clause(self, binder: Optional["SlotBinder"] = None) -> TextClause:
        """
        Escapes characters if it's required.
        Substitutes values of the models if value is a ormar Model with its pk value.
        Compiles the clause.

        :param binder: names the bound value for statement templates
            (``= :ormar_p0`` instead of an anonymous parameter), if given
        :type binder: Optional[SlotBinder]
        :return: complied and escaped clause
        :rtype: sqlalchemy.sql.elements.TextClause
        """
        value = self.bound_value()
        filter_value = value
        if isinstance(value, Expression):
            filter_value = expression_clause(
                node=value,
                columns=self.expression_columns,
                slot=binder.slots[id(self)] if binder is not None else None,
                reference_type=self.aliased_column.type,
            )
            filter_value = like_value_clause(value=filter_value, operator=self.operator)
        elif binder is not None and self.binds_slot():
            filter_value = binder.bind(action=self, value=value)
        return comparison_clause(
            left=self.aliased_column,
            operator=self.operator,
            value=filter_value,
            escape=self.escape_character(),
        )


class ExpressionFilterAction(FilterAction):
    """
    Filter whose filtered side is an expression instead of a field, created
    by the comparisons of ``Comparable``::

        filter(Func("lower", F("album__name")) == "malibu")
        # -> WHERE lower(albums.name) = :ormar_p0_0

    The fields the expression reads are joined like the fields of an
    expression value. In templates the literals of both sides share the
    ``{slot}_{k}`` names, the filtered side first.
    """

    def __init__(
        self, lhs: Expression, operator: str, value: Any, model_cls: type["Model"]
    ) -> None:
        """
        :param lhs: filtered expression
        :type lhs: Expression
        :param operator: filter operator
        :type operator: str
        :param value: compared value, a literal or an expression
        :type value: Any
        :param model_cls: queried model
        :type model_cls: type[Model]
        :raises QueryDefinitionError: if the expression reads a relation or a
            missing field, or an expression value is used with ``in`` or
            ``isnull``
        """
        self.lhs = lhs._node()
        self.lhs_columns = expression_columns(node=self.lhs, model=model_cls)
        super().__init__(filter_str=operator, value=value, model_cls=model_cls)

    def _split_value_into_parts(self, query_str: str) -> None:
        """
        The action filters no field, its query string is the operator only.

        :param query_str: filter operator
        :type query_str: str
        """
        self.operator = query_str

    def _determine_filter_target_table(self) -> None:
        """The action has no own relation path, its fields resolve their own."""

    def path_actions(self) -> list[QueryAction]:
        """
        :return: the fields both sides read, each resolving one relation path
            to a joined table
        :rtype: list[QueryAction]
        """
        return [*self.lhs_columns.values(), *self.expression_columns.values()]

    def update_select_related(self, select_related: list[str]) -> list[str]:
        """
        Add the joins of the fields both sides read.

        :param select_related: list of relation join strings
        :type select_related: list[str]
        :return: list of relation joins with implied joins from filter added
        :rtype: list[str]
        """
        for column in self.path_actions():
            select_related = column.update_select_related(select_related=select_related)
        return select_related

    def cache_key(self) -> tuple[Any, ...]:
        """
        :return: the shape of the filtered expression (see ``expression_key``)
        :rtype: tuple[Any, ...]
        """
        return ("lhs", expression_key(node=self.lhs))

    def value_type(self, lhs_type: Any) -> Any:
        """
        Return the type a literal value is bound with: the type of the
        filtered expression, unless that type comes from a ``Func`` without
        ``type_``, whose result type is only guessed from its first argument
        (``Func("upper", F("uid")) == "AB"`` and
        ``Coalesce(Func("upper", F("uid")), F("name")) == "AB"`` bind a
        string, not a UUID). The type comes from the left operand of
        arithmetic and the first expression argument of ``Greatest``,
        ``Least`` and ``Coalesce``.

        :param lhs_type: SQL type of the filtered expression
        :type lhs_type: Any
        :return: type to compare the value to, None for the value's own type
        :rtype: Any
        """
        node: Any = self.lhs
        while isinstance(node, (Combined, Function)) and not isinstance(node, Func):
            node = (
                node._left
                if isinstance(node, Combined)
                else next(
                    argument
                    for argument in node._arguments
                    if isinstance(argument, Expression)
                )
            )
        if isinstance(node, Func) and node._type is None:
            return None
        return lhs_type

    def slot_params(self, slot: str) -> dict[str, Any]:
        """
        Return the literals of the filtered expression, then the value or the
        literals of an expression value, named in the order
        ``get_text_clause`` builds them.

        :param slot: slot name of the action
        :type slot: str
        :return: values by bind parameter name
        :rtype: dict[str, Any]
        """
        literals = expression_literals(node=self.lhs)
        value = self.bound_value()
        if isinstance(value, Expression):
            literals += expression_literals(node=value)
        elif self.binds_slot():
            literals.append(value)
        return slot_literals(literals=literals, slot=slot)

    def get_text_clause(self, binder: Optional["SlotBinder"] = None) -> TextClause:
        """
        Build the filtered expression and the value with one counter of
        literals, then compare them.

        :param binder: names the bound literals for statement templates, if
            given
        :type binder: Optional[SlotBinder]
        :return: the condition
        :rtype: TextClause
        """
        slot = binder.slots[id(self)] if binder is not None else None
        index = itertools.count()
        lhs_clause = expression_clause(
            node=self.lhs, columns=self.lhs_columns, slot=slot, index=index
        )
        value = self.bound_value()
        filter_value = value
        if isinstance(value, Expression):
            filter_value = expression_clause(
                node=value,
                columns=self.expression_columns,
                slot=slot,
                index=index,
                reference_type=lhs_clause.type,
            )
            filter_value = like_value_clause(value=filter_value, operator=self.operator)
        elif self.binds_slot():
            filter_value = comparison_bind(
                key=f"{slot}_{next(index)}" if slot is not None else None,
                value=value,
                operator=self.operator,
                compared_type=self.value_type(lhs_type=lhs_clause.type),
            )
        return comparison_clause(
            left=lhs_clause,
            operator=self.operator,
            value=filter_value,
            escape=self.escape_character(),
        )


class KeyInFilterAction(FilterAction):
    """
    ``pk__in`` filter on a composite primary key, compiled to one row-value
    ``IN`` over the key columns::

        filter(pk__in=[(1, 7), (2, 8)])
        # -> WHERE (orders.tenant_id, orders.number) IN ((1, 7), (2, 8))

    The values are bound as one expanding parameter, so every list length
    shares one statement. With ``columns`` the action compares other columns
    of the table, e.g. the columns of a foreign key stored in several columns
    (``anchor`` names the field the columns belong to)::

        filter(order__in=[(1, 7)])
        # -> WHERE (lines.ord_tenant, lines.ord_no) IN ((1, 7))
    """

    def __init__(
        self,
        *,
        prefix: str,
        values: list[tuple[Any, ...]],
        model_cls: type["Model"],
        anchor: Optional[str] = None,
        columns: Optional[tuple[str, ...]] = None,
    ) -> None:
        """
        :param prefix: relation path to the filtered model, ``""`` or ending in
            ``__``
        :type prefix: str
        :param values: key values as tuples of parts in key order
        :type values: list[tuple[Any, ...]]
        :param model_cls: queried model
        :type model_cls: type[Model]
        """
        self.columns = columns
        if anchor is None:
            target_key = model_cls.ormar_config.pk
            for part in prefix.split("__")[:-1]:
                target_key = target_key.owner.ormar_config.model_fields[
                    part
                ].to.ormar_config.pk
            anchor = target_key.fields[0]
        super().__init__(
            filter_str=f"{prefix}{anchor}__{self.key_operator()}",
            value=values,
            model_cls=model_cls,
        )
        key_name = "pk" if columns is None else "__".join(columns)
        self.query_str = f"{prefix}{key_name}__{self.key_operator()}"

    @staticmethod
    def key_operator() -> str:
        """
        :return: lookup operator of the action
        :rtype: str
        """
        return "in"

    def key_columns(self) -> list[sqlalchemy.Column]:
        """
        :return: columns of the key, aliased to the table prefix of the filter
        :rtype: list[sqlalchemy.Column]
        """
        names = self.columns or tuple(
            column.name for column in self.target_model.ormar_config.pk.columns()
        )
        return [self.aliased_table.c[name] for name in names]

    def get_text_clause(self, binder: Optional["SlotBinder"] = None) -> TextClause:
        """
        :param binder: names the bound values for statement templates, if given
        :type binder: Optional[SlotBinder]
        :return: row-value ``IN`` over the key columns
        :rtype: TextClause
        """
        columns = self.key_columns()
        value: Any = self.filter_value
        if binder is not None:
            value = binder.bind_key_in(
                action=self,
                value=value,
                column_types=[column.type for column in columns],
            )
        return cast(TextClause, sqlalchemy.tuple_(*columns).in_(value))


class KeyNullFilterAction(KeyInFilterAction):
    """
    ``isnull`` filter on a foreign key stored in several columns: a relation
    with any column null references no row, so it counts as missing::

        filter(order__isnull=True)
        # -> WHERE lines.ord_tenant IS NULL OR lines.ord_no IS NULL
    """

    @staticmethod
    def key_operator() -> str:
        """
        :return: lookup operator of the action
        :rtype: str
        """
        return "isnull"

    def get_text_clause(self, binder: Optional["SlotBinder"] = None) -> TextClause:
        """
        :param binder: slot binder of a statement template (unused, nothing is
            bound)
        :type binder: Optional[SlotBinder]
        :return: the null checks of the key columns
        :rtype: TextClause
        """
        columns = self.key_columns()
        if self.filter_value:
            return cast(
                TextClause, sqlalchemy.or_(*(column.is_(None) for column in columns))
            )
        return cast(
            TextClause, sqlalchemy.and_(*(column.isnot(None) for column in columns))
        )
