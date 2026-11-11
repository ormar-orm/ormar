from typing import TYPE_CHECKING, Any, Optional, cast

import sqlalchemy
from sqlalchemy import TextClause

import ormar  # noqa: I100, I202
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.query_action import QueryAction
from ormar.queryset.actions.select_action import SelectAction, expression_columns
from ormar.queryset.expressions import Expression, expression_clause

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
ESCAPE_CHARACTERS = ["%", "_"]


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
            if self.operator not in ("exact", "ne", "gt", "gte", "lt", "lte"):
                raise QueryDefinitionError(
                    f"Expressions only take exact, ne, gt, gte, lt and lte filters, "
                    f"not '{self.operator}'"
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

    def has_escaped_characters(self) -> bool:
        """Check if value is a string that contains characters to escape"""
        return isinstance(self.filter_value, str) and any(
            c for c in ESCAPE_CHARACTERS if c in self.filter_value
        )

    def _split_value_into_parts(self, query_str: str) -> None:
        self.operator, path = split_filter_key(key=query_str)
        self.field_name = path[-1]
        self.related_parts = path[:-1]

    def _escape_characters_in_clause(self) -> None:
        """
        Escapes the special characters ["%", "_"] if needed.
        Adds `%` for `like` queries.

        :raises QueryDefinitionError: if contains or icontains is used with
        ormar model instance
        :return: escaped value and flag if escaping is needed
        :rtype: tuple[Any, bool]
        """
        self.has_escaped_character = False
        if self.operator in [
            "contains",
            "icontains",
            "startswith",
            "istartswith",
            "endswith",
            "iendswith",
        ]:
            if isinstance(self.filter_value, ormar.Model):
                raise QueryDefinitionError(
                    "You cannot use contains and icontains with instance of the Model"
                )
            self.has_escaped_character = self.has_escaped_characters()
            if self.has_escaped_character:
                self._escape_chars()
            self._prefix_suffix_quote()

    def _escape_chars(self) -> None:
        """Actually replaces chars to escape in value"""
        for char in ESCAPE_CHARACTERS:
            self.filter_value = self.filter_value.replace(char, f"\\{char}")

    def _prefix_suffix_quote(self) -> None:
        """
        Adds % to the beginning of the value if operator checks for containment and not
        starts with.

        Adds % to the end of the value if operator checks for containment and not
        end with.
        :return:
        :rtype:
        """
        prefix = "%" if "start" not in self.operator else ""
        sufix = "%" if "end" not in self.operator else ""
        self.filter_value = f"{prefix}{self.filter_value}{sufix}"

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
        op_attr = FILTER_OPERATORS[self.operator]
        if self.operator == "isnull":
            op_attr = "is_" if value else "isnot"
            filter_value = None
        elif isinstance(value, Expression):
            filter_value = expression_clause(
                node=value,
                columns=self.expression_columns,
                slot=binder.slots[id(self)] if binder is not None else None,
            )
        else:
            filter_value = value
            if binder is not None and self.binds_slot():
                filter_value = binder.bind(action=self, value=value)
        clause = getattr(self.aliased_column, op_attr)(filter_value)
        if self.has_escaped_character:
            clause.modifiers["escape"] = "\\"
        return clause


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
