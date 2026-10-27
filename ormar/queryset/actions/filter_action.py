from typing import TYPE_CHECKING, Any, Optional, cast

import sqlalchemy
from sqlalchemy import TextClause

import ormar  # noqa: I100, I202
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.query_action import QueryAction
from ormar.relations.aliases import prefixed_table

if TYPE_CHECKING:  # pragma: nocover
    from ormar import Model
    from ormar.queryset.templates import SlotBinder

FILTER_OPERATORS = {
    "exact": "__eq__",
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


class FilterAction(QueryAction):
    """
    Represents a single WHERE predicate on a queryset.

    Created for each ``key=value`` (or ``key__op=value``) passed to
    ``filter(...)`` / ``exclude(...)`` and to filtering shortcuts like
    ``get(...)`` / ``first(...)``.
    """

    def __init__(self, filter_str: str, value: Any, model_cls: type["Model"]) -> None:
        super().__init__(query_str=filter_str, model_cls=model_cls)
        self.filter_value = value
        self._escape_characters_in_clause()

    def has_escaped_characters(self) -> bool:
        """Check if value is a string that contains characters to escape"""
        return isinstance(self.filter_value, str) and any(
            c for c in ESCAPE_CHARACTERS if c in self.filter_value
        )

    def _split_value_into_parts(self, query_str: str) -> None:
        parts = query_str.split("__")
        if parts[-1] in FILTER_OPERATORS:
            self.operator = parts[-1]
            self.field_name = parts[-2]
            self.related_parts = parts[:-2]
        else:
            self.operator = "exact"
            self.field_name = parts[-1]
            self.related_parts = parts[:-1]

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
        primary key, so ``filter(author=author)`` binds ``author.pk``.

        ``filter_value`` itself is left untouched, so the action can be read
        again on every call to collect the value for a cached template.

        :return: value to bind
        :rtype: Any
        """
        if isinstance(self.filter_value, ormar.Model):
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
        else:
            filter_value = value
            if binder is not None and self.binds_slot():
                filter_value = binder.bind(action=self, value=value)
        if self.table_prefix:
            aliased_table = prefixed_table(self.table_prefix, self.column.table)
            aliased_column = getattr(aliased_table.c, self.column.name)
        else:
            aliased_column = self.column
        clause = getattr(aliased_column, op_attr)(filter_value)
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
    shares one statement.
    """

    def __init__(
        self, *, prefix: str, values: list[tuple[Any, ...]], model_cls: type["Model"]
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
        target_key = model_cls.ormar_config.pk
        for part in prefix.split("__")[:-1]:
            target_key = target_key.owner.ormar_config.model_fields[
                part
            ].to.ormar_config.pk
        super().__init__(
            filter_str=f"{prefix}{target_key.fields[0]}__in",
            value=values,
            model_cls=model_cls,
        )
        self.query_str = f"{prefix}pk__in"

    def get_text_clause(self, binder: Optional["SlotBinder"] = None) -> TextClause:
        """
        :param binder: names the bound values for statement templates, if given
        :type binder: Optional[SlotBinder]
        :return: row-value ``IN`` over the key columns
        :rtype: TextClause
        """
        columns = list(self.target_model.ormar_config.pk.columns())
        if self.table_prefix:
            aliased_table = prefixed_table(self.table_prefix, self.column.table)
            columns = [getattr(aliased_table.c, column.name) for column in columns]
        value: Any = self.filter_value
        if binder is not None:
            value = binder.bind_key_in(
                action=self,
                value=value,
                column_types=[column.type for column in columns],
            )
        return cast(TextClause, sqlalchemy.tuple_(*columns).in_(value))
