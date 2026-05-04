import copy
from typing import TYPE_CHECKING, Optional

import sqlalchemy
from sqlalchemy import text

from ormar.queryset.actions.query_action import QueryAction  # noqa: I100, I202

if TYPE_CHECKING:  # pragma: nocover
    from ormar import Model


class OrderAction(QueryAction):
    """
    Order Actions is populated by queryset when order_by() is called.

    All required params are extracted but kept raw until actual filter clause value
    is required -> then the action is converted into text() clause.

    Extracted in order to easily change table prefixes on complex relations.
    """

    def __init__(
        self,
        order_str: str,
        model_cls: type["Model"],
        alias: Optional[str] = None,
        nulls_ordering: Optional[str] = None,
    ) -> None:
        self.direction: str = ""
        super().__init__(query_str=order_str, model_cls=model_cls)
        self.is_source_model_order = False
        if alias:
            self.table_prefix = alias
        if self.source_model == self.target_model and "__" not in self.related_str:
            self.is_source_model_order = True
        self.nulls_ordering = nulls_ordering

    @property
    def field_alias(self) -> str:
        return self.target_model.get_column_alias(self.field_name)

    @property
    def is_postgres_bool(self) -> bool:
        dialect = self.target_model.ormar_config.database.dialect.name
        field_type = self.target_model.ormar_config.model_fields[
            self.field_name
        ].__type__
        return dialect == "postgresql" and field_type is bool

    def get_field_name_text(self) -> str:
        """
        Escapes characters if it's required.
        Substitutes values of the models if value is a ormar Model with its pk value.
        Compiles the clause.

        :return: complied and escaped clause
        :rtype: sqlalchemy.sql.elements.TextClause
        """
        prefix = f"{self.table_prefix}_" if self.table_prefix else ""
        return f"{prefix}{self.table.name}.{self.field_alias}"

    def get_min_or_max(self) -> sqlalchemy.sql.expression.TextClause:
        """
        Used in limit sub queries where you need to use aggregated functions
        in order to order by columns not included in group by. For postgres bool
        field it's using bool_or function as aggregates does not work with this type
        of columns.

        :return: min or max function to order
        :rtype: sqlalchemy.sql.elements.TextClause
        """
        reference = self.get_field_name_text()
        if self.direction == "":
            function = "min" if not self.is_postgres_bool else "bool_or"
            return text(f"{function}({reference})")
        function = "max" if not self.is_postgres_bool else "bool_or"
        return text(f"{function}({reference}) desc")

    def get_text_clause(self) -> sqlalchemy.sql.expression.TextClause:
        """
        Escapes characters if it's required.
        Substitutes values of the models if value is a ormar Model with its pk value.
        Compiles the clause.

        :return: complied and escaped clause
        :rtype: sqlalchemy.sql.elements.TextClause
        """
        dialect = self.target_model.ormar_config.database.dialect
        quoter = dialect.identifier_preparer.quote
        prefix = f"{self.table_prefix}_" if self.table_prefix else ""
        table_name = self.table.name
        field_name = self.field_alias
        if not prefix:
            table_name = quoter(table_name)
        else:
            table_name = quoter(f"{prefix}{table_name}")
        field_name = quoter(field_name)
        return text(self._build_order_expression(f"{table_name}.{field_name}"))

    def _build_order_expression(self, full_column: str) -> str:
        """
        Builds the final ORDER BY expression for a fully-qualified column,
        optionally including a `NULLS FIRST`/`NULLS LAST` annotation. On MySQL,
        which lacks the SQL:2003 `NULLS` syntax, emulate it by prepending an
        `IS NULL` / `IS NOT NULL` sort key — it only affects ordering,
        not the result set.

        :param full_column: fully-qualified, quoted column reference
        :type full_column: str
        :return: ORDER BY expression as raw SQL text
        :rtype: str
        """
        direction = f" {self.direction}" if self.direction else ""
        base = f"{full_column}{direction}"
        if self.nulls_ordering is None:
            return base
        dialect_name = self.target_model.ormar_config.database.dialect.name
        if dialect_name == "mysql":  # pragma: no cover
            not_kw = "not " if self.nulls_ordering == "first" else ""
            return f"{full_column} is {not_kw}null, {base}"
        return f"{base} nulls {self.nulls_ordering}"  # pragma: no cover

    def _split_value_into_parts(self, order_str: str) -> None:
        if order_str.startswith("-"):
            self.direction = "desc"
            order_str = order_str[1:]
        parts = order_str.split("__")
        self.field_name = parts[-1]
        self.related_parts = parts[:-1]

    @classmethod
    def from_model_defaults(cls, model_cls: type["Model"]) -> list["OrderAction"]:
        """
        Builds the default list of ``OrderAction`` instances from a model's
        ``OrmarConfig.orders_by`` (which always contains at least the primary
        key, populated by the metaclass).

        :param model_cls: model class whose defaults should be used
        :type model_cls: type["Model"]
        :return: list of default OrderAction instances for the model
        :rtype: list[OrderAction]
        """
        return [
            cls(order_str=str(name), model_cls=model_cls)
            for name in model_cls.ormar_config.orders_by
        ]

    def flipped(self) -> "OrderAction":
        """
        Returns a shallow copy of this order action with the sort direction
        and any `NULLS FIRST`/`NULLS LAST` annotation inverted.

        Used by reverse slicing to turn an ASC/DESC ordering into its mirror
        image so that ``LIMIT N`` fetches rows from the tail of the original
        ordering. Callers are responsible for reversing the result list in
        memory afterwards so the caller-visible ordering is preserved.

        :return: new OrderAction with flipped direction and nulls ordering
        :rtype: OrderAction
        """
        flipped = copy.copy(self)
        flipped.direction = "" if self.direction == "desc" else "desc"
        if self.nulls_ordering == "first":
            flipped.nulls_ordering = "last"
        elif self.nulls_ordering == "last":
            flipped.nulls_ordering = "first"
        return flipped

    def check_if_filter_apply(self, target_model: type["Model"], alias: str) -> bool:
        """
        Checks filter conditions to find if they apply to current join.

        :param target_model: model which is now processed
        :type target_model: type["Model"]
        :param alias: prefix of the relation
        :type alias: str
        :return: result of the check
        :rtype: bool
        """
        return target_model == self.target_model and alias == self.table_prefix
