import copy
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Optional

import sqlalchemy
from sqlalchemy import text

from ormar.queryset.actions.query_action import QueryAction  # noqa: I100, I202

if TYPE_CHECKING:  # pragma: nocover
    from ormar import Model


#: order string of a random order, as in Django
RANDOM_ORDER = "?"


@lru_cache(maxsize=2048)
def order_text_clause(expression: str) -> sqlalchemy.sql.expression.TextClause:
    """
    Builds a text clause for an ORDER BY expression, cached as text clauses are
    immutable and the same orderings (e.g. default pk sort) repeat on every query.

    :param expression: raw ORDER BY expression
    :type expression: str
    :return: text clause for the expression
    :rtype: sqlalchemy.sql.expression.TextClause
    """
    return text(expression)


class OrderAction(QueryAction):
    """
    Represents a single ORDER BY directive on a queryset.

    Created when ``order_by(...)`` or ``Field.asc()`` / ``Field.desc()`` is
    used, and internally to apply the default primary-key ordering.
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
    def field_aliases(self) -> tuple[str, ...]:
        """
        :return: columns the field is ordered by: its column, or every column
            of a foreign key stored in several columns
        :rtype: tuple[str, ...]
        """
        return self.target_model.get_column_aliases(field_name=self.field_name)

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
        prefix = f"{self.table_prefix}_" if self.table_prefix else ""
        config = self.target_model.ormar_config
        function = config.database.rules.ordering_aggregate(
            field_type=config.model_fields[self.field_name].__type__,
            descending=bool(self.direction),
        )
        suffix = " desc" if self.direction else ""
        return text(
            ", ".join(
                f"{function}({prefix}{self.table.name}.{alias}){suffix}"
                for alias in self.field_aliases
            )
        )

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
        table_name = quoter(f"{prefix}{self.table.name}")
        return order_text_clause(
            ", ".join(
                self._build_order_expression(f"{table_name}.{quoter(alias)}")
                for alias in self.field_aliases
            )
        )

    def _build_order_expression(self, full_column: str) -> str:
        """
        Builds the final ORDER BY expression for a fully-qualified column,
        optionally including a `NULLS FIRST`/`NULLS LAST` annotation (emulated
        by the dialect rules where the syntax is missing).

        :param full_column: fully-qualified, quoted column reference
        :type full_column: str
        :return: ORDER BY expression as raw SQL text
        :rtype: str
        """
        direction = f" {self.direction}" if self.direction else ""
        base = f"{full_column}{direction}"
        if self.nulls_ordering is None:
            return base
        return self.target_model.ormar_config.database.rules.nulls_order(
            full_column=full_column, ordered=base, nulls_ordering=self.nulls_ordering
        )

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
            action
            for name in model_cls.ormar_config.orders_by
            for action in order_actions_for(order_str=str(name), model_cls=model_cls)
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


class RandomOrderAction(OrderAction):
    """
    Orders the rows of the queried model randomly, created for
    ``order_by("?")``. It reads no field, so it has no relation path, and like
    an order on the model's own field it replaces the default primary key
    order.
    """

    def __init__(self, model_cls: type["Model"]) -> None:
        super().__init__(order_str=RANDOM_ORDER, model_cls=model_cls)

    def _split_value_into_parts(self, order_str: str) -> None:
        self.field_name = ""
        self.related_parts = []

    def _random_order(self) -> str:
        return self.source_model.ormar_config.database.rules.random_order

    def get_field_name_text(self) -> str:
        """
        :return: the random function, which keys the ordering of the limited
            sub query of the main model
        :rtype: str
        """
        return self._random_order()

    def get_min_or_max(self) -> sqlalchemy.sql.expression.TextClause:
        """
        :return: the random function, it needs no aggregate in the grouped
            limit sub query
        :rtype: sqlalchemy.sql.elements.TextClause
        """
        return self.get_text_clause()

    def get_text_clause(self) -> sqlalchemy.sql.expression.TextClause:
        """
        :return: the random function of the database dialect
        :rtype: sqlalchemy.sql.elements.TextClause
        """
        return order_text_clause(self._random_order())


class RandomOrder:
    """
    Random order of the queried model, the ``ormar.random()`` form of
    ``order_by("?")``.
    """

    def __repr__(self) -> str:
        return "ormar.random()"


def random() -> RandomOrder:
    """
    Order the rows randomly, same as ``"?"``::

        await User.objects.order_by(ormar.random()).limit(3).all()

    :return: random order to pass to ``order_by()``
    :rtype: RandomOrder
    """
    return RandomOrder()


def order_actions_for(
    order_str: str,
    model_cls: type["Model"],
    alias: Optional[str] = None,
    nulls_ordering: Optional[str] = None,
) -> list[OrderAction]:
    """
    Build the order actions for one order string, expanding ``pk`` (also after
    relations) into the primary key fields of the model it points at, in key
    order, and ``?`` into a random order::

        "-pk"         -> ["-tenant_id", "-number"]
        "-stock__pk"  -> ["-stock__code", "-stock__sku"]
        "shop__pk"    -> ["shop__id"]
        "?"           -> [RandomOrderAction]

    :param order_str: order string, ``-`` prefixed for descending order
    :type order_str: str
    :param model_cls: model the order string starts from
    :type model_cls: type[Model]
    :param alias: table alias of the ordered model, if already known
    :type alias: Optional[str]
    :param nulls_ordering: optional nulls placement, ``"first"`` or ``"last"``
    :type nulls_ordering: Optional[str]
    :return: order actions, one per key field for a ``pk`` order
    :rtype: list[OrderAction]
    """
    if order_str.lstrip("-") == RANDOM_ORDER:
        return [RandomOrderAction(model_cls=model_cls)]
    action = OrderAction(
        order_str=order_str,
        model_cls=model_cls,
        alias=alias,
        nulls_ordering=nulls_ordering,
    )
    if action.field_name != "pk":
        return [action]
    prefix = order_str[: -len("pk")]
    return [
        OrderAction(
            order_str=f"{prefix}{field}",
            model_cls=model_cls,
            alias=alias,
            nulls_ordering=nulls_ordering,
        )
        for field in action.target_model.ormar_config.pk.fields
    ]


def expand_pk_orders(model_cls: type["Model"], columns: list[Any]) -> list[OrderAction]:
    """
    Turn an ``order_by`` list into order actions, expanding ``pk`` orders (also
    after relations and from ``Model.pk.asc()`` / ``Model.rel.pk.desc()``) into
    the primary key fields with :func:`order_actions_for`.

    :param model_cls: ordered model
    :type model_cls: type[Model]
    :param columns: order strings, ``OrderAction`` or ``RandomOrder`` instances
    :type columns: list[Any]
    :return: order actions with ``pk`` expanded
    :rtype: list[OrderAction]
    """
    expanded: list[OrderAction] = []
    for column in columns:
        if isinstance(column, RandomOrder):
            expanded.append(RandomOrderAction(model_cls=model_cls))
        elif isinstance(column, str):
            expanded.extend(order_actions_for(order_str=column, model_cls=model_cls))
        elif column.field_name == "pk":
            expanded.extend(
                order_actions_for(
                    order_str=column.query_str,
                    model_cls=model_cls,
                    nulls_ordering=column.nulls_ordering,
                )
            )
        else:
            expanded.append(column)
    return expanded
