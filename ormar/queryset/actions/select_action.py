import decimal
from typing import TYPE_CHECKING, Any, Callable

import sqlalchemy

from ormar.queryset.actions.query_action import QueryAction  # noqa: I202

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model


class SelectAction(QueryAction):
    """
    Represents a single column target for an aggregate function on a queryset.

    Created when ``.min()`` / ``.max()`` / ``.sum()`` / ``.avg()`` is called,
    optionally traversing relations via ``__``.
    """

    def __init__(self, select_str: str, model_cls: type["Model"]) -> None:
        super().__init__(query_str=select_str, model_cls=model_cls)

    def _split_value_into_parts(self, order_str: str) -> None:
        parts = order_str.split("__")
        self.field_name = parts[-1]
        self.related_parts = parts[:-1]

    @property
    def is_numeric(self) -> bool:
        return self.get_target_field_type() in [int, float, decimal.Decimal]

    def get_target_field_type(self) -> Any:
        return self.target_model.ormar_config.model_fields[self.field_name].__type__

    def get_text_clause(self) -> sqlalchemy.sql.expression.ColumnClause:
        alias = f"{self.table_prefix}_" if self.table_prefix else ""
        return sqlalchemy.column(f"{alias}{self.field_name}")

    def apply_func(
        self, func: Callable, use_label: bool = True
    ) -> sqlalchemy.sql.expression.ColumnElement:
        result = func(self.get_text_clause())
        if use_label:
            rel_prefix = f"{self.related_str}__" if self.related_str else ""
            result = result.label(f"{rel_prefix}{self.field_name}")
        return result
