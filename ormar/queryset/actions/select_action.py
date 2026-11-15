import decimal
from typing import TYPE_CHECKING, Any, Callable

import sqlalchemy

from ormar.exceptions import QueryDefinitionError  # noqa: I202
from ormar.queryset.actions.query_action import QueryAction
from ormar.queryset.expressions import F, Inserted, expression_leaves

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model


class SelectAction(QueryAction):
    """
    Represents a single column target for an aggregate function on a queryset.

    Created when ``.min()`` / ``.max()`` / ``.sum()`` / ``.avg()`` is called,
    optionally traversing relations via ``__``. Also resolves the fields read
    by column expressions (see ``expression_columns``).
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


def expression_columns(
    node: Any, model: type["Model"], upsert: bool = False
) -> dict[str, SelectAction]:
    """
    Resolve the fields an expression reads, by path from ``model``, to the
    model and table alias each one is read from, like a filter on that path.

    In the ``update_values`` of an upsert (``upsert=True``) fields are the
    model's own only, as there are no joins, and ``Inserted`` fields resolve
    like ``F`` ones; anywhere else ``Inserted`` is rejected.

    :param node: expression node
    :type node: Any
    :param model: queried model
    :type model: type[Model]
    :param upsert: the expression is a value of an upsert's ``update_values``
    :type upsert: bool
    :raises QueryDefinitionError: if a path does not lead to a non-relation
        field, an upsert reads a related model or ``Inserted`` is used outside
        an upsert
    :return: resolved field actions by field path
    :rtype: dict[str, SelectAction]
    """
    columns: dict[str, SelectAction] = {}
    for leaf in expression_leaves(node=node):
        if isinstance(leaf, Inserted) and not upsert:
            raise QueryDefinitionError(
                f"Inserted('{leaf._path}') is only allowed in update_values of "
                f"create() and bulk_create() with on_conflict='update'"
            )
        if isinstance(leaf, (F, Inserted)) and leaf._path not in columns:
            name = type(leaf).__name__
            try:
                column = SelectAction(select_str=leaf._path, model_cls=model)
                field = column.target_model.ormar_config.model_fields.get(
                    column.field_name
                )
            except (KeyError, AttributeError):
                field = None
            if field is None or field.is_relation:
                raise QueryDefinitionError(
                    f"{name}('{leaf._path}') has to name a non-relation field, "
                    f"after the relations leading to its model if any."
                )
            if upsert and column.related_parts:
                raise QueryDefinitionError(
                    f"{name}('{leaf._path}') reads a related model: update_values "
                    f"of an upsert read only fields of the inserted model"
                )
            columns[leaf._path] = column
    return columns
