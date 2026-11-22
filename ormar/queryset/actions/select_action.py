import decimal
from typing import TYPE_CHECKING, Any

import sqlalchemy

from ormar.exceptions import QueryDefinitionError  # noqa: I202
from ormar.queryset.actions.query_action import QueryAction
from ormar.queryset.expressions import F, Inserted, expression_leaves
from ormar.relations.aliases import alias_for

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
        column_name = self.target_model.get_column_alias(field_name=self.field_name)
        return sqlalchemy.column(f"{alias}{column_name}")

    @property
    def result_label(self) -> str:
        """
        :return: label of the aggregated column in the result row, its dunder path
            without the through model name
        :rtype: str
        """
        rel_prefix = f"{self.related_str}__" if self.related_str else ""
        return f"{rel_prefix}{self.field_name}"

    def repeated_by(self, many_paths: set[str]) -> bool:
        """
        Checks if a join of a relation holding many models repeats the rows the
        column is read from, which is the case for every such relation that is
        not on the column's own relation path, e.g. ``"articles"`` for
        ``rating`` but not for ``articles__score`` from ``Writer``.

        :param many_paths: dunder paths of the joined relations holding many
            models
        :type many_paths: set[str]
        :return: whether some of the joins repeat the column's rows
        :rtype: bool
        """
        return any(
            self.related_str != many_path
            and not self.related_str.startswith(f"{many_path}__")
            for many_path in many_paths
        )

    def row_key_names(self) -> tuple[str, ...]:
        """
        Returns the labels of the primary key columns of the queried model and of
        every table joined on the way to the column, through tables of many to
        many relations included. Together they identify one row of the related
        model the column is read from, e.g. ``("id", "t1a2b3c4d5_id")`` for
        ``articles__score`` from ``Writer``, so the rows repeated by other joins
        can be dropped.

        :return: primary key column labels in the joined select
        :rtype: tuple[str, ...]
        """
        tables: list[tuple[str, type["Model"]]] = [("", self.source_model)]
        target_model = self.source_model
        path_parts: list[str] = []
        for relation in self.related_parts:
            related_field = target_model.ormar_config.model_fields[relation]
            path_parts.append(relation)
            relation_path = "__".join(path_parts)
            if related_field.is_multi:
                through_model = related_field.through
                through_path = f"{relation_path}__{through_model.get_name()}"
                tables.append((alias_for(through_path), through_model))
            target_model = related_field.to
            tables.append((alias_for(relation_path), target_model))
        return tuple(
            f"{table_prefix}_{column_name}" if table_prefix else column_name
            for table_prefix, model in tables
            for column_name in model.ormar_config.pk.column_aliases()
        )


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
