"""Builds the derived-table join that backs a single ``annotate`` aggregate."""

from typing import TYPE_CHECKING, Optional

import sqlalchemy

from ormar.queryset.aggregations import AggregateFunction
from ormar.queryset.utils import get_relationship_alias_model_and_str

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model


class AggregationAction:
    """
    Compiles one ``annotate(name=Func("relation[__column]"))`` entry into a
    pre-grouped derived table joined 1:1 onto the parent by primary key.

    :param name: label of the annotation in the result set
    :type name: str
    :param aggregate: the requested aggregate function value object
    :type aggregate: AggregateFunction
    :param model_cls: the queried (parent) model
    :type model_cls: type["Model"]
    """

    def __init__(
        self, name: str, aggregate: AggregateFunction, model_cls: type["Model"]
    ) -> None:
        self.name = name
        self.aggregate = aggregate
        self.source_model = model_cls
        parts = aggregate.field.split("__")
        self.relation_name = parts[0]
        self.column_name: Optional[str] = parts[1] if len(parts) > 1 else None
        _, self.target_model, self.related_str, _ = (
            get_relationship_alias_model_and_str(model_cls, [self.relation_name])
        )
        self.result_column: sqlalchemy.sql.ColumnElement = None  # type: ignore

    def _child_group_key(self) -> sqlalchemy.Column:
        """
        Returns the child-table FK column pointing back to the parent.

        Mirrors ``SqlJoin._get_to_and_from_keys`` (``ormar/queryset/join.py``)
        for the reverse-FK (``virtual``) branch: the relation field stored on
        the parent model exposes ``get_related_name()``, which resolves to
        the name of the FK field declared on the child model. That name is
        then translated to its database column alias on the child table.

        :return: child table column used as the ``GROUP BY`` key
        :rtype: sqlalchemy.Column
        """
        relation_field = self.source_model.ormar_config.model_fields[self.relation_name]
        related_name = relation_field.get_related_name()
        fk_alias = self.target_model.get_column_alias(related_name)
        return self.target_model.ormar_config.table.columns[fk_alias]

    def _aggregate_target(self) -> sqlalchemy.sql.ColumnElement:
        """
        Returns the column the aggregate function is applied to.

        :return: ``*`` literal for count-all, otherwise the resolved column
        :rtype: sqlalchemy.sql.ColumnElement
        """
        if self.column_name is None:
            return sqlalchemy.literal_column("*")
        col_alias = self.target_model.get_column_alias(self.column_name)
        return self.target_model.ormar_config.table.columns[col_alias]

    def apply_join(
        self,
        select_from: sqlalchemy.sql.expression.FromClause,
        parent_table: sqlalchemy.Table,
    ) -> sqlalchemy.sql.expression.FromClause:
        """
        Builds the grouped derived table, LEFT JOINs it to ``select_from`` on the
        parent primary key, stores the labelled result column and returns the new
        from-clause.

        :param select_from: current from-clause the join is appended to
        :type select_from: sqlalchemy.sql.expression.FromClause
        :param parent_table: table (or alias) of the queried (parent) model
        :type parent_table: sqlalchemy.Table
        :return: from-clause extended with the derived-table LEFT JOIN
        :rtype: sqlalchemy.sql.expression.FromClause
        """
        group_key = self._child_group_key()
        target = self._aggregate_target()
        func = getattr(sqlalchemy.func, self.aggregate.function_name)
        expr = func(target.distinct()) if self.aggregate.distinct else func(target)
        derived = (
            sqlalchemy.select(group_key.label("ormar_agg_key"), expr.label(self.name))
            .group_by(group_key)
            .alias(f"{self.name}_agg")
        )
        pk_alias = self.source_model.get_column_alias(
            self.source_model.ormar_config.pkname
        )
        parent_pk = parent_table.columns[pk_alias]
        value: sqlalchemy.sql.ColumnElement = derived.c[self.name]
        if self.aggregate.function_name == "count":
            value = sqlalchemy.func.coalesce(value, 0)
        self.result_column = value.label(self.name)
        return sqlalchemy.sql.outerjoin(
            select_from, derived, derived.c.ormar_agg_key == parent_pk
        )

    def order_text(self, descending: bool) -> sqlalchemy.sql.expression.TextClause:
        """
        Returns an ORDER BY clause referencing the annotation label.

        :param descending: whether to sort in descending order
        :type descending: bool
        :return: text clause quoting the annotation label
        :rtype: sqlalchemy.sql.expression.TextClause
        """
        direction = " desc" if descending else ""
        return sqlalchemy.text(f'"{self.name}"{direction}')
