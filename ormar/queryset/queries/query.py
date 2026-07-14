from typing import TYPE_CHECKING, Any, Optional, Union, cast

import sqlalchemy
from sqlalchemy import Column, Select, Table, TextClause
from sqlalchemy.sql import Join
from sqlalchemy.sql.roles import FromClauseRole

import ormar  # noqa I100
from ormar.models.helpers.models import group_related_list
from ormar.queryset.actions.aggregation_action import AggregationAction
from ormar.queryset.actions.filter_action import FilterAction
from ormar.queryset.join import SqlJoin
from ormar.queryset.queries import FilterQuery, LimitQuery, OffsetQuery, OrderQuery

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model
    from ormar.models.excludable import ExcludableItems
    from ormar.queryset import OrderAction


class Query:
    def __init__(  # noqa CFQ002
        self,
        model_cls: type["Model"],
        filter_clauses: list[FilterAction],
        exclude_clauses: list[FilterAction],
        select_related: list,
        limit_count: Optional[int],
        offset: Optional[int],
        excludable: "ExcludableItems",
        order_bys: Optional[list["OrderAction"]],
        limit_raw_sql: bool,
        annotations: Optional[dict] = None,
        having_clauses: Optional[list] = None,
    ) -> None:
        self.query_offset = offset
        self.limit_count = limit_count
        self._select_related = select_related[:]
        self.filter_clauses = filter_clauses[:]
        self.exclude_clauses = exclude_clauses[:]
        self.excludable = excludable

        self.model_cls = model_cls
        self.table = self.model_cls.ormar_config.table

        self.used_aliases: list[str] = []

        self.select_from: Union[Join, Table, list[str]] = []
        self.columns: list[Column] = []
        self.order_columns = order_bys
        self.sorted_orders: dict[OrderAction, TextClause] = {}
        self._init_sorted_orders()

        self.limit_raw_sql = limit_raw_sql
        self.annotations = annotations or {}
        self.having_clauses = having_clauses or []
        self.aggregation_actions: list[AggregationAction] = []

    def _init_sorted_orders(self) -> None:
        """
        Initialize empty order_by dict to be populated later during the query call
        """
        if self.order_columns:
            for clause in self.order_columns:
                self.sorted_orders[clause] = None  # type: ignore

    def apply_order_bys_for_primary_model(self) -> None:  # noqa: CCR001
        """
        Applies order_by queries on main model when it's used as a subquery.
        That way the subquery with limit and offset only on main model has proper
        sorting applied and correct models are fetched.
        """
        current_table_sorted = False
        if self.order_columns:
            for clause in self.order_columns:
                if clause.is_source_model_order:
                    current_table_sorted = True
                    self.sorted_orders[clause] = clause.get_text_clause()

        if not current_table_sorted:
            self._apply_default_model_sorting()

    def _apply_default_model_sorting(self) -> None:
        """
        Applies orders_by from model OrmarConfig (if provided), if it was not provided
        it was filled by metaclass, so it's always there and falls back to pk column
        """
        for clause in ormar.OrderAction.from_model_defaults(self.model_cls):
            self.sorted_orders[clause] = clause.get_text_clause()

    def _pagination_query_required(self) -> bool:
        """
        Checks if limit or offset are set, the flag limit_sql_raw is not set
        and query has select_related applied. Otherwise we can limit/offset normally
        at the end of whole query.

        :return: result of the check
        :rtype: bool
        """
        return bool(
            (self.limit_count or self.query_offset)
            and not self.limit_raw_sql
            and self._select_related
        )

    def build_select_expression(self) -> sqlalchemy.sql.Select:
        """
        Main entry point from outside (after proper initialization).

        Extracts columns list to fetch,
        construct all required joins for select related,
        then applies all conditional and sort clauses.

        Returns ready to run query with all joins and clauses.

        :return: ready to run query with all joins and clauses.
        :rtype: sqlalchemy.sql.selectable.Select
        """
        self_related_fields = self.model_cls.own_table_columns(
            model=self.model_cls, excludable=self.excludable, use_alias=True
        )
        self.columns = self.model_cls.ormar_config.alias_manager.prefixed_columns(  # type: ignore
            "", self.table, self_related_fields
        )
        self.apply_order_bys_for_primary_model()
        self.select_from = self.table

        related_models = group_related_list(self._select_related)

        for related in related_models:
            remainder = None
            if isinstance(related_models, dict) and related_models[related]:
                remainder = related_models[related]
            sql_join = SqlJoin(
                used_aliases=self.used_aliases,
                select_from=self.select_from,  # type: ignore
                columns=self.columns,
                excludable=self.excludable,
                order_columns=self.order_columns,
                sorted_orders=self.sorted_orders,
                main_model=self.model_cls,
                relation_name=related,
                relation_str=related,
                related_models=remainder,
            )

            (
                self.used_aliases,
                self.select_from,
                self.columns,
                self.sorted_orders,
            ) = sql_join.build_join()  # type: ignore

        for name, aggregate in self.annotations.items():
            action = AggregationAction(
                name=name, aggregate=aggregate, model_cls=self.model_cls
            )
            joined = action.apply_join(self.select_from, self.table)  # type: ignore
            self.select_from = joined  # type: ignore
            self.columns.append(action.result_column)  # type: ignore
            self.aggregation_actions.append(action)

        if self._pagination_query_required():
            limit_qry, on_clause = self._build_pagination_condition()
            self.select_from = sqlalchemy.sql.join(
                cast("FromClauseRole", self.select_from), limit_qry, on_clause
            )

        expr = sqlalchemy.sql.select(*self.columns)
        expr = expr.select_from(cast("FromClauseRole", self.select_from))

        expr = self._apply_expression_modifiers(expr)

        # print("\n", expr.compile(compile_kwargs={"literal_binds": True}))
        self._reset_query_parameters()

        return expr

    def _build_pagination_condition(
        self,
    ) -> tuple[
        sqlalchemy.sql.expression.TextClause, sqlalchemy.sql.expression.TextClause
    ]:
        """
        In order to apply limit and offset on main table in join only
        (otherwise you can get only partially constructed main model
        if number of children exceeds the applied limit and select_related is used)

        Used also to change first and get() without argument behaviour.
        Needed only if limit or offset are set, the flag limit_sql_raw is not set
        and query has select_related applied. Otherwise we can limit/offset normally
        at the end of whole query.

        The condition is added to filters to filter out desired number of main model
        primary key values. Whole query is used to determine the values.
        """
        pk_alias = self.model_cls.get_column_alias(self.model_cls.ormar_config.pkname)
        pk_aliased_name = f"{self.table.name}.{pk_alias}"
        qry_text = sqlalchemy.text(f"{pk_aliased_name}")
        maxes = {}
        for order in list(self.sorted_orders.keys()):
            if order is not None and order.get_field_name_text() != pk_aliased_name:
                aliased_col = order.get_field_name_text()
                # maxes[aliased_col] = order.get_text_clause()
                maxes[aliased_col] = order.get_min_or_max()
            elif order.get_field_name_text() == pk_aliased_name:
                maxes[pk_aliased_name] = order.get_text_clause()

        limit_qry: Select[Any] = sqlalchemy.sql.select(qry_text)
        limit_qry = limit_qry.select_from(self.select_from)  # type: ignore
        limit_qry = FilterQuery(filter_clauses=self.filter_clauses).apply(limit_qry)
        limit_qry = FilterQuery(
            filter_clauses=self.exclude_clauses, exclude=True
        ).apply(limit_qry)
        limit_qry = limit_qry.group_by(qry_text)
        for order_by in maxes.values():
            limit_qry = limit_qry.order_by(order_by)
        limit_qry = LimitQuery(limit_count=self.limit_count).apply(limit_qry)
        limit_qry = OffsetQuery(query_offset=self.query_offset).apply(limit_qry)
        limit_qry = limit_qry.alias("limit_query")  # type: ignore
        on_clause = sqlalchemy.text(
            f"limit_query.{pk_alias}={self.table.name}.{pk_alias}"
        )
        return limit_qry, on_clause  # type: ignore

    def _apply_expression_modifiers(
        self, expr: sqlalchemy.sql.Select
    ) -> sqlalchemy.sql.Select:
        """
        Receives the select query (might be join) and applies:
        * Filter clauses
        * Exclude filter clauses
        * Limit clauses
        * Offset clauses
        * Order by clauses

        Returns complete ready to run query.

        :param expr: select expression before clauses
        :type expr: sqlalchemy.sql.selectable.Select
        :return: expression with all present clauses applied
        :rtype: sqlalchemy.sql.selectable.Select
        """
        expr = FilterQuery(filter_clauses=self.filter_clauses).apply(expr)
        expr = FilterQuery(filter_clauses=self.exclude_clauses, exclude=True).apply(
            expr
        )
        if not self._pagination_query_required():
            expr = LimitQuery(limit_count=self.limit_count).apply(expr)
            expr = OffsetQuery(query_offset=self.query_offset).apply(expr)
        expr = OrderQuery(sorted_orders=self.sorted_orders).apply(expr)
        return expr

    def _reset_query_parameters(self) -> None:
        """
        Although it should be created each time before the call we reset the key params
        anyway.
        """
        self.select_from = []
        self.columns = []
        self.used_aliases = []
