from typing import TYPE_CHECKING, Any, Optional, cast

import sqlalchemy
from sqlalchemy import TextClause, text

import ormar  # noqa I100
from ormar.exceptions import ModelDefinitionError, RelationshipInstanceError
from ormar.relations import AliasManager

if TYPE_CHECKING:  # pragma no cover
    from ormar import ManyToManyField, Model
    from ormar.models.excludable import ExcludableItems
    from ormar.queryset import OrderAction


class SqlJoin:
    def __init__(  # noqa:  CFQ002
        self,
        used_aliases: list,
        select_from: sqlalchemy.sql.Select,
        columns: list[sqlalchemy.Column],
        excludable: "ExcludableItems",
        order_columns: Optional[list["OrderAction"]],
        sorted_orders: dict,
        main_model: type["Model"],
        relation_name: str,
        relation_str: str,
        related_models: Any = None,
        own_alias: str = "",
        source_model: Optional[type["Model"]] = None,
        already_sorted: Optional[dict] = None,
    ) -> None:
        self.relation_name = relation_name
        self.related_models = related_models or []
        self.select_from = select_from
        self.columns = columns
        self.excludable = excludable

        self.order_columns = order_columns
        self.sorted_orders = sorted_orders
        self.already_sorted = already_sorted or dict()

        self.main_model = main_model
        self.own_alias = own_alias
        self.used_aliases = used_aliases
        self.target_field = self.main_model.ormar_config.model_fields[
            self.relation_name
        ]

        self._next_model: Optional[type["Model"]] = None
        self._next_alias: Optional[str] = None

        self.relation_str = relation_str
        self.source_model = source_model

    @property
    def next_model(self) -> type["Model"]:
        if not self._next_model:  # pragma: nocover
            raise RelationshipInstanceError(
                "Cannot link to related table if relation.to model is not set."
            )
        return self._next_model

    @next_model.setter
    def next_model(self, value: type["Model"]) -> None:
        self._next_model = value

    @property
    def next_alias(self) -> str:
        if not self._next_alias:  # pragma: nocover
            raise RelationshipInstanceError("Alias for given relation not found.")
        return self._next_alias

    @next_alias.setter
    def next_alias(self, value: str) -> None:
        self._next_alias = value

    @property
    def alias_manager(self) -> AliasManager:
        """
        Shortcut for ormar's model AliasManager stored on OrmarConfig.

        :return: alias manager from model's OrmarConfig
        :rtype: AliasManager
        """
        return self.main_model.ormar_config.alias_manager

    @property
    def to_table(self) -> sqlalchemy.Table:
        """
        Shortcut to table name of the next model
        :return: name of the target table
        :rtype: str
        """
        return self.next_model.ormar_config.table

    def _on_clause(
        self,
        previous_alias: str,
        from_table_name: str,
        from_column_name: str,
        to_table_name: str,
        to_column_name: str,
    ) -> TextClause:
        """
        Receives aliases and names of both ends of the join and combines them
        into one text clause used in joins.

        :param previous_alias: alias of previous table
        :type previous_alias: str
        :param from_table_name: from table name
        :type from_table_name: str
        :param from_column_name: from column name
        :type from_column_name: str
        :param to_table_name: to table name
        :type to_table_name: str
        :param to_column_name: to column name
        :type to_column_name: str
        :return: clause combining all strings
        :rtype: sqlalchemy.text
        """
        dialect = self.main_model.ormar_config.database.dialect
        quoter = dialect.identifier_preparer.quote
        left_part = (
            f"{quoter(f'{self.next_alias}_{to_table_name}')}.{quoter(to_column_name)}"
        )
        if not previous_alias:
            right_part = f"{quoter(from_table_name)}.{quoter(from_column_name)}"
        else:
            right_part = (
                f"{quoter(f'{previous_alias}_{from_table_name}')}.{from_column_name}"
            )

        return text(f"{left_part}={right_part}")

    def build_join(self) -> tuple[list, sqlalchemy.sql.Select, list, dict]:
        """
        Main external access point for building a join.
        Splits the join definition, updates fields and exclude_fields if needed,
        handles switching to through models for m2m relations, returns updated lists of
        used_aliases and sort_orders.

        :return: list of used aliases, select from, list of aliased columns, sort orders
        :rtype: tuple[list[str], Join, list[TextClause], dict]
        """
        if self.target_field.is_multi:
            self._process_m2m_through_table()

        self.next_model = self.target_field.to
        self._forward_join()

        self._process_following_joins()

        return (self.used_aliases, self.select_from, self.columns, self.sorted_orders)

    def _forward_join(self) -> None:
        """
        Process actual join.
        Registers complex relation join on encountering of the duplicated alias.
        """
        self.next_alias = self.alias_manager.resolve_relation_alias(
            from_model=self.target_field.owner, relation_name=self.relation_name
        )
        if self.next_alias not in self.used_aliases:
            self._process_join()
        else:
            if "__" in self.relation_str and self.source_model:
                relation_key = f"{self.source_model.get_name()}_{self.relation_str}"
                if relation_key not in self.alias_manager:
                    self.next_alias = self.alias_manager.add_alias(
                        alias_key=relation_key
                    )
                else:
                    self.next_alias = self.alias_manager[relation_key]
                self._process_join()

    def _process_following_joins(self) -> None:
        """
        Iterates through nested models to create subsequent joins.
        """
        for related_name in self.related_models:
            remainder = None
            if (
                isinstance(self.related_models, dict)
                and self.related_models[related_name]
            ):
                remainder = self.related_models[related_name]
            self._process_deeper_join(related_name=related_name, remainder=remainder)

    def _process_deeper_join(self, related_name: str, remainder: Any) -> None:
        """
        Creates nested recurrent instance of SqlJoin for each nested join table,
        updating needed return params here as a side effect.

        Updated are:

        * self.used_aliases,
        * self.select_from,
        * self.columns,
        * self.sorted_orders,

        :param related_name: name of the relation to follow
        :type related_name: str
        :param remainder: deeper tables if there are more nested joins
        :type remainder: Any
        """
        sql_join = SqlJoin(
            used_aliases=self.used_aliases,
            select_from=self.select_from,
            columns=self.columns,
            excludable=self.excludable,
            order_columns=self.order_columns,
            sorted_orders=self.sorted_orders,
            main_model=self.next_model,
            relation_name=related_name,
            related_models=remainder,
            relation_str="__".join([self.relation_str, related_name]),
            own_alias=self.next_alias,
            source_model=self.source_model or self.main_model,
            already_sorted=self.already_sorted,
        )
        (
            self.used_aliases,
            self.select_from,
            self.columns,
            self.sorted_orders,
        ) = sql_join.build_join()

    def _process_m2m_through_table(self) -> None:
        """
        Process Through table of the ManyToMany relation so that source table is
        linked to the through table (one additional join)

        Replaces needed parameters like:

        * self.next_model,
        * self.next_alias,
        * self.relation_name,
        * self.own_alias,
        * self.target_field

        To point to through model
        """
        new_part = self._process_m2m_related_name_change()

        self.next_model = self.target_field.through
        self._forward_join()

        self.relation_name = new_part
        self.own_alias = self.next_alias
        self.target_field = self.next_model.ormar_config.model_fields[
            self.relation_name
        ]

    def _process_m2m_related_name_change(self, reverse: bool = False) -> str:
        """
        Extracts relation name to link join through the Through model declared on
        relation field.

        Changes the same names in order_by queries if they are present.

        :param reverse: flag if it's on_clause lookup - use reverse fields
        :type reverse: bool
        :return: new relation name switched to through model field
        :rtype: str
        """
        target_field = self.target_field
        is_primary_self_ref = (
            target_field.self_reference
            and self.relation_name == target_field.self_reference_primary
        )
        if (is_primary_self_ref and not reverse) or (
            not is_primary_self_ref and reverse
        ):
            new_part = target_field.default_source_field_name()  # type: ignore
        else:
            new_part = target_field.default_target_field_name()  # type: ignore
        return new_part

    def _process_join(self) -> None:  # noqa: CFQ002
        """
        Resolves to and from column names and table names.

        Produces on_clause.

        Performs actual join updating select_from parameter.

        Adds aliases of required column to list of columns to include in query.

        Updates the used aliases list directly.

        Process order_by causes for non m2m relations.

        """
        to_key, from_key = self._get_to_and_from_keys()

        on_clause = self._on_clause(
            previous_alias=self.own_alias,
            from_table_name=self.target_field.owner.ormar_config.tablename,
            from_column_name=from_key,
            to_table_name=self.to_table.name,
            to_column_name=to_key,
        )
        target_table = self.alias_manager.prefixed_table_name(
            self.next_alias, self.to_table
        )
        self.select_from = sqlalchemy.sql.outerjoin(
            self.select_from,  # type: ignore
            target_table,
            on_clause,
        )

        self._get_order_bys()

        self_related_fields = self.next_model.own_table_columns(
            model=self.next_model,
            excludable=self.excludable,
            alias=self.next_alias,
            use_alias=True,
        )
        self.columns.extend(
            self.alias_manager.prefixed_columns(  # type: ignore
                self.next_alias,
                target_table,  # type: ignore
                self_related_fields,
            )
        )
        self.used_aliases.append(self.next_alias)

    def _set_default_primary_key_order_by(self) -> None:
        for order_by in self.next_model.ormar_config.orders_by:
            clause = ormar.OrderAction(
                order_str=order_by, model_cls=self.next_model, alias=self.next_alias
            )
            self.sorted_orders[clause] = clause.get_text_clause()

    def _verify_allowed_order_field(self, order_by: str) -> None:
        """
        Verifies if proper field string is used.
        :param order_by: string with order by definition
        :type order_by: str
        """
        parts = order_by.split("__")
        if len(parts) > 2 or parts[0] != self.target_field.through.get_name():
            raise ModelDefinitionError(
                "You can order the relation only by related or link table columns!"
            )

    def _get_alias_and_model(self, order_by: str) -> tuple[str, type["Model"]]:
        """
        Returns proper model and alias to be applied in the clause.

        :param order_by: string with order by definition
        :type order_by: str
        :return: alias and model to be used in clause
        :rtype: tuple[str, type["Model"]]
        """
        if self.target_field.is_multi and "__" in order_by:
            self._verify_allowed_order_field(order_by=order_by)
            alias = self.next_alias
            model = self.target_field.owner
        elif self.target_field.is_multi:
            alias = self.alias_manager.resolve_relation_alias(
                from_model=self.target_field.through,
                relation_name=cast(
                    "ManyToManyField", self.target_field
                ).get_relation_name(),
            )
            model = self.target_field.to
        else:
            alias = self.alias_manager.resolve_relation_alias(
                from_model=self.target_field.owner, relation_name=self.target_field.name
            )
            model = self.target_field.to

        return alias, model

    def _get_order_bys(self) -> None:  # noqa: CCR001
        """
        Triggers construction of order bys if they are given.
        Otherwise by default each table is sorted by a primary key column asc.
        """
        alias = self.next_alias
        current_table_sorted = False
        if f"{alias}_{self.next_model.get_name()}" in self.already_sorted:
            current_table_sorted = True
        if self.order_columns:
            for condition in self.order_columns:
                if condition.check_if_filter_apply(
                    target_model=self.next_model, alias=alias
                ):
                    current_table_sorted = True
                    self.sorted_orders[condition] = condition.get_text_clause()
                    self.already_sorted[
                        f"{self.next_alias}_{self.next_model.get_name()}"
                    ] = condition
        if self.target_field.orders_by and not current_table_sorted:
            current_table_sorted = True
            for order_by in self.target_field.orders_by:
                alias, model = self._get_alias_and_model(order_by=order_by)
                clause = ormar.OrderAction(
                    order_str=order_by, model_cls=model, alias=alias
                )
                self.sorted_orders[clause] = clause.get_text_clause()
                self.already_sorted[f"{alias}_{model.get_name()}"] = clause

        if not current_table_sorted and not self.target_field.is_multi:
            self._set_default_primary_key_order_by()

    def _get_to_and_from_keys(self) -> tuple[str, str]:
        """
        Based on the relation type, name of the relation and previous models and parts
        stored in JoinParameters it resolves the current to and from keys, which are
        different for ManyToMany relation, ForeignKey and reverse related of relations.

        :return: to key and from key
        :rtype: tuple[str, str]
        """
        if self.target_field.is_multi:
            to_key = self._process_m2m_related_name_change(reverse=True)
            from_key = self.main_model.get_column_alias(
                self.main_model.ormar_config.pkname
            )

        elif self.target_field.virtual:
            to_field = self.target_field.get_related_name()
            to_key = self.target_field.to.get_column_alias(to_field)
            from_key = self.main_model.get_column_alias(
                self.main_model.ormar_config.pkname
            )

        else:
            to_key = self.target_field.to.get_column_alias(
                self.target_field.to.ormar_config.pkname
            )
            from_key = self.main_model.get_column_alias(self.relation_name)

        return to_key, from_key
