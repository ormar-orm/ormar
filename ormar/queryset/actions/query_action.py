import abc
from typing import TYPE_CHECKING, Any

import sqlalchemy

from ormar.queryset.utils import get_relationship_alias_model_and_str  # noqa: I202
from ormar.relations.aliases import prefixed_table

if TYPE_CHECKING:  # pragma: nocover
    from ormar import Model


class QueryAction(abc.ABC):
    """
    Base QueryAction class with common params for Filter and Order actions.
    """

    def __init__(self, query_str: str, model_cls: type["Model"]) -> None:
        self.query_str = query_str
        self.field_name: str = ""
        self.related_parts: list[str] = []
        self.related_str: str = ""

        self.table_prefix = ""
        self.source_model = model_cls
        self.target_model = model_cls
        self.is_through = False

        self._split_value_into_parts(query_str)
        self._determine_filter_target_table()

    def __eq__(self, other: object) -> bool:  # pragma: no cover
        if not isinstance(other, QueryAction):
            return False
        return self.query_str == other.query_str

    def __hash__(self) -> Any:
        return hash((self.table_prefix, self.query_str))

    @abc.abstractmethod
    def _split_value_into_parts(self, query_str: str) -> None:  # pragma: no cover
        """
        Splits string into related parts and field_name
        :param query_str: query action string to split (i..e filter or order by)
        :type query_str: str
        """
        pass

    @abc.abstractmethod
    def get_text_clause(
        self,
    ) -> sqlalchemy.sql.expression.ClauseElement:  # pragma: no cover
        pass

    @property
    def table(self) -> sqlalchemy.Table:
        """Shortcut to sqlalchemy Table of filtered target model"""
        return self.target_model.ormar_config.table

    @property
    def aliased_table(self) -> Any:
        """Table of the target model under the alias of its join, if joined"""
        if not self.table_prefix:
            return self.table
        return prefixed_table(alias=self.table_prefix, table=self.table)

    @property
    def column(self) -> sqlalchemy.Column:
        """Shortcut to sqlalchemy column of filtered target model"""
        aliased_name = self.target_model.get_column_alias(self.field_name)
        return self.target_model.ormar_config.table.columns[aliased_name]

    def update_select_related(self, select_related: list[str]) -> list[str]:
        """
        Updates list of select related with related part included in the filter key.
        That way If you want to just filter by relation you do not have to provide
        select_related separately.

        :param select_related: list of relation join strings
        :type select_related: list[str]
        :return: list of relation joins with implied joins from filter added
        :rtype: list[str]
        """
        select_related = select_related[:]
        if self.related_str and not any(
            rel.startswith(self.related_str) for rel in select_related
        ):
            select_related.append(self.related_str)
        return select_related

    def _determine_filter_target_table(self) -> None:
        """
        Walks the relation to retrieve the actual model on which the clause should be
        constructed, extracts alias based on last relation leading to target model.
        """
        (
            self.table_prefix,
            self.target_model,
            self.related_str,
            self.is_through,
        ) = get_relationship_alias_model_and_str(self.source_model, self.related_parts)
