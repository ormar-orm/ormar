from enum import Enum
from typing import TYPE_CHECKING, Any, Generator, Optional, Union

import sqlalchemy
from sqlalchemy import ColumnElement

import ormar  # noqa I100
from ormar.queryset.actions.filter_action import FilterAction

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model


class FilterType(Enum):
    AND = 1
    OR = 2


class NullsOrdering(str, Enum):
    """Nulls placement options for the `.order_by()` queries."""

    FIRST = "first"
    LAST = "last"


class FilterGroup:
    """
    Filter groups are used in complex queries condition to group and and or
    clauses in where condition
    """

    def __init__(
        self,
        *args: Any,
        _filter_type: FilterType = FilterType.AND,
        _exclude: bool = False,
        **kwargs: Any,
    ) -> None:
        self.filter_type = _filter_type
        self.exclude = _exclude
        self._nested_groups: list["FilterGroup"] = list(args)
        self._resolved = False
        self.is_source_model_filter = False
        self._kwargs_dict = kwargs
        self.actions: list[FilterAction] = []

    def __and__(self, other: "FilterGroup") -> "FilterGroup":
        return FilterGroup(self, other)

    def __or__(self, other: "FilterGroup") -> "FilterGroup":
        return FilterGroup(self, other, _filter_type=FilterType.OR)

    def __invert__(self) -> "FilterGroup":
        self.exclude = not self.exclude
        return self

    def resolve(
        self,
        model_cls: type["Model"],
        select_related: Optional[list] = None,
        filter_clauses: Optional[list] = None,
    ) -> tuple[list[FilterAction], list[str]]:
        """
        Resolves the FilterGroups actions to use proper target model, replace
        complex relation prefixes if needed and nested groups also resolved.

        :param model_cls: model from which the query is run
        :type model_cls: type["Model"]
        :param select_related: list of models to join
        :type select_related: list[str]
        :param filter_clauses: list of filter conditions
        :type filter_clauses: list[FilterAction]
        :return: list of filter conditions and select_related list
        :rtype: tuple[list[FilterAction], list[str]]
        """
        select_related = select_related if select_related is not None else []
        filter_clauses = filter_clauses if filter_clauses is not None else []
        qryclause = QueryClause(
            model_cls=model_cls,
            select_related=select_related,
            filter_clauses=filter_clauses,
        )
        own_filter_clauses, select_related = qryclause.prepare_filter(
            _own_only=True, **self._kwargs_dict
        )
        self.actions = own_filter_clauses
        filter_clauses = filter_clauses + own_filter_clauses
        self._resolved = True
        if self._nested_groups:
            for group in self._nested_groups:
                filter_clauses, select_related = group.resolve(
                    model_cls=model_cls,
                    select_related=select_related,
                    filter_clauses=filter_clauses,
                )
        return filter_clauses, select_related

    def _iter(self) -> Generator:
        """
        Iterates all actions in a tree
        :return: generator yielding from own actions and nested groups
        :rtype: Generator
        """
        for group in self._nested_groups:
            yield from group._iter()
        yield from self.actions

    def _get_text_clauses(
        self,
    ) -> list[Union[sqlalchemy.sql.expression.TextClause, ColumnElement[Any]]]:
        """
        Helper to return list of text queries from actions and nested groups
        :return: list of text queries from actions and nested groups
        :rtype: list[sqlalchemy.sql.elements.TextClause]
        """
        return [x.get_text_clause() for x in self._nested_groups] + [
            x.get_text_clause() for x in self.actions
        ]

    def get_text_clause(self) -> ColumnElement[bool]:
        """
        Returns all own actions and nested groups conditions compiled and joined
        inside parentheses.
        Escapes characters if it's required.
        Substitutes values of the models if value is a ormar Model with its pk value.
        Compiles the clause.

        :return: complied and escaped clause
        :rtype: sqlalchemy.sql.elements.TextClause
        """
        if self.filter_type == FilterType.AND:
            clause = sqlalchemy.sql.and_(*self._get_text_clauses()).self_group()
        else:
            clause = sqlalchemy.sql.or_(*self._get_text_clauses()).self_group()
        if self.exclude:
            clause = sqlalchemy.sql.not_(clause)
        return clause


def or_(*args: FilterGroup, **kwargs: Any) -> FilterGroup:
    """
    Construct or filter from nested groups and keyword arguments

    :param args: nested filter groups
    :type args: tuple[FilterGroup]
    :param kwargs: fields names and proper value types
    :type kwargs: Any
    :return: FilterGroup ready to be resolved
    :rtype: ormar.queryset.clause.FilterGroup
    """
    return FilterGroup(_filter_type=FilterType.OR, *args, **kwargs)


def and_(*args: FilterGroup, **kwargs: Any) -> FilterGroup:
    """
    Construct and filter from nested groups and keyword arguments

    :param args: nested filter groups
    :type args: tuple[FilterGroup]
    :param kwargs: fields names and proper value types
    :type kwargs: Any
    :return: FilterGroup ready to be resolved
    :rtype: ormar.queryset.clause.FilterGroup
    """
    return FilterGroup(_filter_type=FilterType.AND, *args, **kwargs)


class QueryClause:
    """
    Constructs FilterActions from strings passed as arguments
    """

    def __init__(
        self, model_cls: type["Model"], filter_clauses: list, select_related: list
    ) -> None:
        self._select_related = select_related[:]
        self.filter_clauses = filter_clauses[:]

        self.model_cls = model_cls
        self.table = self.model_cls.ormar_config.table

    def prepare_filter(  # noqa: A003
        self, _own_only: bool = False, **kwargs: Any
    ) -> tuple[list[FilterAction], list[str]]:
        """
        Main external access point that processes the clauses into sqlalchemy text
        clauses and updates select_related list with implicit related tables
        mentioned in select_related strings but not included in select_related.

        :param _own_only:
        :type _own_only:
        :param kwargs: key, value pair with column names and values
        :type kwargs: Any
        :return: tuple with list of where clauses and updated select_related list
        :rtype: tuple[list[sqlalchemy.sql.elements.TextClause], list[str]]
        """
        if kwargs.get("pk"):
            pk_name = self.model_cls.get_column_alias(
                self.model_cls.ormar_config.pkname
            )
            kwargs[pk_name] = kwargs.pop("pk")

        filter_clauses, select_related = self._populate_filter_clauses(
            _own_only=_own_only, **kwargs
        )

        return filter_clauses, select_related

    def _populate_filter_clauses(
        self, _own_only: bool, **kwargs: Any
    ) -> tuple[list[FilterAction], list[str]]:
        """
        Iterates all clauses and extracts used operator and field from related
        models if needed. Based on the chain of related names the target table
        is determined and the final clause is escaped if needed and compiled.

        :param kwargs: key, value pair with column names and values
        :type kwargs: Any
        :return: tuple with list of where clauses and updated select_related list
        :rtype: tuple[list[sqlalchemy.sql.elements.TextClause], list[str]]
        """
        filter_clauses = self.filter_clauses
        own_filter_clauses = []
        select_related = list(self._select_related)

        for key, value in kwargs.items():
            filter_action = FilterAction(
                filter_str=key, value=value, model_cls=self.model_cls
            )
            select_related = filter_action.update_select_related(
                select_related=select_related
            )

            own_filter_clauses.append(filter_action)

        if _own_only:
            return own_filter_clauses, select_related
        return filter_clauses + own_filter_clauses, select_related
