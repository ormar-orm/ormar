from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Generator, Optional, Union

import sqlalchemy
from sqlalchemy import ColumnElement

import ormar  # noqa I100
from ormar.models.helpers.models import group_related_list, ordered_join_paths
from ormar.queryset.actions.filter_action import FilterAction
from ormar.queryset.utils import get_relationship_alias_model_and_str

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


@dataclass
class Prefix:
    source_model: type["Model"]
    table_prefix: str
    model_cls: type["Model"]
    relation_str: str
    is_through: bool

    @property
    def alias_key(self) -> str:
        source_model_name = self.source_model.get_name()
        return f"{source_model_name}_{self.relation_str}"


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
        self._complex_dup_keys: set[str] = set()

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

        self._register_complex_duplicates(select_related)
        filter_clauses = self._switch_filter_action_prefixes(
            filter_clauses=filter_clauses + own_filter_clauses
        )
        if _own_only:
            return own_filter_clauses, select_related
        return filter_clauses, select_related

    def _register_complex_duplicates(self, select_related: list[str]) -> None:
        """
        Checks if duplicate aliases are presented which can happen in self relation
        or when two joins end with the same pair of models.

        Mirrors the join builder (:meth:`ormar.queryset.join.SqlJoin._forward_join`):
        relation paths are walked in the same depth-first order the join uses, the
        first path to reach a given basic alias keeps it, and every later path that
        would reuse the same alias is registered as a complex duplicate keyed on the
        whole relation string (not just the last relation name).

        The keys registered for *this* query are stored on
        :attr:`_complex_dup_keys` so that :meth:`_verify_prefix_and_switch` only
        reroutes filters that are genuinely duplicated here, ignoring complex
        aliases that linger in the globally shared alias manager from earlier
        queries.

        :param select_related: list of relation strings
        :type select_related: list[str]
        :return: None
        :rtype: None
        """
        manager = self.model_cls.ormar_config.alias_manager
        self._complex_dup_keys = set()
        seen_prefixes: set[str] = set()
        for prefix in self._parse_related_prefixes(select_related=select_related):
            if prefix.table_prefix in seen_prefixes:
                self._complex_dup_keys.add(prefix.alias_key)
                if prefix.alias_key not in manager:
                    manager.add_alias(alias_key=prefix.alias_key)
            else:
                seen_prefixes.add(prefix.table_prefix)

    def _parse_related_prefixes(self, select_related: list[str]) -> list[Prefix]:
        """
        Walks all relation strings and parses the target models and prefixes.

        Relation strings are expanded into the depth-first order of joined tables
        (shared prefixes visited once) so the duplicate detection matches the join
        builder exactly.

        :param select_related: list of relation strings
        :type select_related: list[str]
        :return: list of parsed prefixes
        :rtype: list[Prefix]
        """
        prefixes: list[Prefix] = []
        for related in ordered_join_paths(group_related_list(select_related)):
            prefix = Prefix(
                self.model_cls,
                *get_relationship_alias_model_and_str(
                    self.model_cls, related.split("__")
                ),
            )
            prefixes.append(prefix)
        return prefixes

    def _switch_filter_action_prefixes(
        self, filter_clauses: list[FilterAction]
    ) -> list[FilterAction]:
        """
        Substitutes aliases for filter action if the complex key (whole relation str) is
        present in alias_manager.

        :param filter_clauses: raw list of actions
        :type filter_clauses: list[FilterAction]
        :return: list of actions with aliases changed if needed
        :rtype: list[FilterAction]
        """

        for action in filter_clauses:
            if isinstance(action, FilterGroup):
                for action2 in action._iter():
                    self._verify_prefix_and_switch(action2)
            else:
                self._verify_prefix_and_switch(action)
        return filter_clauses

    def _verify_prefix_and_switch(self, action: "FilterAction") -> None:
        """
        Helper to switch prefix to complex relation one if required.

        Only reroutes filters whose relation string is a complex duplicate
        registered for this query (see :meth:`_register_complex_duplicates`), so a
        complex alias left in the shared alias manager by an earlier query cannot
        hijack a filter that is not duplicated here.

        :param action: action to switch prefix in
        :type action: ormar.queryset.actions.filter_action.FilterAction
        """
        manager = self.model_cls.ormar_config.alias_manager
        alias_key = f"{self.model_cls.get_name()}_{action.related_str}"
        if "__" in action.related_str and alias_key in self._complex_dup_keys:
            action.table_prefix = manager.resolve_relation_alias(
                self.model_cls, action.related_str
            )
