from typing import Any, Union

import sqlalchemy
from sqlalchemy import ColumnElement, Select, TextClause

from ormar.queryset.actions.filter_action import FilterAction


class FilterQuery:
    """
    Modifies the select query with given list of where/filter clauses.
    """

    def __init__(
        self, filter_clauses: list[FilterAction], exclude: bool = False
    ) -> None:
        self.exclude = exclude
        self.filter_clauses = filter_clauses

    def apply(
        self,
        expr: Select[Any],
    ) -> Select[Any]:
        """
        Applies all filter clauses if set.

        :param expr: query to modify
        :type expr: sqlalchemy.sql.selectable.Select
        :return: modified query
        :rtype: sqlalchemy.sql.selectable.Select
        """
        if self.filter_clauses:
            clauses: list[Union[TextClause, ColumnElement[Any]]] = [
                x.get_text_clause() for x in self.filter_clauses
            ]
            if self.exclude:
                # every exclude() call is its own NOT, all of them are ANDed
                clauses = [sqlalchemy.sql.not_(x) for x in clauses]
            clause: Union[TextClause, ColumnElement[Any]] = (
                clauses[0] if len(clauses) == 1 else sqlalchemy.sql.and_(*clauses)
            )
            expr = expr.where(clause)
        return expr
