from typing import TYPE_CHECKING, Any, Optional, Union

import sqlalchemy
from sqlalchemy import ColumnElement, Select, TextClause

from ormar.queryset.actions.filter_action import FilterAction

if TYPE_CHECKING:  # pragma: no cover
    from ormar.queryset.templates import SlotBinder


class FilterQuery:
    """
    Modifies the select query with given list of where/filter clauses.

    With a ``binder`` (see ``templates.SlotBinder``) the filter values become
    named bind parameters, so the built select can be cached as a statement
    template and run again with other values.
    """

    def __init__(
        self,
        filter_clauses: list[FilterAction],
        exclude: bool = False,
        binder: Optional["SlotBinder"] = None,
    ) -> None:
        self.exclude = exclude
        self.filter_clauses = filter_clauses
        self.binder = binder

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
                x.get_text_clause(binder=self.binder) for x in self.filter_clauses
            ]
            if self.exclude:
                # every exclude() call is negated on its own, all of them are ANDed
                clauses = [sqlalchemy.sql.not_(x) for x in clauses]
            clause: Union[TextClause, ColumnElement[Any]] = (
                clauses[0] if len(clauses) == 1 else sqlalchemy.sql.and_(*clauses)
            )
            expr = expr.where(clause)
        return expr
