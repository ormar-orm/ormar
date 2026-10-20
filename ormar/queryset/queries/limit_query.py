from typing import TYPE_CHECKING, Optional

import sqlalchemy

if TYPE_CHECKING:  # pragma: no cover
    from ormar.queryset.templates import SlotBinder


class LimitQuery:
    """
    Modifies the select query with limit clause.
    """

    def __init__(
        self, limit_count: Optional[int], binder: Optional["SlotBinder"] = None
    ) -> None:
        self.limit_count = limit_count
        self.binder = binder

    def apply(self, expr: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        """
        Applies the limit clause.

        :param expr: query to modify
        :type expr: sqlalchemy.sql.selectable.Select
        :return: modified query
        :rtype: sqlalchemy.sql.selectable.Select
        """

        if self.limit_count is not None:
            value = (
                self.binder.limit(value=self.limit_count)
                if self.binder is not None
                else self.limit_count
            )
            expr = expr.limit(limit=value)

        return expr
