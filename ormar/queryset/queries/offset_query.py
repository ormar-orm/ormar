from typing import TYPE_CHECKING, Optional

import sqlalchemy

if TYPE_CHECKING:  # pragma: no cover
    from ormar.queryset.templates import SlotBinder


class OffsetQuery:
    """
    Modifies the select query with offset if set

    With a ``binder`` (see ``templates.SlotBinder``) the offset is bound as
    ``:ormar_offset``, so one cached statement serves every non-zero offset.
    """

    def __init__(
        self, query_offset: Optional[int], binder: Optional["SlotBinder"] = None
    ) -> None:
        self.query_offset = query_offset
        self.binder = binder

    def apply(self, expr: sqlalchemy.sql.Select) -> sqlalchemy.sql.Select:
        """
        Applies the offset clause.

        :param expr: query to modify
        :type expr: sqlalchemy.sql.selectable.Select
        :return: modified query
        :rtype: sqlalchemy.sql.selectable.Select
        """
        if self.query_offset:
            value = (
                self.binder.offset(value=self.query_offset)
                if self.binder is not None
                else self.query_offset
            )
            expr = expr.offset(offset=value)
        return expr
