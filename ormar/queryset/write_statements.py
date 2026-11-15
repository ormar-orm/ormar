"""
Statements of ``QuerySet.build_write_statement``, one class per ``WriteKind``.

Every statement is built in the same steps, each kind overriding only the
steps it changes::

    target    UPDATE / DELETE / SELECT <pk> FOR UPDATE
    joins     the tables the updated values read
    restrict  WHERE: the filters, the matched rows or the locked primary keys
    finish    RETURNING, for the update of ``update_returning()``

    WRITE_STATEMENTS[WriteKind.DELETE](queryset=qs, values={}, binder=b).build()
"""

import abc
from typing import TYPE_CHECKING, Any, cast

import sqlalchemy

from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.select_action import expression_columns
from ormar.queryset.expressions import Expression, expression_clause
from ormar.queryset.queries import FilterQuery
from ormar.queryset.templates import PKS_SLOT, SlotBinder, WriteKind, value_binds
from ormar.relations.aliases import alias_for, prefixed_table

if TYPE_CHECKING:  # pragma no cover
    from ormar import ForeignKeyField, Model
    from ormar.queryset.queryset import QuerySet


class WriteStatement(abc.ABC):
    """
    Filtered write of a queryset with named slots for the updated values and
    the filter values. The base restricts the rows by the filters, subclasses
    give the ``target`` and override the other steps they change.
    """

    def __init__(self, queryset: "QuerySet", values: dict, binder: SlotBinder):
        """
        :param queryset: written queryset
        :type queryset: QuerySet
        :param values: updated values by column name (empty for delete)
        :type values: dict
        :param binder: names filter values for templates
        :type binder: SlotBinder
        """
        self.queryset = queryset
        self.table = queryset.table
        self.values = values
        self.binder = binder
        self.assignments: dict = {}
        self.join_conditions: dict[str, list] = {}
        self.read_values()

    def read_values(self) -> None:
        """
        Fill the column assignments of the updated values, with expressions in
        place of the binds of the expression values, and the join conditions
        of the related tables those expressions read.
        """
        model = self.queryset.model
        names = tuple(sorted(self.values))
        self.assignments = value_binds(table=self.table, names=names)
        for index, name in enumerate(names):
            if isinstance(self.values[name], Expression):
                columns = expression_columns(node=self.values[name], model=model)
                self.assignments[self.table.c[name]] = expression_clause(
                    node=self.values[name], columns=columns, slot=f"ormar_v{index}"
                )
                for column in columns.values():
                    self.join_conditions.update(
                        forward_join_conditions(model=model, path=column.related_parts)
                    )

    def build(self) -> Any:
        """
        :return: statement to execute
        :rtype: Any
        """
        expr = self.join_read_tables(expr=self.target())
        return self.finish(expr=self.restrict(expr=expr))

    @abc.abstractmethod
    def target(self) -> Any:
        """
        :return: statement before its rows are restricted
        :rtype: Any
        """

    def join_read_tables(self, expr: Any) -> Any:
        """
        Join the tables the updated values read (``UPDATE ... FROM`` or a
        multi-table ``UPDATE`` on MySQL, as SQLAlchemy renders it).

        :param expr: statement to join
        :type expr: Any
        :return: joined statement
        :rtype: Any
        """
        for conditions in self.join_conditions.values():
            expr = expr.where(*conditions)
        return expr

    def restrict(self, expr: Any) -> Any:
        """
        Restrict the statement to the rows the queryset selects in ``all()``:
        by the primary keys it selects (see ``QuerySet.matched_rows_clause``)
        when a filter reads a related model, by the filter and exclude
        clauses otherwise.

        :param expr: statement to restrict
        :type expr: Any
        :return: restricted statement
        :rtype: Any
        """
        queryset = self.queryset
        if queryset._filters_read_related():
            return expr.where(queryset.matched_rows_clause(binder=self.binder))
        expr = FilterQuery(
            filter_clauses=queryset.filter_clauses, binder=self.binder
        ).apply(expr=expr)
        return FilterQuery(
            filter_clauses=queryset.exclude_clauses, exclude=True, binder=self.binder
        ).apply(expr=expr)

    def finish(self, expr: Any) -> Any:
        """
        :param expr: restricted statement
        :type expr: Any
        :return: statement to execute
        :rtype: Any
        """
        return expr


class UpdateWrite(WriteStatement):
    """``UPDATE ... SET <values> WHERE <filters>``"""

    def target(self) -> Any:
        """
        :return: update setting the values
        :rtype: Any
        """
        return self.table.update().values(self.assignments)


class UpdateReturningWrite(UpdateWrite):
    """``UPDATE ... WHERE <filters> RETURNING <all columns>``"""

    def finish(self, expr: Any) -> Any:
        """
        :param expr: restricted update
        :type expr: Any
        :return: update returning all columns of the table
        :rtype: Any
        """
        return expr.returning(*self.table.c)


class UpdateLockedWrite(UpdateWrite):
    """
    ``UPDATE ... WHERE <pk> IN (<locked keys>)``: the update of the rows
    ``LockWrite`` locked, the filters already chose them.
    """

    def restrict(self, expr: Any) -> Any:
        """
        :param expr: update to restrict
        :type expr: Any
        :return: update of the primary keys bound to ``PKS_SLOT``
        :rtype: Any
        """
        return expr.where(self.queryset.model_config.pk.in_clause(slot=PKS_SLOT))


class DeleteWrite(WriteStatement):
    """``DELETE ... WHERE <filters>``"""

    def target(self) -> Any:
        """
        :return: delete of the table
        :rtype: Any
        """
        return self.table.delete()


class LockWrite(WriteStatement):
    """
    ``SELECT <pk> ... WHERE <filters> FOR UPDATE``: the rows the update would
    touch, locked (``update_returning()`` without ``RETURNING``). It joins the
    tables the values read too, so it selects the same rows as the update.
    """

    def target(self) -> Any:
        """
        :return: select of the primary key locking the rows
        :rtype: Any
        """
        primary_key = self.queryset.model_config.pk
        return sqlalchemy.select(*primary_key.columns()).with_for_update(of=self.table)


WRITE_STATEMENTS: dict[WriteKind, type[WriteStatement]] = {
    WriteKind.UPDATE: UpdateWrite,
    WriteKind.DELETE: DeleteWrite,
    WriteKind.UPDATE_RETURNING: UpdateReturningWrite,
    WriteKind.LOCK: LockWrite,
    WriteKind.UPDATE_LOCKED: UpdateLockedWrite,
}


def forward_join_conditions(model: type["Model"], path: list[str]) -> dict[str, list]:
    """
    Return the join conditions of the tables an update expression reads from,
    by table alias, following a path of foreign keys from ``model``::

        forward_join_conditions(model=Track, path=["album"])
        # -> {"t1b26c7": [t1b26c7_albums.id == tracks.album]}

    Every table is aliased like in selects, so the conditions join the aliased
    columns the expression reads.

    :param model: updated model
    :type model: type[Model]
    :param path: relation names leading to the read model
    :type path: list[str]
    :raises QueryDefinitionError: if the path follows a reverse, many to many or
        through relation, which can match many rows
    :return: join conditions by table alias
    :rtype: dict[str, list]
    """
    conditions: dict[str, list] = {}
    source_table: Any = model.ormar_config.table
    for index, relation in enumerate(path):
        field = cast("ForeignKeyField", model.ormar_config.model_fields[relation])
        if field.is_multi or field.virtual or field.is_through:
            raise QueryDefinitionError(
                f"update() expressions read related models only through foreign "
                f"keys, '{'__'.join(path)}' follows the reverse or many to many "
                f"relation '{relation}', which can match many rows."
            )
        alias = alias_for("__".join(path[: index + 1]))
        target_table = prefixed_table(alias=alias, table=field.to.ormar_config.table)
        conditions[alias] = [
            target_table.c[pair.target_column] == source_table.c[pair.source_column]
            for pair in field.link.pairs
        ]
        model, source_table = field.to, target_table
    return conditions
