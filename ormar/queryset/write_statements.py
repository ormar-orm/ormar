"""
UPDATE, DELETE and locking SELECT statements of a model: filtered by a
queryset (``QuerySet.build_write_template``) or by primary key
(``row_template``, ``bulk_update``).

Every statement is built in the same steps, each class overriding only the
steps it changes::

    target    UPDATE / DELETE / SELECT
    joins     the tables the updated values read
    restrict  WHERE: the filters, the matched rows or the primary key(s)
    finish    RETURNING or FOR UPDATE

    DeleteWrite(model=Author, queryset=qs, binder=binder).build()
    DeleteWrite.make(model=Author)  # DELETE ... WHERE authors.id = :ormar_pk
"""

import abc
from typing import TYPE_CHECKING, Any, Mapping, Optional, cast

import sqlalchemy

from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.select_action import expression_columns
from ormar.queryset.expressions import Expression, expression_clause
from ormar.queryset.queries import FilterQuery
from ormar.queryset.templates import (
    PKS_SLOT,
    SlotBinder,
    StatementTemplate,
    cached_statement,
    value_binds,
    value_params,
)
from ormar.relations.aliases import alias_for, prefixed_table

if TYPE_CHECKING:  # pragma no cover
    from ormar import ForeignKeyField, Model
    from ormar.models.primary_key import PrimaryKey
    from ormar.queryset.queryset import QuerySet


class WriteStatement(abc.ABC):
    """
    Write of a model with named slots for the updated values and the filter
    values: of the rows of a queryset when one is given, of one row by
    primary key (``ormar_pk`` slots) otherwise. Subclasses give the
    ``target`` and override the other steps they change.

    :cvar row_key: cache key of the statement by primary key (``row_template``)
    """

    row_key = ""

    def __init__(
        self,
        model: type["Model"],
        queryset: Optional["QuerySet"] = None,
        values: Optional[Mapping[str, Any]] = None,
        binder: Optional[SlotBinder] = None,
    ):
        """
        :param model: written model
        :type model: type[Model]
        :param queryset: queryset selecting the written rows, if not by key
        :type queryset: Optional[QuerySet]
        :param values: updated values by column name (none for delete)
        :type values: Optional[Mapping[str, Any]]
        :param binder: names filter values for templates
        :type binder: Optional[SlotBinder]
        """
        self.model = model
        self.queryset = queryset
        self.table = model.ormar_config.table
        self.primary_key = model.ormar_config.pk
        self.values = values or {}
        self.binder = binder
        self.join_conditions: dict[str, list] = {}
        self.assignments = self.read_values()

    @classmethod
    def make(cls, **kwargs: Any) -> Any:
        """
        :param kwargs: arguments of the statement
        :type kwargs: Any
        :return: built statement, a builder of ``cached_statement``
        :rtype: Any
        """
        return cls(**kwargs).build()

    def read_values(self) -> dict:
        """
        Return the column assignments of the updated values, with expressions
        in place of the binds of the expression values, and collect the join
        conditions of the related tables those expressions read.

        :return: bind or expression per column
        :rtype: dict
        """
        names = tuple(sorted(self.values))
        assignments = value_binds(table=self.table, names=names)
        for index, name in enumerate(names):
            if isinstance(self.values[name], Expression):
                columns = expression_columns(node=self.values[name], model=self.model)
                assignments[self.table.c[name]] = expression_clause(
                    node=self.values[name], columns=columns, slot=f"ormar_v{index}"
                )
                for column in columns.values():
                    self.join_conditions.update(
                        forward_join_conditions(
                            model=self.model, path=column.related_parts
                        )
                    )
        return assignments

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
        Restrict the statement to one row by primary key without a queryset,
        else to the rows the queryset selects in ``all()``: by the primary
        keys it selects (see ``QuerySet.matched_rows_clause``) when a filter
        reads a related model, by the filter and exclude clauses otherwise.

        :param expr: statement to restrict
        :type expr: Any
        :return: restricted statement
        :rtype: Any
        """
        queryset = self.queryset
        if queryset is None:
            slots = self.primary_key.bind_slots()
            return expr.where(self.primary_key.where_clause(slots=slots))
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

    @staticmethod
    def key_params(primary_key: "PrimaryKey", key: Any) -> dict:
        """
        :param primary_key: primary key of the model
        :type primary_key: PrimaryKey
        :param key: primary key value of the row
        :type key: Any
        :return: parameters binding the key (see ``row_template``)
        :rtype: dict
        """
        return primary_key.bind_params(value=key)


class UpdateWrite(WriteStatement):
    """``UPDATE ... SET <values> WHERE <filters or pk>``"""

    row_key = "update"

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
        :param expr: statement to restrict
        :type expr: Any
        :return: statement of the primary keys bound to ``PKS_SLOT``
        :rtype: Any
        """
        return expr.where(self.primary_key.in_clause(slot=PKS_SLOT))


class BulkUpdateWrite(UpdateWrite):
    """
    ``UPDATE ... SET <col>=:new_<col> WHERE <pk> = :<prefix><pk>``: the per-row
    update of ``bulk_update``, run once per row by ``executemany``::

        UPDATE authors SET name=:new_name WHERE authors.id = :new_id
        # rows: [{"new_id": 1, "new_name": "a"}, {"new_id": 2, "new_name": "b"}]
    """

    def __init__(
        self, model: type["Model"], columns: tuple[str, ...], where_prefix: str
    ):
        """
        :param model: updated model
        :type model: type[Model]
        :param columns: updated column names
        :type columns: tuple[str, ...]
        :param where_prefix: prefix of the key parameters matching each row:
            ``new_`` for the current key, ``old_`` for the persisted key when a
            key in the batch was changed (the key columns are then also updated)
        :type where_prefix: str
        """
        super().__init__(model=model)
        self.where_prefix = where_prefix
        self.assignments = {
            name: sqlalchemy.bindparam(key="new_" + name) for name in columns
        }

    def restrict(self, expr: Any) -> Any:
        """
        :param expr: update to restrict
        :type expr: Any
        :return: update of the row with the prefixed untyped key parameters
        :rtype: Any
        """
        aliases = self.primary_key.column_aliases()
        slots = tuple(self.where_prefix + alias for alias in aliases)
        return expr.where(self.primary_key.where_clause(slots=slots, typed=False))


class DeleteWrite(WriteStatement):
    """``DELETE ... WHERE <filters or pk>``"""

    row_key = "delete"

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
        primary_key = self.primary_key.columns()
        return sqlalchemy.select(*primary_key).with_for_update(of=self.table)


class RowSelectWrite(WriteStatement):
    """
    ``SELECT <all columns> ... WHERE <pk> = :ormar_pk``: the row of
    ``Model.load()`` and of the existence check of ``upsert(force_save=True)``.
    """

    row_key = "select_pk"

    def target(self) -> Any:
        """
        :return: select of all columns of the table
        :rtype: Any
        """
        return self.table.select()


class RowsLockWrite(RowSelectWrite):
    """
    ``SELECT <all columns> ... WHERE <pk> IN (:ormar_pks) FOR UPDATE``: the
    rows ``update_returning()`` reads back where the database has no
    ``UPDATE ... RETURNING``. A locking read sees the latest committed row
    versions, a plain read in a MySQL REPEATABLE READ transaction would see its
    snapshot. The update already locked the rows, so the lock costs nothing.
    """

    row_key = "select_pks"
    restrict = UpdateLockedWrite.restrict

    def finish(self, expr: Any) -> Any:
        """
        :param expr: restricted select
        :type expr: Any
        :return: select locking the rows
        :rtype: Any
        """
        return expr.with_for_update()

    @staticmethod
    def key_params(primary_key: "PrimaryKey", key: Any) -> dict:
        """
        :param primary_key: primary key of the model
        :type primary_key: PrimaryKey
        :param key: primary key values, tuples of column values for a
            composite key
        :type key: Any
        :return: parameters binding the keys to ``PKS_SLOT``
        :rtype: dict
        """
        return {PKS_SLOT: key}


def row_template(
    statement: type[WriteStatement],
    model_cls: type["Model"],
    pk: Any,
    values: Optional[Mapping[str, Any]] = None,
) -> StatementTemplate:
    """
    Return a write of the rows of a model by primary key, from the model's
    template cache, with the values to bind: ``Model.update()``,
    ``Model.delete()``, ``Model.load()`` and the read back of
    ``update_returning()``::

        row_template(UpdateWrite, model_cls=Author, pk=1, values={"name": "b"})
        # -> UPDATE authors SET name=:ormar_v0 WHERE authors.id = :ormar_pk
        #    {"ormar_v0": "b", "ormar_pk": 1}

    One statement is cached per class and, for an update, set of columns.

    :param statement: statement class
    :type statement: type[WriteStatement]
    :param model_cls: written model
    :type model_cls: type[Model]
    :param pk: primary key of the row (keys of the rows for ``RowsLockWrite``)
    :type pk: Any
    :param values: updated values by column name, for an update
    :type values: Optional[Mapping[str, Any]]
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    params = statement.key_params(primary_key=model_cls.ormar_config.pk, key=pk)
    if values is None:
        key: tuple = (statement.row_key,)
    else:
        names = tuple(sorted(values))
        key = (statement.row_key, names)
        params.update(value_params(names=names, values=values))
    cached = cached_statement(
        model_cls=model_cls,
        key=key,
        builder=statement.make,
        model=model_cls,
        values=values,
    )
    return StatementTemplate(statement=cached, params=params)


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
