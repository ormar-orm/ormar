"""
Write templates of a model: single-row insert, update, delete and the select
by primary key, plus the statements of ``bulk_create`` and ``bulk_update``.

Each runs one cached statement per model and set of written columns::

    insert_template(model_cls=Author, values={"name": "a", "score": 1})
    # -> INSERT INTO authors (name, score) VALUES (:ormar_v0, :ormar_v1)
    #    {"ormar_v0": "a", "ormar_v1": 1}
"""

import weakref
from typing import (
    Any,
    Dict,
    Mapping,
    Tuple,
    Type,
)

import sqlalchemy
from sqlalchemy.sql import Executable

from ormar.queryset.templates import cross_check
from ormar.queryset.templates.cache import StatementTemplate, cached_statement
from ormar.queryset.templates.slots import (
    PK_SLOT,
    primary_key_bind,
    value_binds,
    value_params,
)

# primary key column per model class, looked up on the first write
PK_COLUMNS: "weakref.WeakKeyDictionary[type, sqlalchemy.Column]" = (
    weakref.WeakKeyDictionary()
)


def primary_key_column(model_cls: Type[Any]) -> sqlalchemy.Column:
    """
    Return the primary key column of a model's table, resolved once per model,
    so a write does not look up the column alias of the primary key field
    every time.

    :param model_cls: model class
    :type model_cls: Type[Any]
    :return: primary key column
    :rtype: sqlalchemy.Column
    """
    column = PK_COLUMNS.get(model_cls)
    if column is None:
        alias = model_cls.get_column_alias(field_name=model_cls.ormar_config.pkname)
        column = PK_COLUMNS.setdefault(model_cls, model_cls.ormar_config.table.c[alias])
    return column


def literal_values(
    table: sqlalchemy.Table, names: Tuple[str, ...], values: Mapping[str, Any]
) -> Dict[Any, Any]:
    """
    Return the written values keyed by column, in the template's column order.
    Only the cross check uses it, to build the statement with literal values
    as without templates.

    :param table: written table
    :type table: sqlalchemy.Table
    :param names: written column names, sorted
    :type names: Tuple[str, ...]
    :param values: written values by column name
    :type values: Mapping[str, Any]
    :return: value per column
    :rtype: Dict[Any, Any]
    """
    return {table.c[name]: values[name] for name in names}


def build_insert(table: sqlalchemy.Table, names: Tuple[str, ...]) -> Executable:
    """
    Build an insert of the given columns with named value binds::

        INSERT INTO authors (name, score) VALUES (:ormar_v0, :ormar_v1)

    The names are sorted by the caller, so the same columns given in any
    order share one statement.

    :param table: written table
    :type table: sqlalchemy.Table
    :param names: written column names, sorted
    :type names: Tuple[str, ...]
    :return: insert statement
    :rtype: Executable
    """
    return table.insert().values(value_binds(table=table, names=names))


def build_update(
    table: sqlalchemy.Table, names: Tuple[str, ...], pk_column: sqlalchemy.Column
) -> Executable:
    """
    Build an update of the given columns of one row, by primary key::

        UPDATE authors SET name=:ormar_v0, score=:ormar_v1
        WHERE authors.id = :ormar_pk

    :param table: written table
    :type table: sqlalchemy.Table
    :param names: updated column names, sorted
    :type names: Tuple[str, ...]
    :param pk_column: primary key column
    :type pk_column: sqlalchemy.Column
    :return: update statement
    :rtype: Executable
    """
    return (
        table.update()
        .values(value_binds(table=table, names=names))
        .where(pk_column == primary_key_bind(column=pk_column))
    )


def build_delete(table: sqlalchemy.Table, pk_column: sqlalchemy.Column) -> Executable:
    """
    Build a delete of one row, by primary key::

        DELETE FROM authors WHERE authors.id = :ormar_pk

    :param table: written table
    :type table: sqlalchemy.Table
    :param pk_column: primary key column
    :type pk_column: sqlalchemy.Column
    :return: delete statement
    :rtype: Executable
    """
    return table.delete().where(pk_column == primary_key_bind(column=pk_column))


def build_pk_select(
    table: sqlalchemy.Table, pk_column: sqlalchemy.Column
) -> Executable:
    """
    Build a select of all columns of one row, by primary key::

        SELECT authors.id, authors.name, ... FROM authors
        WHERE authors.id = :ormar_pk

    :param table: queried table
    :type table: sqlalchemy.Table
    :param pk_column: primary key column
    :type pk_column: sqlalchemy.Column
    :return: select statement
    :rtype: Executable
    """
    return table.select().where(pk_column == primary_key_bind(column=pk_column))


def build_bulk_update(
    table: sqlalchemy.Table, pk_column: sqlalchemy.Column, columns: Tuple[str, ...]
) -> Executable:
    """
    Build the per-row update used by ``bulk_update``: values and the primary
    key come from ``new_<column>`` parameters of each row, and the driver's
    ``executemany`` runs it once per row::

        UPDATE authors SET name=:new_name WHERE authors.id = :new_id
        # rows: [{"new_id": 1, "new_name": "a"}, {"new_id": 2, "new_name": "b"}]

    :param table: updated table
    :type table: sqlalchemy.Table
    :param pk_column: primary key column
    :type pk_column: sqlalchemy.Column
    :param columns: updated column names
    :type columns: Tuple[str, ...]
    :return: update statement
    :rtype: Executable
    """
    return (
        table.update()
        .where(pk_column == sqlalchemy.bindparam(key="new_" + pk_column.name))
        .values({name: sqlalchemy.bindparam(key="new_" + name) for name in columns})
    )


def insert_statement(model_cls: Type[Any], names: Tuple[str, ...]) -> Executable:
    """
    Return the insert of a model for the given sorted column names, from the
    model's template cache. ``save()`` binds one row to it and
    ``bulk_create`` runs it with ``executemany`` for every row with the same
    columns.

    :param model_cls: inserted model
    :type model_cls: Type[Any]
    :param names: inserted column names, sorted
    :type names: Tuple[str, ...]
    :return: insert statement with ``ormar_v{i}`` binds
    :rtype: Executable
    """
    table = model_cls.ormar_config.table
    return cached_statement(
        model_cls=model_cls,
        key=("insert", names),
        builder=build_insert,
        table=table,
        names=names,
    )


def insert_template(
    model_cls: Type[Any], values: Mapping[str, Any]
) -> StatementTemplate:
    """
    Return the insert of a model for the given column values, used by
    ``save()`` and one-row ``bulk_create``::

        insert_template(model_cls=Author, values={"name": "a", "score": 1})
        # -> INSERT INTO authors (name, score) VALUES (:ormar_v0, :ormar_v1)
        #    {"ormar_v0": "a", "ormar_v1": 1}

    Rows that leave out the primary key or a column with a server default
    write a different set of columns, so each set gets its own statement.

    :param model_cls: inserted model
    :type model_cls: Type[Any]
    :param values: values by column name
    :type values: Mapping[str, Any]
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    names = tuple(sorted(values))
    statement = insert_statement(model_cls=model_cls, names=names)
    params = value_params(names=names, values=values)
    table = model_cls.ormar_config.table
    if cross_check.ENABLED:
        literal = table.insert().values(
            literal_values(table=table, names=names, values=values)
        )
        cross_check.cross_check_write(
            model_cls=model_cls, statement=statement, literal=literal, params=params
        )
    return StatementTemplate(statement=statement, params=params)


def update_template(
    model_cls: Type[Any], values: Mapping[str, Any], pk: Any
) -> StatementTemplate:
    """
    Return the update of one row of a model for the given column values, used
    by ``Model.update()``::

        update_template(model_cls=Author, values={"name": "b"}, pk=1)
        # -> UPDATE authors SET name=:ormar_v0 WHERE authors.id = :ormar_pk
        #    {"ormar_v0": "b", "ormar_pk": 1}

    :param model_cls: updated model
    :type model_cls: Type[Any]
    :param values: values by column name
    :type values: Mapping[str, Any]
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    names = tuple(sorted(values))
    table = model_cls.ormar_config.table
    pk_column = primary_key_column(model_cls=model_cls)
    statement = cached_statement(
        model_cls=model_cls,
        key=("update", names),
        builder=build_update,
        table=table,
        names=names,
        pk_column=pk_column,
    )
    params = {**value_params(names=names, values=values), PK_SLOT: pk}
    if cross_check.ENABLED:
        literal = (
            table.update()
            .values(literal_values(table=table, names=names, values=values))
            .where(pk_column == pk)
        )
        cross_check.cross_check_write(
            model_cls=model_cls, statement=statement, literal=literal, params=params
        )
    return StatementTemplate(statement=statement, params=params)


def delete_template(model_cls: Type[Any], pk: Any) -> StatementTemplate:
    """
    Return the delete of one row of a model, used by ``Model.delete()``::

        delete_template(model_cls=Author, pk=1)
        # -> DELETE FROM authors WHERE authors.id = :ormar_pk, {"ormar_pk": 1}

    :param model_cls: model
    :type model_cls: Type[Any]
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    table = model_cls.ormar_config.table
    pk_column = primary_key_column(model_cls=model_cls)
    statement = cached_statement(
        model_cls=model_cls,
        key=("delete",),
        builder=build_delete,
        table=table,
        pk_column=pk_column,
    )
    params = {PK_SLOT: pk}
    if cross_check.ENABLED:
        cross_check.cross_check_write(
            model_cls=model_cls,
            statement=statement,
            literal=table.delete().where(pk_column == pk),
            params=params,
        )
    return StatementTemplate(statement=statement, params=params)


def pk_select_template(model_cls: Type[Any], pk: Any) -> StatementTemplate:
    """
    Return the select of one row of a model by primary key, used by
    ``Model.load()`` and the existence check of ``upsert(force_save=True)``::

        pk_select_template(model_cls=Author, pk=1)
        # -> SELECT ... FROM authors WHERE authors.id = :ormar_pk, {"ormar_pk": 1}

    :param model_cls: model
    :type model_cls: Type[Any]
    :param pk: primary key of the row
    :type pk: Any
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    table = model_cls.ormar_config.table
    pk_column = primary_key_column(model_cls=model_cls)
    statement = cached_statement(
        model_cls=model_cls,
        key=("select_pk",),
        builder=build_pk_select,
        table=table,
        pk_column=pk_column,
    )
    params = {PK_SLOT: pk}
    if cross_check.ENABLED:
        cross_check.cross_check_write(
            model_cls=model_cls,
            statement=statement,
            literal=table.select().where(pk_column == pk),
            params=params,
        )
    return StatementTemplate(statement=statement, params=params)
