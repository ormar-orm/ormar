"""
Write templates of a model: single-row insert, update, delete and the select
by primary key, plus the statements of ``bulk_create`` and ``bulk_update``.

Each runs one cached statement per model and set of written columns::

    insert_template(model_cls=Author, values={"name": "a", "score": 1})
    # -> INSERT INTO authors (name, score) VALUES (:ormar_v0, :ormar_v1)
    #    {"ormar_v0": "a", "ormar_v1": 1}
"""

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
)

import sqlalchemy
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.sql import Executable

from ormar.queryset.templates.cache import StatementTemplate, cached_statement
from ormar.queryset.templates.slots import value_binds, value_params

if TYPE_CHECKING:  # pragma: no cover
    from ormar.models.primary_key import PrimaryKey


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


DIALECT_INSERTS: dict[str, Callable[..., Any]] = {
    "mysql": mysql.insert,
    "postgresql": postgresql.insert,
    "sqlite": sqlite.insert,
}


def build_conflict_insert(
    table: sqlalchemy.Table,
    names: Tuple[str, ...],
    primary_key: "PrimaryKey",
    dialect_name: str,
    on_conflict: str,
    target: Tuple[str, ...],
    update_names: Tuple[str, ...],
    on_update: Tuple[str, ...],
    returning: bool,
) -> Executable:
    """
    Build an insert that skips or updates a row conflicting with an existing
    one::

        INSERT INTO tags (name, score) VALUES (:ormar_v0, :ormar_v1)
        ON CONFLICT (name) DO UPDATE SET score = excluded.score
        -- MySQL: ON DUPLICATE KEY UPDATE score = VALUES(score)

    MySQL reacts to a conflict on any unique key, so the target is not
    emitted there, and ``ignore`` becomes a no-op update of the first key
    column (a duplicate key error would abort the whole ``executemany``).
    Columns in ``on_update`` are set from ``ormar_ou_<column>`` parameters.

    :param table: written table
    :type table: sqlalchemy.Table
    :param names: inserted column names, sorted
    :type names: Tuple[str, ...]
    :param primary_key: primary key of the written model
    :type primary_key: PrimaryKey
    :param dialect_name: name of the database dialect
    :type dialect_name: str
    :param on_conflict: ``ignore`` or ``update``
    :type on_conflict: str
    :param target: conflicting column names
    :type target: Tuple[str, ...]
    :param update_names: columns updated from the inserted row
    :type update_names: Tuple[str, ...]
    :param on_update: columns updated with their ``on_update`` value
    :type on_update: Tuple[str, ...]
    :param returning: return the primary key of a written row
    :type returning: bool
    :return: insert statement
    :rtype: Executable
    """
    statement = DIALECT_INSERTS[dialect_name](table).values(
        value_binds(table=table, names=names)
    )
    excluded = statement.inserted if dialect_name == "mysql" else statement.excluded
    set_ = {
        **{name: excluded[name] for name in update_names},
        **{
            name: sqlalchemy.bindparam(key=f"ormar_ou_{name}", type_=table.c[name].type)
            for name in on_update
        },
    }
    if dialect_name == "mysql":
        key_column = primary_key.column_aliases()[0]
        return statement.on_duplicate_key_update(
            set_ if on_conflict == "update" else {key_column: table.c[key_column]}
        )
    if on_conflict == "update":
        statement = statement.on_conflict_do_update(index_elements=target, set_=set_)
    else:
        statement = statement.on_conflict_do_nothing()
    return statement.returning(*primary_key.columns()) if returning else statement


def build_update(
    table: sqlalchemy.Table, names: Tuple[str, ...], primary_key: "PrimaryKey"
) -> Executable:
    """
    Build an update of the given columns of one row, by primary key::

        UPDATE authors SET name=:ormar_v0, score=:ormar_v1
        WHERE authors.id = :ormar_pk

    :param table: written table
    :type table: sqlalchemy.Table
    :param names: updated column names, sorted
    :type names: Tuple[str, ...]
    :param primary_key: primary key of the written model
    :type primary_key: PrimaryKey
    :return: update statement
    :rtype: Executable
    """
    return (
        table.update()
        .values(value_binds(table=table, names=names))
        .where(primary_key.where_clause(slots=primary_key.bind_slots()))
    )


def build_delete(table: sqlalchemy.Table, primary_key: "PrimaryKey") -> Executable:
    """
    Build a delete of one row, by primary key::

        DELETE FROM authors WHERE authors.id = :ormar_pk

    :param table: written table
    :type table: sqlalchemy.Table
    :param primary_key: primary key of the written model
    :type primary_key: PrimaryKey
    :return: delete statement
    :rtype: Executable
    """
    return table.delete().where(
        primary_key.where_clause(slots=primary_key.bind_slots())
    )


def build_pk_select(table: sqlalchemy.Table, primary_key: "PrimaryKey") -> Executable:
    """
    Build a select of all columns of one row, by primary key::

        SELECT authors.id, authors.name, ... FROM authors
        WHERE authors.id = :ormar_pk

    :param table: queried table
    :type table: sqlalchemy.Table
    :param primary_key: primary key of the queried model
    :type primary_key: PrimaryKey
    :return: select statement
    :rtype: Executable
    """
    return table.select().where(
        primary_key.where_clause(slots=primary_key.bind_slots())
    )


def build_bulk_update(
    table: sqlalchemy.Table,
    primary_key: "PrimaryKey",
    columns: Tuple[str, ...],
    where_prefix: str = "new_",
) -> Executable:
    """
    Build the per-row update used by ``bulk_update``: values and the primary
    key come from ``new_<column>`` parameters of each row, and the driver's
    ``executemany`` runs it once per row::

        UPDATE authors SET name=:new_name WHERE authors.id = :new_id
        # rows: [{"new_id": 1, "new_name": "a"}, {"new_id": 2, "new_name": "b"}]

    :param table: updated table
    :type table: sqlalchemy.Table
    :param primary_key: primary key of the updated model
    :type primary_key: PrimaryKey
    :param columns: updated column names
    :type columns: Tuple[str, ...]
    :param where_prefix: prefix of the key parameters matching each row:
        ``new_`` for the current key, ``old_`` for the persisted key when a
        key in the batch was changed (the key columns are then also updated)
    :type where_prefix: str
    :return: update statement
    :rtype: Executable
    """
    slots = tuple(where_prefix + alias for alias in primary_key.column_aliases())
    return (
        table.update()
        .where(primary_key.where_clause(slots=slots, typed=False))
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


def conflict_insert_statement(
    model_cls: Type[Any],
    names: Tuple[str, ...],
    on_conflict: str,
    conflict_target: Optional[Sequence[str]] = None,
    update_fields: Optional[Sequence[str]] = None,
    on_update: Tuple[str, ...] = (),
    returning: bool = False,
) -> Executable:
    """
    Return the insert of a model that skips or updates conflicting rows, from
    the model's template cache (see ``build_conflict_insert``).

    The target defaults to the primary key columns and the updated columns to
    the inserted ones other than the target, the key and ``on_update``.

    :param model_cls: inserted model
    :type model_cls: Type[Any]
    :param names: inserted column names, sorted
    :type names: Tuple[str, ...]
    :param on_conflict: ``ignore`` or ``update``
    :type on_conflict: str
    :param conflict_target: names of the fields of the conflicting unique key
    :type conflict_target: Optional[Sequence[str]]
    :param update_fields: names of the fields updated from the inserted row
    :type update_fields: Optional[Sequence[str]]
    :param on_update: columns updated with their ``on_update`` value
    :type on_update: Tuple[str, ...]
    :param returning: return the primary key of a written row
    :type returning: bool
    :return: insert statement with ``ormar_v{i}`` binds
    :rtype: Executable
    """
    config = model_cls.ormar_config
    aliases = model_cls.get_column_aliases
    target = (
        tuple(c for name in conflict_target for c in aliases(field_name=name))
        if conflict_target
        else config.pk.column_aliases()
    )
    update_names: Tuple[str, ...] = ()
    if on_conflict == "update":
        update_names = tuple(
            sorted(c for name in update_fields for c in aliases(field_name=name))
            if update_fields
            else (
                name
                for name in names
                if name not in {*target, *config.pk.column_aliases(), *on_update}
            )
        )
    return cached_statement(
        model_cls=model_cls,
        key=("insert", names, on_conflict, target, update_names, on_update, returning),
        builder=build_conflict_insert,
        table=config.table,
        names=names,
        primary_key=config.pk,
        dialect_name=config.database.dialect.name,
        on_conflict=on_conflict,
        target=target,
        update_names=update_names,
        on_update=on_update,
        returning=returning,
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
    primary_key = model_cls.ormar_config.pk
    statement = cached_statement(
        model_cls=model_cls,
        key=("update", names),
        builder=build_update,
        table=table,
        names=names,
        primary_key=primary_key,
    )
    params = {
        **value_params(names=names, values=values),
        **primary_key.bind_params(value=pk),
    }
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
    primary_key = model_cls.ormar_config.pk
    statement = cached_statement(
        model_cls=model_cls,
        key=("delete",),
        builder=build_delete,
        table=table,
        primary_key=primary_key,
    )
    params = primary_key.bind_params(value=pk)
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
    primary_key = model_cls.ormar_config.pk
    statement = cached_statement(
        model_cls=model_cls,
        key=("select_pk",),
        builder=build_pk_select,
        table=table,
        primary_key=primary_key,
    )
    params = primary_key.bind_params(value=pk)
    return StatementTemplate(statement=statement, params=params)
