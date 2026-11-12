"""
Write templates of a model: single-row insert, update, delete and the select
by primary key, plus the statements of ``bulk_create`` and ``bulk_update``.

Each runs one cached statement per model and set of written columns::

    insert_template(model_cls=Author, values={"name": "a", "score": 1})
    # -> INSERT INTO authors (name, score) VALUES (:ormar_v0, :ormar_v1)
    #    {"ormar_v0": "a", "ormar_v1": 1}
"""

import functools
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
)

import sqlalchemy
from sqlalchemy.sql import Executable, visitors
from sqlalchemy.sql.elements import BindParameter

from ormar.databases.dialects import update_assignment
from ormar.queryset.actions.select_action import expression_columns
from ormar.queryset.expressions import (
    Expression,
    expression_clause,
    expression_key,
    expression_params,
)
from ormar.queryset.templates.cache import StatementTemplate, cached_statement
from ormar.queryset.templates.reads import release_values
from ormar.queryset.templates.slots import value_binds, value_params

if TYPE_CHECKING:  # pragma: no cover
    from ormar.databases.dialects import DialectRules
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


def update_value_clauses(
    inserted: Any, model_cls: Type[Any], update_values: Mapping[str, Any]
) -> Dict[str, Any]:
    """
    Build the SQL of the ``update_values`` of an upsert, in sorted column
    order: a value binds slot ``ormar_u{i}`` typed like its column, an
    expression reads ``F`` fields from the written table (the existing row) and
    ``Inserted`` fields from ``inserted``, with its literals bound as
    ``ormar_u{i}_{k}``::

        update_value_clauses(
            inserted=insert.excluded,
            model_cls=Track,
            update_values={"completed": F("completed") + 1, "note": "x"},
        )
        # -> {"completed": tracks.completed + :ormar_u0_0, "note": :ormar_u1}

    :param inserted: the inserted row (``excluded`` or MySQL ``inserted``)
    :type inserted: Any
    :param model_cls: written model
    :type model_cls: Type[Any]
    :param update_values: value or expression node by column name
    :type update_values: Mapping[str, Any]
    :return: SQL by column name
    :rtype: Dict[str, Any]
    """
    table = model_cls.ormar_config.table
    clauses: Dict[str, Any] = {}
    for index, name in enumerate(sorted(update_values)):
        value = update_values[name]
        clauses[name] = (
            expression_clause(
                node=value,
                columns=expression_columns(node=value, model=model_cls, upsert=True),
                slot=f"ormar_u{index}",
                inserted=inserted,
            )
            if isinstance(value, Expression)
            else sqlalchemy.bindparam(key=f"ormar_u{index}", type_=table.c[name].type)
        )
    return clauses


def update_values_params(update_values: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Return the values bound by the ``update_values`` of an upsert, named like
    ``update_value_clauses`` names them::

        update_values_params(update_values={"completed": F("completed") + 1})
        # -> {"ormar_u0_0": 1}

    :param update_values: value or expression node by column name
    :type update_values: Mapping[str, Any]
    :return: values by bind parameter name
    :rtype: Dict[str, Any]
    """
    params: Dict[str, Any] = {}
    for index, name in enumerate(sorted(update_values)):
        params.update(
            expression_params(value=update_values[name], slot=f"ormar_u{index}")
        )
    return params


def update_values_key(update_values: Mapping[str, Any]) -> Tuple[Any, ...]:
    """
    Return the shape of the ``update_values`` of an upsert for cache keys: the
    column of a value, the column and ``expression_key`` of an expression, so
    the literals do not split the cache.

    :param update_values: value or expression node by column name
    :type update_values: Mapping[str, Any]
    :return: hashable shape
    :rtype: Tuple[Any, ...]
    """
    return tuple(
        (name, expression_key(node=update_values[name]))
        if isinstance(update_values[name], Expression)
        else name
        for name in sorted(update_values)
    )


def build_update_assignment(
    model: Type[Any],
    on_update: Tuple[str, ...],
    update_values: Mapping[str, Any],
) -> Executable:
    """
    Build the MySQL ``SET`` of the user variables a multi-row upsert reads its
    ``on_update`` values and the bound ``update_values`` from (see
    ``update_assignment``).

    :param model: written model
    :type model: Type[Any]
    :param on_update: columns updated with their ``on_update`` value
    :type on_update: Tuple[str, ...]
    :param update_values: value or expression node by column name
    :type update_values: Mapping[str, Any]
    :return: assignment statement
    :rtype: Executable
    """
    table = model.ormar_config.table
    clauses = update_value_clauses(
        inserted=table.c, model_cls=model, update_values=update_values
    )
    return update_assignment(
        table=table,
        on_update=on_update,
        value_binds=tuple(
            element
            for clause in clauses.values()
            for element in visitors.iterate(clause)
            if isinstance(element, BindParameter)
        ),
    )


def build_conflict_insert(
    table: sqlalchemy.Table,
    names: Tuple[str, ...],
    rules: "DialectRules",
    on_conflict: str,
    target: Tuple[str, ...],
    update_names: Tuple[str, ...],
    on_update: Tuple[str, ...],
    returning: Tuple[str, ...],
    model: Optional[Type[Any]] = None,
    update_values: Optional[Mapping[str, Any]] = None,
    update_variables: bool = False,
) -> Executable:
    """
    Build the insert of the database's dialect that skips or updates a row
    conflicting with an existing one. The value binds are built here, on a
    cache miss only, so the rules get them ready made::

        INSERT INTO tags (name, score) VALUES (:ormar_v0, :ormar_v1)
        ON CONFLICT (name) DO UPDATE SET score = excluded.score
        -- MySQL: ON DUPLICATE KEY UPDATE score = VALUES(score)

    :param table: written table
    :type table: sqlalchemy.Table
    :param names: inserted column names, sorted
    :type names: Tuple[str, ...]
    :param rules: rules of the database dialect
    :type rules: DialectRules
    :param on_conflict: ``ignore`` or ``update``
    :type on_conflict: str
    :param target: conflicting column names (the primary key for ``ignore``)
    :type target: Tuple[str, ...]
    :param update_names: columns updated from the inserted row
    :type update_names: Tuple[str, ...]
    :param on_update: columns updated with their ``on_update`` value
    :type on_update: Tuple[str, ...]
    :param returning: columns of the written row to return, if any
    :type returning: Tuple[str, ...]
    :param model: written model, resolving the fields of ``update_values``
    :type model: Optional[Type[Any]]
    :param update_values: value or expression node set by column name
    :type update_values: Optional[Mapping[str, Any]]
    :param update_variables: read the bound update values from MySQL user
        variables
    :type update_variables: bool
    :return: insert statement
    :rtype: Executable
    """
    statement = rules.conflict_insert(
        table=table,
        values=value_binds(table=table, names=names),
        on_conflict=on_conflict,
        target=target,
        update_names=update_names,
        on_update=on_update,
        returning=returning,
        value_updates=(
            functools.partial(
                update_value_clauses, model_cls=model, update_values=update_values
            )
            if model is not None and update_values
            else None
        ),
        update_variables=update_variables,
    )
    release_values(statement=statement)
    return statement


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


def insert_statement(
    model_cls: Type[Any],
    names: Tuple[str, ...],
    on_conflict: Optional[str] = None,
    conflict_target: Optional[Sequence[str]] = None,
    update_fields: Optional[Sequence[str]] = None,
    on_update: Tuple[str, ...] = (),
    returning: Tuple[str, ...] = (),
    update_values: Optional[Mapping[str, Any]] = None,
    update_variables: bool = False,
) -> Executable:
    """
    Return the insert of a model for the given sorted column names, from the
    model's template cache. ``save()`` binds one row to it and
    ``bulk_create`` runs it with ``executemany`` for every row with the same
    columns.

    With ``on_conflict`` the insert skips or updates conflicting rows (see
    ``build_conflict_insert``): the target defaults to the primary key and the
    updated columns to the inserted ones but the target, the key and
    ``on_update``, or to none with ``update_values``. One statement is cached
    per shape of ``update_values`` (see ``update_values_key``).

    :param model_cls: inserted model
    :type model_cls: Type[Any]
    :param names: inserted column names, sorted
    :type names: Tuple[str, ...]
    :param on_conflict: ``ignore`` or ``update`` conflicting rows
    :type on_conflict: Optional[str]
    :param conflict_target: names of the fields of the conflicting unique key
    :type conflict_target: Optional[Sequence[str]]
    :param update_fields: names of the fields updated from the inserted row
    :type update_fields: Optional[Sequence[str]]
    :param on_update: columns updated with their ``on_update`` value
    :type on_update: Tuple[str, ...]
    :param returning: columns of the written row to return, if any
    :type returning: Tuple[str, ...]
    :param update_values: value or expression node set by column name
    :type update_values: Optional[Mapping[str, Any]]
    :param update_variables: read the bound update values from MySQL user
        variables set by ``update_assignment`` instead of binds
    :type update_variables: bool
    :return: insert statement with ``ormar_v{i}`` binds
    :rtype: Executable
    """
    config = model_cls.ormar_config
    if not on_conflict:
        return cached_statement(
            model_cls=model_cls,
            key=("insert", names),
            builder=build_insert,
            table=config.table,
            names=names,
        )
    pk = config.pk.column_aliases()
    target = (
        tuple(
            column
            for field_name in conflict_target or ()
            for column in model_cls.get_column_aliases(field_name=field_name)
        )
        or pk
    )
    update_names = tuple(
        sorted(
            column
            for field_name in update_fields or ()
            for column in model_cls.get_column_aliases(field_name=field_name)
        )
    )
    if not update_names and not update_values:
        update_names = tuple(
            column for column in names if column not in {*target, *pk, *on_update}
        )
    if (
        on_conflict == "update"
        and not update_names
        and not on_update
        and not update_values
    ):
        # nothing left to update: set a target column to itself, so the
        # conflicting row is still returned (DO NOTHING would return no row)
        update_names = target[:1]
    rules = config.database.rules
    conflict = dict(
        on_conflict=on_conflict,
        target=target,
        update_names=update_names,
        on_update=on_update,
        returning=returning,
        update_variables=update_variables,
    )
    return cached_statement(
        model_cls=model_cls,
        key=(
            "insert",
            names,
            rules.backend_name,
            *conflict.values(),
            update_values_key(update_values=update_values or {}),
        ),
        builder=build_conflict_insert,
        table=config.table,
        names=names,
        rules=rules,
        model=model_cls,
        update_values=update_values,
        **conflict,
    )


def insert_template(
    model_cls: Type[Any], values: Mapping[str, Any], **statement_kwargs: Any
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
    :param statement_kwargs: conflict handling (see ``insert_statement``)
    :type statement_kwargs: Any
    :return: statement and values to bind
    :rtype: StatementTemplate
    """
    names = tuple(sorted(values))
    statement = insert_statement(model_cls=model_cls, names=names, **statement_kwargs)
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
