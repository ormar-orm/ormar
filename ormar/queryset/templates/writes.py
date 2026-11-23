"""
Insert templates of a model: the single-row insert, the statements of
``bulk_create`` and upserts. Updates, deletes and selects by primary key are
built by ``write_statements``.

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
from sqlalchemy.sql.sqltypes import TupleType

from ormar.databases.dialects import update_assignment
from ormar.queryset.actions.select_action import expression_columns
from ormar.queryset.expressions import (
    Expression,
    expression_clause,
    expression_key,
    expression_params,
)
from ormar.queryset.templates.cache import (
    StatementTemplate,
    cached_statement,
    template_cache_for,
)
from ormar.queryset.templates.reads import release_values
from ormar.queryset.templates.slots import value_binds, value_params

if TYPE_CHECKING:  # pragma: no cover
    from ormar.databases.dialects import DialectRules


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
                reference_type=table.c[name].type,
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


def update_value_binds(
    model: Type[Any], update_values: Mapping[str, Any]
) -> Tuple[BindParameter, ...]:
    """
    :param model: written model
    :type model: Type[Any]
    :param update_values: value or expression node by column name
    :type update_values: Mapping[str, Any]
    :return: bind parameters of the ``update_values`` clauses, in build order
    :rtype: Tuple[BindParameter, ...]
    """
    table = model.ormar_config.table
    clauses = update_value_clauses(
        inserted=table.alias("inserted").c, model_cls=model, update_values=update_values
    )
    return tuple(
        element
        for clause in clauses.values()
        for element in visitors.iterate(clause)
        if isinstance(element, BindParameter)
    )


def build_update_assignment(
    model: Type[Any],
    on_update: Tuple[str, ...],
    update_values: Mapping[str, Any],
) -> Executable:
    """
    Build the MySQL ``SET`` of the user variables a multi-row upsert reads its
    ``on_update`` values and the bound ``update_values`` from (see
    ``update_assignment``). An ``in`` list sets one variable per item, so the
    statement is cached per length of the lists (see ``update_list_keys``).

    :param model: written model
    :type model: Type[Any]
    :param on_update: columns updated with their ``on_update`` value
    :type on_update: Tuple[str, ...]
    :param update_values: value or expression node by column name
    :type update_values: Mapping[str, Any]
    :return: assignment statement
    :rtype: Executable
    """
    return update_assignment(
        table=model.ormar_config.table,
        on_update=on_update,
        value_binds=update_value_binds(model=model, update_values=update_values),
    )


def update_list_keys(
    model: Type[Any], update_values: Mapping[str, Any]
) -> Tuple[Tuple[str, int], ...]:
    """
    Return the ``in`` lists bound by the ``When`` conditions of
    ``update_values``: the name of each and the number of parts of its key
    tuples, 0 for plain values, cached per shape of ``update_values``::

        update_list_keys(
            model=Player,
            update_values={"rank": Case(When(score__in=[1, 8], then=1))},
        )
        # -> (("ormar_u0_w0_0", 0),)

    A multi-row MySQL upsert reads each item from its own user variable, so
    its statements are cached per length of these lists (see
    ``list_variable_params``).

    :param model: written model
    :type model: Type[Any]
    :param update_values: value or expression node by column name
    :type update_values: Mapping[str, Any]
    :return: name and number of key parts per ``in`` list
    :rtype: Tuple[Tuple[str, int], ...]
    """
    cache = template_cache_for(model_cls=model)
    key = ("update_list_keys", update_values_key(update_values=update_values))
    list_keys = cache.get(key)
    if list_keys is None:
        list_keys = tuple(
            (
                bind.key,
                len(bind.type.types) if isinstance(bind.type, TupleType) else 0,
            )
            for bind in update_value_binds(model=model, update_values=update_values)
            if bind.expanding
        )
        cache.put(key=key, statement=list_keys)
    return list_keys


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


def conflict_target_columns(
    model_cls: Type[Any], conflict_target: Optional[Sequence[str]]
) -> Tuple[str, ...]:
    """
    Return the columns of the unique key an upsert conflicts on: the columns
    of the ``conflict_target`` fields, or of the primary key without one::

        conflict_target_columns(model_cls=Tag, conflict_target=["name"])
        # -> ("name",)

    :param model_cls: inserted model
    :type model_cls: Type[Any]
    :param conflict_target: names of the fields of the conflicting unique key
    :type conflict_target: Optional[Sequence[str]]
    :return: column names
    :rtype: Tuple[str, ...]
    """
    return (
        tuple(
            column
            for field_name in conflict_target or ()
            for column in model_cls.get_column_aliases(field_name=field_name)
        )
        or model_cls.ormar_config.pk.column_aliases()
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
    list_lengths: Tuple[int, ...] = (),
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
    :param list_lengths: lengths of the ``in`` lists of ``update_values``
        read from user variables, one variable per item (see
        ``update_list_keys``)
    :type list_lengths: Tuple[int, ...]
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
    target = conflict_target_columns(
        model_cls=model_cls, conflict_target=conflict_target
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
            list_lengths,
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
