"""
Dialect rules: the SQL behaviour that differs between database backends.

One rules object per ``DatabaseConnection``, picked by the backend name of its
url, so it is known before ``connect()``::

    database.rules.conflict_insert(table=tags, values=binds, ...)
    # PostgreSQL / SQLite: INSERT ... ON CONFLICT (name) DO UPDATE ...
    # MySQL / MariaDB:     INSERT ... ON DUPLICATE KEY UPDATE ...

Rules only build statements and answer questions about the dialect. Running
statements and reading results stays with the driver adapters.
"""

import sqlite3
from typing import (
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

import sqlalchemy
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import Executable, operators
from sqlalchemy.sql.elements import BinaryExpression, BindParameter
from sqlalchemy.sql.sqltypes import TupleType
from sqlalchemy.sql.visitors import replacement_traverse

from ormar.exceptions import QueryDefinitionError

# MySQL server error ER_DUP_ENTRY: an insert hit an existing unique key
MYSQL_DUPLICATE_ENTRY_ERROR = 1062


def conflict_updates(
    *,
    table: sqlalchemy.Table,
    inserted: Any,
    update_names: Tuple[str, ...],
    on_update: Tuple[str, ...],
    value_updates: Optional[Callable[[Any], Dict[str, Any]]] = None,
    update_variables: bool = False,
) -> Dict[str, Any]:
    """
    Return the columns a conflicting row is updated with: ``update_names`` from
    the inserted row, ``on_update`` columns from ``ormar_on_update_<column>``
    parameters and the ``update_values`` columns ``value_updates`` builds for
    the inserted row (see ``update_value_clauses``)::

        score = excluded.score
        completed = tracks.completed + :ormar_u0_0

    With ``update_variables`` every bound value is read from a MySQL user
    variable instead (see ``update_assignment``), an ``in`` list from one
    variable per item (see ``bind_variables``).

    :param table: written table
    :type table: sqlalchemy.Table
    :param inserted: the inserted row (``excluded`` or MySQL ``inserted``)
    :type inserted: Any
    :param update_names: columns updated from the inserted row
    :type update_names: Tuple[str, ...]
    :param on_update: columns updated with their ``on_update`` value
    :type on_update: Tuple[str, ...]
    :param value_updates: builds the ``update_values`` columns from the
        inserted row
    :type value_updates: Optional[Callable[[Any], Dict[str, Any]]]
    :param update_variables: read the bound values from user variables
    :type update_variables: bool
    :return: value per updated column
    :rtype: Dict[str, Any]
    """
    updates: Dict[str, Any] = {name: inserted[name] for name in update_names}
    for name in on_update:
        updates[name] = sqlalchemy.bindparam(
            key=f"ormar_on_update_{name}", type_=table.c[name].type
        )
    if value_updates is not None:
        updates.update(value_updates(inserted))
    if not update_variables:
        return updates
    variables = {
        f"ormar_on_update_{name}": on_update_variable(index=index)
        for index, name in enumerate(on_update)
    }

    def read_variable(element: Any, **kwargs: Any) -> Any:
        """
        :param element: element of an update clause
        :type element: Any
        :param kwargs: traversal options, unused
        :type kwargs: Any
        :return: the user variable of a bind parameter, the variables of an
            ``in`` list, the constant of an empty one, None keeps the element
        :rtype: Any
        """
        if (
            isinstance(element, BinaryExpression)
            and isinstance(element.right, BindParameter)
            and element.right.expanding
            and not element.right.value
        ):
            # nothing is in an empty list, as SQLAlchemy renders it: in is
            # false and not in true, also for NULL
            return (
                sqlalchemy.false()
                if element.operator is operators.in_op
                else sqlalchemy.true()
            )
        if isinstance(element, BindParameter) and element.expanding:
            return read_list_variables(bind=element)
        if isinstance(element, BindParameter):
            return sqlalchemy.literal_column(
                variables.get(element.key, f"@{element.key}"), type_=element.type
            )
        return None

    return {
        name: replacement_traverse(value, {}, read_variable)
        for name, value in updates.items()
    }


def list_item_variables(
    key: str, items: Iterable[Any], list_type: Any
) -> Iterator[Tuple[str, Any, Any]]:
    """
    Yield the MySQL user variables holding the items of an ``in`` list, with
    their types and values: one per item named ``@<key>_<item>``, for a list
    of key tuples (``list_type`` a ``TupleType``) one per part named
    ``@<key>_<item>_<part>``::

        list_item_variables(key="ormar_u0_w0_0", items=[1, 8], list_type=Integer())
        # -> ("@ormar_u0_w0_0_0", Integer(), 1), ("@ormar_u0_w0_0_1", Integer(), 8)

    :param key: name of the list's bind parameter
    :type key: str
    :param items: items of the list
    :type items: Iterable[Any]
    :param list_type: type of the list's bind parameter
    :type list_type: Any
    :return: variable name, type and value per held value
    :rtype: Iterator[Tuple[str, Any, Any]]
    """
    for item, value in enumerate(items):
        if isinstance(list_type, TupleType):
            for part, part_type in enumerate(list_type.types):
                yield f"@{key}_{item}_{part}", part_type, value[part]
        else:
            yield f"@{key}_{item}", list_type, value


def bind_variables(bind: BindParameter) -> List[Tuple[str, Any]]:
    """
    Return the MySQL user variables holding the value of a bind parameter and
    their types: ``@<key>``, for an ``in`` list one per item (see
    ``list_item_variables``). The values are set by ``update_assignment``
    (see ``list_variable_params``).

    :param bind: bind parameter
    :type bind: BindParameter
    :return: variable name and type per held value
    :rtype: List[Tuple[str, Any]]
    """
    if not bind.expanding:
        return [(f"@{bind.key}", bind.type)]
    return [
        (name, variable_type)
        for name, variable_type, _ in list_item_variables(
            key=bind.key, items=bind.value or (), list_type=bind.type
        )
    ]


def read_list_variables(bind: BindParameter) -> Any:
    """
    Return an ``in`` list read from the user variables of its items (see
    ``bind_variables``)::

        score IN (@ormar_u0_w0_0_0, @ormar_u0_w0_0_1)
        (tenant, number) IN ((@ormar_u0_w0_0_0_0, @ormar_u0_w0_0_0_1))

    :param bind: expanding bind parameter of a non-empty ``in`` list
    :type bind: BindParameter
    :return: tuple of the variables, of a tuple of them per item for key tuples
    :rtype: Any
    """
    columns: List[Any] = [
        sqlalchemy.literal_column(name, type_=variable_type)
        for name, variable_type in bind_variables(bind=bind)
    ]
    if isinstance(bind.type, TupleType):
        width = len(bind.type.types)
        columns = [
            sqlalchemy.tuple_(*columns[start : start + width])
            for start in range(0, len(columns), width)
        ]
    return sqlalchemy.tuple_(*columns)


def list_variable_params(
    params: Dict[str, Any], list_keys: Tuple[Tuple[str, Any], ...]
) -> Dict[str, Any]:
    """
    Return the values of ``update_assignment``: each ``in`` list replaced by
    its items, named like their variables (see ``list_item_variables``)::

        list_variable_params(
            params={"ormar_u0_0": 5, "ormar_u0_w0_0": [1, 8]},
            list_keys=(("ormar_u0_w0_0", Integer()),),
        )
        # -> {"ormar_u0_0": 5, "ormar_u0_w0_0_0": 1, "ormar_u0_w0_0_1": 8}

    :param params: bound values by name
    :type params: Dict[str, Any]
    :param list_keys: name and bind type of each ``in`` list
    :type list_keys: Tuple[Tuple[str, Any], ...]
    :return: values by variable name
    :rtype: Dict[str, Any]
    """
    variable_params = dict(params)
    for key, list_type in list_keys:
        for name, _, value in list_item_variables(
            key=key, items=variable_params.pop(key), list_type=list_type
        ):
            variable_params[name[1:]] = value
    return variable_params


def on_update_variable(index: int) -> str:
    """
    Return the MySQL user variable holding the ``on_update`` value of the column
    at ``index`` of the sorted ``on_update`` columns; named by position, as
    variable names are limited to 64 characters::

        on_update_variable(index=0)  # -> @ormar_on_update_0

    The values of ``update_values`` are held in variables named like their
    bind parameters (``@ormar_u0``, ``@ormar_u1_0``).

    :param index: position of the column in ``on_update``
    :type index: int
    :return: variable name
    :rtype: str
    """
    return f"@ormar_on_update_{index}"


def update_assignment(
    table: sqlalchemy.Table,
    on_update: Tuple[str, ...],
    value_binds: Tuple[BindParameter, ...] = (),
) -> Executable:
    """
    Build the MySQL statement that stores the values a conflicting row is
    updated with in user variables: the ``on_update`` values bound as
    ``ormar_on_update_<index>`` in the order of ``on_update`` and the bound
    values of ``update_values`` under their own names::

        SET @ormar_on_update_0 = %s, @ormar_u0_0 = %s

    An ``in`` list gets one variable per item (see ``bind_variables``),
    an empty one none.

    MySQL drivers turn ``executemany`` of an insert into one multi-row VALUES
    and leave binds after it unfilled, so a multi-row upsert reads the values
    from these variables instead (see ``conflict_updates``).

    :param table: written table
    :type table: sqlalchemy.Table
    :param on_update: columns updated with their ``on_update`` value
    :type on_update: Tuple[str, ...]
    :param value_binds: bind parameters of the ``update_values`` columns
    :type value_binds: Tuple[BindParameter, ...]
    :return: assignment statement
    :rtype: Executable
    """
    binds = [
        sqlalchemy.bindparam(key=f"ormar_on_update_{index}", type_=table.c[name].type)
        for index, name in enumerate(on_update)
    ]
    variables = [on_update_variable(index=index) for index in range(len(on_update))]
    for bind in value_binds:
        for variable, variable_type in bind_variables(bind=bind):
            variables.append(variable)
            binds.append(sqlalchemy.bindparam(key=variable[1:], type_=variable_type))
    assignments = ", ".join(
        f"{variable} = :{bind.key}" for variable, bind in zip(variables, binds)
    )
    return sqlalchemy.text(f"SET {assignments}").bindparams(*binds)


class DialectRules:
    """
    Rules of a backend ormar has no specific rules for, and the base of the
    ones it has. Its conflict insert is the ``ON CONFLICT`` of PostgreSQL and
    SQLite, which only lack the insert construct here.
    """

    #: the engine runs in AUTOCOMMIT and only transactions leave it, because
    #: switching a pooled connection into AUTOCOMMIT and back costs round trips
    autocommit_engine: ClassVar[bool] = False
    #: a skipping conflict insert returns the written primary key
    conflict_returning: ClassVar[bool] = True
    #: ``update_returning()`` runs one ``UPDATE ... RETURNING``; without it
    #: the rows are locked, updated and read back by primary key
    update_returning: ClassVar[bool] = True
    #: ``execute_many`` fills binds placed after the VALUES clause
    execute_many_with_trailing_binds: ClassVar[bool] = True
    #: foreign keys may reference a table in another schema
    cross_schema_foreign_keys: ClassVar[bool] = True
    #: connections run ``PRAGMA foreign_keys=ON`` when opened
    foreign_keys_pragma: ClassVar[bool] = False
    #: transactions send an explicit ``BEGIN`` so a released savepoint does
    #: not commit
    explicit_begin: ClassVar[bool] = False
    #: function that orders rows randomly, used by ``order_by("?")``
    random_order: ClassVar[str] = "random()"
    #: a subquery of an update or delete reads the written table directly;
    #: without it the table is read from a derived table (aggregates)
    subquery_reads_written_table: ClassVar[bool] = True

    def __init__(self, *, backend_name: str) -> None:
        """
        :param backend_name: backend name of the database url
        :type backend_name: str
        """
        self.backend_name = backend_name

    def make_insert(
        self, *, table: sqlalchemy.Table
    ) -> Union[postgresql.Insert, sqlite.Insert]:
        """
        Return the dialect insert construct that takes a conflict clause.

        :raises QueryDefinitionError: the backend has no conflict insert
        :param table: written table
        :type table: sqlalchemy.Table
        :return: insert of the table
        :rtype: Union[postgresql.Insert, sqlite.Insert]
        """
        raise QueryDefinitionError(
            f"on_conflict is not supported on the {self.backend_name} backend"
        )

    def conflict_insert(
        self,
        *,
        table: sqlalchemy.Table,
        values: Dict[Any, Any],
        on_conflict: str,
        target: Tuple[str, ...],
        update_names: Tuple[str, ...],
        on_update: Tuple[str, ...],
        returning: Tuple[str, ...],
        value_updates: Optional[Callable[[Any], Dict[str, Any]]] = None,
        update_variables: bool = False,
    ) -> Executable:
        """
        Build an insert that skips or updates a row conflicting with an
        existing one::

            INSERT INTO tags (name, score) VALUES (:ormar_v0, :ormar_v1)
            ON CONFLICT (name) DO UPDATE SET score = excluded.score

        :param table: written table
        :type table: sqlalchemy.Table
        :param values: bind parameter per inserted column
        :type values: Dict[Any, Any]
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
        :param value_updates: builds the ``update_values`` columns from the
            inserted row
        :type value_updates: Optional[Callable[[Any], Dict[str, Any]]]
        :param update_variables: read the bound update values from MySQL user
            variables (see ``update_assignment``)
        :type update_variables: bool
        :return: insert statement
        :rtype: Executable
        """
        statement = self.make_insert(table=table).values(values)
        if on_conflict == "update":
            statement = statement.on_conflict_do_update(
                index_elements=target,
                set_=conflict_updates(
                    table=table,
                    inserted=statement.excluded,
                    update_names=update_names,
                    on_update=on_update,
                    value_updates=value_updates,
                    update_variables=update_variables,
                ),
            )
        else:
            statement = statement.on_conflict_do_nothing()
        if not returning:
            return statement
        return statement.returning(*(table.c[name] for name in returning))

    def is_duplicate_key(self, *, error: IntegrityError) -> bool:
        """
        Tell whether the error of a plain insert run with ``on_conflict`` means
        the row was skipped. Dialects with ``conflict_returning`` skip in the
        statement and never run that plain insert.

        :param error: error raised by the insert
        :type error: IntegrityError
        :return: the row conflicted with an existing unique key
        :rtype: bool
        """
        return False

    def ordering_aggregate(self, *, field_type: Any, descending: bool) -> str:
        """
        Return the aggregate that orders by a column not in the GROUP BY of a
        limited sub query.

        :param field_type: python type of the ordered field
        :type field_type: Any
        :param descending: the order is descending
        :type descending: bool
        :return: name of the aggregate function
        :rtype: str
        """
        return "max" if descending else "min"

    def nulls_order(
        self, *, full_column: str, ordered: str, nulls_ordering: str
    ) -> str:
        """
        Return an ORDER BY expression that puts nulls first or last::

            authors.name desc nulls last

        :param full_column: fully-qualified, quoted column reference
        :type full_column: str
        :param ordered: the column with its direction
        :type ordered: str
        :param nulls_ordering: ``first`` or ``last``
        :type nulls_ordering: str
        :return: ORDER BY expression as raw SQL text
        :rtype: str
        """
        return f"{ordered} nulls {nulls_ordering}"


class SqliteRules(DialectRules):
    """
    SQLite: ``ON CONFLICT`` inserts, foreign keys switched on per connection,
    an explicit ``BEGIN`` in transactions and no cross-schema foreign keys.
    """

    autocommit_engine = True
    cross_schema_foreign_keys = False
    foreign_keys_pragma = True
    explicit_begin = True
    # RETURNING came with SQLite 3.35, aiosqlite runs the stdlib sqlite3
    update_returning = sqlite3.sqlite_version_info >= (3, 35)

    def make_insert(self, *, table: sqlalchemy.Table) -> sqlite.Insert:
        """
        :param table: written table
        :type table: sqlalchemy.Table
        :return: SQLite insert of the table
        :rtype: sqlite.Insert
        """
        return sqlite.insert(table=table)


class PostgresRules(DialectRules):
    """
    PostgreSQL: ``ON CONFLICT`` inserts and ``bool_or`` to order by booleans,
    which ``min`` and ``max`` do not take.
    """

    def make_insert(self, *, table: sqlalchemy.Table) -> postgresql.Insert:
        """
        :param table: written table
        :type table: sqlalchemy.Table
        :return: PostgreSQL insert of the table
        :rtype: postgresql.Insert
        """
        return postgresql.insert(table=table)

    def ordering_aggregate(self, *, field_type: Any, descending: bool) -> str:
        """
        :param field_type: python type of the ordered field
        :type field_type: Any
        :param descending: the order is descending
        :type descending: bool
        :return: ``bool_or`` for booleans, else ``min`` or ``max``
        :rtype: str
        """
        if field_type is bool:
            return "bool_or"
        return super().ordering_aggregate(field_type=field_type, descending=descending)


class MysqlRules(DialectRules):
    """
    MySQL and MariaDB: ``ON DUPLICATE KEY UPDATE`` inserts and updates without
    RETURNING,
    no ``NULLS FIRST`` / ``NULLS LAST``, ``rand()`` and an AUTOCOMMIT engine.
    """

    autocommit_engine = True
    conflict_returning = False
    update_returning = False
    execute_many_with_trailing_binds = False
    random_order = "rand()"
    subquery_reads_written_table = False

    def conflict_insert(
        self,
        *,
        table: sqlalchemy.Table,
        values: Dict[Any, Any],
        on_conflict: str,
        target: Tuple[str, ...],
        update_names: Tuple[str, ...],
        on_update: Tuple[str, ...],
        returning: Tuple[str, ...],
        value_updates: Optional[Callable[[Any], Dict[str, Any]]] = None,
        update_variables: bool = False,
    ) -> Executable:
        """
        Build an insert that skips or updates a row conflicting with an
        existing one::

            INSERT INTO tags (name, score) VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE score = VALUES(score)

        MySQL reacts to a conflict on any unique key, so the target is not
        emitted and ``ignore`` becomes a no-op update of a key column (a
        duplicate key error would abort the whole ``executemany``). There is no
        RETURNING, so ``returning`` is not used. The other parameters are
        described in ``DialectRules.conflict_insert``.

        :return: insert statement
        :rtype: Executable
        """
        statement = mysql.insert(table=table).values(values)
        if on_conflict == "update":
            return statement.on_duplicate_key_update(
                conflict_updates(
                    table=table,
                    inserted=statement.inserted,
                    update_names=update_names,
                    on_update=on_update,
                    value_updates=value_updates,
                    update_variables=update_variables,
                )
            )
        return statement.on_duplicate_key_update({target[0]: table.c[target[0]]})

    def is_duplicate_key(self, *, error: IntegrityError) -> bool:
        """
        :param error: error raised by the insert
        :type error: IntegrityError
        :return: the server reported ER_DUP_ENTRY
        :rtype: bool
        """
        return error.orig is not None and error.orig.args[:1] == (
            MYSQL_DUPLICATE_ENTRY_ERROR,
        )

    def nulls_order(
        self, *, full_column: str, ordered: str, nulls_ordering: str
    ) -> str:
        """
        Emulate ``NULLS FIRST`` / ``NULLS LAST`` with an ``IS [NOT] NULL`` sort
        key in front, it only changes the order of the rows::

            authors.name is not null, authors.name desc

        :param full_column: fully-qualified, quoted column reference
        :type full_column: str
        :param ordered: the column with its direction
        :type ordered: str
        :param nulls_ordering: ``first`` or ``last``
        :type nulls_ordering: str
        :return: ORDER BY expression as raw SQL text
        :rtype: str
        """
        negation = "not " if nulls_ordering == "first" else ""
        return f"{full_column} is {negation}null, {ordered}"


DIALECT_RULES: Dict[str, Type[DialectRules]] = {
    "sqlite": SqliteRules,
    "postgresql": PostgresRules,
    "mysql": MysqlRules,
    "mariadb": MysqlRules,
}


def rules_for(*, backend_name: str) -> DialectRules:
    """
    Return the rules of a backend; one without its own rules gets the base
    ones, which keep the standard SQL and reject ``on_conflict``.

    :param backend_name: backend name of the database url, e.g. ``mariadb``
    :type backend_name: str
    :return: rules of the backend
    :rtype: DialectRules
    """
    rules_class = DIALECT_RULES.get(backend_name, DialectRules)
    return rules_class(backend_name=backend_name)
