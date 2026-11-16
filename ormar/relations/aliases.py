import hashlib
from functools import lru_cache
from typing import Any, Optional

import sqlalchemy
from sqlalchemy import Label
from sqlalchemy.sql.selectable import NamedFromClause


@lru_cache(maxsize=2048)
def build_prefixed_columns(
    *, alias: str, table: sqlalchemy.Table, fields: Optional[frozenset[str]]
) -> tuple[Label[Any], ...]:
    """
    Builds labelled columns of a table, cached as labels are immutable and the
    same model/alias/fields combination is selected over and over.

    :param alias: alias of given table
    :type alias: str
    :param table: table from which fields should be aliased
    :type table: sqlalchemy.Table
    :param fields: column names (ormar aliases) to include, None for all
    :type fields: Optional[frozenset[str]]
    :return: tuple of labelled columns
    :rtype: tuple[Label[Any], ...]
    """
    alias = f"{alias}_" if alias else ""
    aliased_fields = {f"{alias}{x}" for x in fields} if fields else set()
    all_columns = (
        table.columns
        if not fields
        else [
            col
            for col in table.columns
            if col.name in fields or col.name in aliased_fields
        ]
    )
    return tuple(column.label(f"{alias}{column.name}") for column in all_columns)


# bytes of the blake2s digest of a relation path in its alias: 10 hex digits,
# so two paths of one statement practically never share a hash (the salted
# fallback below still handles it); the alias prefixes table and column names,
# which PostgreSQL truncates to 63 bytes consistently and MySQL allows up to 256
ALIAS_DIGEST_SIZE = 5
# relation path that owns each alias handed out in this process; a path whose
# hash is owned by another path gets the next free salted hash instead
ALIAS_OWNERS: dict[str, str] = {}


def path_hash(relation_path: str, attempt: int) -> str:
    """
    Returns a candidate alias of a relation path, the plain hash of the path on
    the first attempt and a hash salted with the attempt number afterwards.

    :param relation_path: dunder separated relation path
    :type relation_path: str
    :param attempt: number of the candidate, 0 for the plain hash
    :type attempt: int
    :return: candidate alias
    :rtype: str
    """
    key = relation_path if not attempt else f"{relation_path}#{attempt}"
    return (
        "t" + hashlib.blake2s(key.encode(), digest_size=ALIAS_DIGEST_SIZE).hexdigest()
    )


@lru_cache(maxsize=4096)
def alias_for(relation_path: str) -> str:
    """
    Returns the table alias for a relation path.

    The path is the dunder separated chain of relation names from the root model
    of a single sql statement. The alias is a short hash of the path, stable
    between queries and processes. Two paths with the same hash would share an
    alias in one statement, so each alias is owned by the first path that gets
    it and a colliding path gets a salted hash (an alias that is unique in the
    process, but depends on which path came first). Through tables of many to
    many relations use the relation path followed by the through model name.

    :param relation_path: dunder separated relation path, empty for the root model
    :type relation_path: str
    :return: alias of the table joined under that path, empty for the root model
    :rtype: str
    """
    if not relation_path:
        return ""
    attempt = 0
    alias = path_hash(relation_path=relation_path, attempt=attempt)
    while ALIAS_OWNERS.setdefault(alias, relation_path) != relation_path:
        attempt += 1
        alias = path_hash(relation_path=relation_path, attempt=attempt)
    return alias


def prefixed_columns(
    alias: str, table: sqlalchemy.Table, fields: Optional[list] = None
) -> list[Label[Any]]:
    """
    Creates a list of labelled columns of a table prefixed with the table alias.

    Optional list of fields to include can be passed to extract only those columns.
    List has to have sqlalchemy names of columns (ormar aliases) not the ormar ones.

    :param alias: alias of given table
    :type alias: str
    :param table: table from which fields should be aliased
    :type table: sqlalchemy.Table
    :param fields: fields to include
    :type fields: Optional[list[str]]
    :return: list of labelled columns
    :rtype: list[Label[Any]]
    """
    return list(
        build_prefixed_columns(
            alias=alias, table=table, fields=frozenset(fields) if fields else None
        )
    )


@lru_cache(maxsize=None)
def prefixed_table(*, alias: str, table: sqlalchemy.Table) -> NamedFromClause:
    """
    Returns the table aliased under its table alias.

    Cached without a size limit, as filters and joins of one statement have to
    reference the same aliased table object, and the number of aliases is bounded
    by the relation paths of the defined models.
    The parameters are keyword-only, as the cache keys keyword and positional
    calls differently, and two aliased objects of one table break the query.

    :param alias: alias of given table
    :type alias: str
    :param table: table to alias
    :type table: sqlalchemy.Table
    :return: aliased table
    :rtype: NamedFromClause
    """
    return table.alias(f"{alias}_{table.name}")
