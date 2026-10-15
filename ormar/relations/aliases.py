import hashlib
from functools import lru_cache
from typing import Any, Optional

import sqlalchemy
from sqlalchemy import Label
from sqlalchemy.sql.selectable import NamedFromClause


@lru_cache(maxsize=2048)
def build_prefixed_columns(
    alias: str, table: sqlalchemy.Table, fields: Optional[frozenset[str]]
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


@lru_cache(maxsize=4096)
def alias_for(relation_path: str) -> str:
    """
    Returns the table alias for a relation path.

    The path is the dunder separated chain of relation names from the root model
    of a single sql statement, so the alias is unique within that statement and
    stable between queries and processes. Through tables of many to many
    relations use the relation path followed by the through model name.

    :param relation_path: dunder separated relation path, empty for the root model
    :type relation_path: str
    :return: alias of the table joined under that path, empty for the root model
    :rtype: str
    """
    if not relation_path:
        return ""
    return "t" + hashlib.blake2s(relation_path.encode(), digest_size=3).hexdigest()


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
        build_prefixed_columns(alias, table, frozenset(fields) if fields else None)
    )


@lru_cache(maxsize=None)
def prefixed_table(alias: str, table: sqlalchemy.Table) -> NamedFromClause:
    """
    Returns the table aliased under its table alias.

    Cached without a size limit, as filters and joins of one statement have to
    reference the same aliased table object, and the number of aliases is bounded
    by the relation paths of the defined models.

    :param alias: alias of given table
    :type alias: str
    :param table: table to alias
    :type table: sqlalchemy.Table
    :return: aliased table
    :rtype: NamedFromClause
    """
    return table.alias(f"{alias}_{table.name}")
