import hashlib
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Optional

import sqlalchemy
from sqlalchemy import Label
from sqlalchemy.sql.selectable import NamedFromClause

if TYPE_CHECKING:  # pragma: no cover
    pass


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


class AliasManager:
    """
    Builds aliased tables and columns for joins.
    One instance is shared between all models.
    """

    def __init__(self) -> None:
        self._prefixed_tables: dict[str, NamedFromClause] = dict()

    @staticmethod
    def prefixed_columns(
        alias: str, table: sqlalchemy.Table, fields: Optional[list] = None
    ) -> list[Label[Any]]:
        """
        Creates a list of aliases sqlalchemy text clauses from
        string alias and sqlalchemy.Table.

        Optional list of fields to include can be passed to extract only those columns.
        List has to have sqlalchemy names of columns (ormar aliases) not the ormar ones.

        :param alias: alias of given table
        :type alias: str
        :param table: table from which fields should be aliased
        :type table: sqlalchemy.Table
        :param fields: fields to include
        :type fields: Optional[list[str]]
        :return: list of sqlalchemy text clauses with "column name as aliased name"
        :rtype: list[text]
        """
        return list(
            build_prefixed_columns(alias, table, frozenset(fields) if fields else None)
        )

    def prefixed_table_name(
        self, alias: str, table: sqlalchemy.Table
    ) -> NamedFromClause:
        """
        Creates text clause with table name with aliased name.

        :param alias: alias of given table
        :type alias: str
        :param table: table
        :type table: sqlalchemy.Table
        :return: sqlalchemy text clause as "table_name aliased_name"
        :rtype: sqlalchemy text clause
        """
        full_alias = f"{alias}_{table.name}"
        key = f"{full_alias}_{id(table)}"
        return self._prefixed_tables.setdefault(key, table.alias(full_alias))
