import string
import uuid
from functools import lru_cache
from random import choices
from typing import TYPE_CHECKING, Any, Optional, Union

import sqlalchemy
from sqlalchemy import Label
from sqlalchemy.sql.selectable import NamedFromClause

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model
    from ormar.fields import ForeignKeyField
    from ormar.models import ModelRow


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


def get_table_alias() -> str:
    """
    Creates a random string that is used to alias tables in joins.
    It's necessary that each relation has it's own aliases cause you can link
    to the same target tables from multiple fields on one model as well as from
    multiple different models in one join.

    :return: randomly generated alias
    :rtype: str
    """
    alias = "".join(choices(string.ascii_uppercase, k=2)) + uuid.uuid4().hex[:4]
    return alias.lower()


class AliasManager:
    """
    Keep all aliases of relations between different tables.
    One global instance is shared between all models.
    """

    def __init__(self) -> None:
        self._aliases_new: dict[str, str] = dict()
        self._reversed_aliases: dict[str, str] = dict()
        self._prefixed_tables: dict[str, NamedFromClause] = dict()

    def __contains__(self, item: str) -> bool:
        return self._aliases_new.__contains__(item)

    def __getitem__(self, key: str) -> Any:
        return self._aliases_new.__getitem__(key)

    @property
    def reversed_aliases(self) -> dict:
        """
        Returns swapped key-value pairs from aliases where alias is the key.

        :return: dictionary of prefix to relation
        :rtype: dict
        """
        if self._reversed_aliases:
            return self._reversed_aliases
        reversed_aliases = {v: k for k, v in self._aliases_new.items()}
        self._reversed_aliases = reversed_aliases
        return self._reversed_aliases

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

    def add_relation_type(
        self,
        source_model: type["Model"],
        relation_name: str,
        reverse_name: Optional[str] = None,
    ) -> None:
        """
        Registers the relations defined in ormar models.
        Given the relation it registers also the reverse side of this relation.

        Used by both ForeignKey and ManyToMany relations.

        Each relation is registered as Model name and relation name.
        Each alias registered has to be unique.

        Aliases are used to construct joins to assure proper links between tables.
        That way you can link to the same target tables from multiple fields
        on one model as well as from multiple different models in one join.

        :param source_model: model with relation defined
        :type source_model: source Model
        :param relation_name: name of the relation to define
        :type relation_name: str
        :param reverse_name: name of related_name fo given relation for m2m relations
        :type reverse_name: Optional[str]
        :return: none
        :rtype: None
        """
        parent_key = f"{source_model.get_name()}_{relation_name}"
        if parent_key not in self._aliases_new:
            self.add_alias(parent_key)

        to_field = source_model.ormar_config.model_fields[relation_name]
        child_model = to_field.to
        child_key = f"{child_model.get_name()}_{reverse_name}"
        if child_key not in self._aliases_new:
            self.add_alias(child_key)

    def add_alias(self, alias_key: str) -> str:
        """
        Adds alias to the dictionary of aliases under given key.

        :param alias_key: key of relation to generate alias for
        :type alias_key: str
        :return: generated alias
        :rtype: str
        """
        alias = get_table_alias()
        self._aliases_new[alias_key] = alias
        return alias

    def resolve_relation_alias(
        self, from_model: Union[type["Model"], type["ModelRow"]], relation_name: str
    ) -> str:
        """
        Given model and relation name returns the alias for this relation.

        :param from_model: model with relation defined
        :type from_model: source Model
        :param relation_name: name of the relation field
        :type relation_name: str
        :return: alias of the relation
        :rtype: str
        """
        alias = self._aliases_new.get(f"{from_model.get_name()}_{relation_name}", "")
        return alias

    def resolve_relation_alias_after_complex(
        self,
        source_model: Union[type["Model"], type["ModelRow"]],
        relation_str: str,
        relation_field: "ForeignKeyField",
    ) -> str:
        """
        Given source model and relation string returns the alias for this complex
        relation if it exists, otherwise fallback to normal relation from a relation
        field definition.

        :param relation_field: field with direct relation definition
        :type relation_field: "ForeignKeyField"
        :param source_model: model with query starts
        :type source_model: source Model
        :param relation_str: string with relation joins defined
        :type relation_str: str
        :return: alias of the relation
        :rtype: str
        """
        alias = ""
        if relation_str and "__" in relation_str:
            alias = self.resolve_relation_alias(
                from_model=source_model, relation_name=relation_str
            )
        if not alias:
            alias = self.resolve_relation_alias(
                from_model=relation_field.get_source_model(),
                relation_name=relation_field.get_relation_name(),
            )
        return alias
