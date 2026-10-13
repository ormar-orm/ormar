from typing import TYPE_CHECKING, cast

from ormar.relations.alias_manager import alias_for

if TYPE_CHECKING:  # pragma: no cover
    from ormar import ForeignKeyField, Model
    from ormar.models.excludable import ExcludableItems


class ReverseAliasResolver:
    """
    Class is used to reverse resolve table aliases into relation strings
    to parse raw data columns and replace table prefixes with full relation string
    """

    def __init__(
        self,
        model_cls: type["Model"],
        excludable: "ExcludableItems",
        select_related: list[str],
        exclude_through: bool = False,
    ) -> None:
        self.select_related = select_related
        self.model_cls = model_cls
        self.excludable = excludable
        self.exclude_through = exclude_through

        self._fields: dict[str, "ForeignKeyField"] = dict()
        self._prefixes: dict[str, str] = dict()
        self._parent_aliases: dict[str, str] = dict()
        self._resolved_names: dict[str, str] = dict()

    def resolve_columns(self, columns_names: list[str]) -> dict:
        """
        Takes raw query prefixed column and resolves the prefixes to
        relation strings (relation names connected with dunders).

        :param columns_names: list of column names with prefixes from query
        :type columns_names: list[str]
        :return: dictionary of prefix: resolved names
        :rtype: Union[None, dict[str, str]]
        """
        self._create_prefixes_map()
        for column_name in columns_names:
            column_parts = column_name.split("_")
            potential_prefix = column_parts[0]
            if potential_prefix in self._prefixes:
                self._resolve_column_with_prefix(
                    column_name=column_name, prefix=potential_prefix
                )
            else:
                allowed_columns = self.model_cls.own_table_columns(
                    model=self.model_cls,
                    excludable=self.excludable,
                    add_pk_columns=False,
                )
                if column_name in allowed_columns:
                    self._resolved_names[column_name] = column_name

        return self._resolved_names

    def _resolve_column_with_prefix(self, column_name: str, prefix: str) -> None:
        """
        Takes the prefixed column, checks if field should be excluded, and if not
        it proceeds to replace prefix of a table with full relation string.

        Sample: translates: "xsd12df_name" -> into: "posts__user__name"

        :param column_name: prefixed name of the column
        :type column_name: str
        :param prefix: extracted prefix
        :type prefix: str
        """
        relation_str = self._prefixes[prefix]
        field = self._fields[prefix]
        is_through = field.is_multi and field.through.get_name() in relation_str
        if self._check_if_field_is_excluded(
            prefix=prefix, field=field, is_through=is_through
        ):
            return

        target_model = field.through if is_through else field.to
        allowed_columns = target_model.own_table_columns(
            model=target_model,
            excludable=self.excludable,
            alias=prefix,
            add_pk_columns=False,
        )
        new_column_name = column_name.replace(f"{prefix}_", "")
        if new_column_name in allowed_columns:
            self._resolved_names[column_name] = column_name.replace(
                f"{prefix}_", f"{relation_str}__"
            )

    def _check_if_field_is_excluded(
        self, prefix: str, field: "ForeignKeyField", is_through: bool
    ) -> bool:
        """
        Checks if given relation is excluded in current query.
        Exclusions are stored on the model owning the relation, which is the model
        joined under the parent path of the current table.

        :param prefix: table alias
        :type prefix: str
        :param field: field with relation
        :type field: ForeignKeyField
        :param is_through: flag if current table is a through table
        :type is_through: bool
        :return: result of the check
        :rtype: bool
        """
        field_name = field.through.get_name() if is_through else field.name
        parent_excludable = self.excludable.get(
            field.owner, alias=self._parent_aliases[prefix]
        )
        return parent_excludable.is_excluded(field_name)

    def _create_prefixes_map(self) -> None:
        """
        Creates a map of table aliases to relation strings used as column labels,
        together with the relation field and the alias of the parent table.
        Through tables of many to many relations are labelled with the through
        model name following the parent relation string.
        """
        for related in self.select_related:
            model_cls = self.model_cls
            related_str = ""
            for relation in related.split("__"):
                parent_str = related_str
                related_str = f"{parent_str}__{relation}" if parent_str else relation
                field = cast(
                    "ForeignKeyField", model_cls.ormar_config.model_fields[relation]
                )
                if field.is_multi and not self.exclude_through:
                    through_name = field.through.get_name()
                    self._register_prefix(
                        alias=alias_for(f"{related_str}__{through_name}"),
                        label=(
                            f"{parent_str}__{through_name}"
                            if parent_str
                            else through_name
                        ),
                        field=field,
                        parent_str=parent_str,
                    )
                self._register_prefix(
                    alias=alias_for(related_str),
                    label=related_str,
                    field=field,
                    parent_str=parent_str,
                )
                model_cls = field.to

    def _register_prefix(
        self, alias: str, label: str, field: "ForeignKeyField", parent_str: str
    ) -> None:
        """
        Registers a joined table alias with its label, field and parent alias.

        :param alias: alias of the joined table
        :type alias: str
        :param label: relation string used to label the table columns
        :type label: str
        :param field: relation field leading to the table
        :type field: ForeignKeyField
        :param parent_str: relation path of the model owning the relation
        :type parent_str: str
        """
        self._prefixes[alias] = label
        self._fields[alias] = field
        self._parent_aliases[alias] = alias_for(parent_str)
