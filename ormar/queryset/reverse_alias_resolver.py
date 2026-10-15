from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:  # pragma: no cover
    from ormar import ForeignKeyField, Model
    from ormar.models.excludable import Excludable, ExcludableItems


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
        self.reversed_aliases = (
            self.model_cls.ormar_config.alias_manager.reversed_aliases
        )
        self.excludable = excludable
        self.exclude_through = exclude_through

        self._fields: dict[str, "ForeignKeyField"] = dict()
        self._prefixes: dict[str, str] = dict()
        self._previous_prefixes: list[str] = [""]
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
            if potential_prefix in self.reversed_aliases:
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
        relation = self.reversed_aliases.get(prefix, None)
        assert relation is not None, "Prefix not found in reversed aliases"
        relation_str = self._prefixes.get(relation, None)
        field = self._fields.get(relation, None)
        if relation_str is None or field is None:
            return
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

        Note that in contrary to other queryset methods here you can exclude the
        in-between models but keep the end columns, which does not make sense
        when parsing the raw data into models.

        So in relation category -> category_x_post -> post -> user you can exclude
        category_x_post and post models but can keep the user one. (in ormar model
        context that is not possible as if you would exclude through and post model
        there would be no way to reach user model).

        Exclusions happen on a model before the current one, so we need to move back
        in chain of model by one or by two (m2m relations have through model in between)

        :param prefix: table alias
        :type prefix: str
        :param field: field with relation
        :type field: ForeignKeyField
        :param is_through: flag if current table is a through table
        :type is_through: bool
        :return: result of the check
        :rtype: bool
        """
        shift, field_name = 1, field.name
        if is_through:
            field_name = field.through.get_name()
        elif field.is_multi:
            shift = 2
        previous_excludable = self._get_previous_excludable(
            prefix=prefix, field=field, shift=shift
        )
        return previous_excludable.is_excluded(field_name)

    def _get_previous_excludable(
        self, prefix: str, field: "ForeignKeyField", shift: int = 1
    ) -> "Excludable":
        """
        Returns excludable related to model previous in chain of models.
        Used to check if current model should be excluded.

        :param prefix: prefix of a current table
        :type prefix: str
        :param field: field with relation
        :type field: ForeignKeyField
        :param shift: how many model back to go - for m2m it's 2 due to through models
        :type shift: int
        :return: excludable for previous model
        :rtype: Excludable
        """
        if prefix not in self._previous_prefixes:
            self._previous_prefixes.append(prefix)
        previous_prefix_ind = self._previous_prefixes.index(prefix)
        previous_prefix = (
            self._previous_prefixes[previous_prefix_ind - shift]
            if previous_prefix_ind > (shift - 1)
            else ""
        )
        return self.excludable.get(field.owner, alias=previous_prefix)

    def _create_prefixes_map(self) -> None:
        """
        Creates a map of alias manager aliases keys to relation strings.
        I.e in alias manager you can have alias user_roles: xas12ad

        This method will create entry user_roles: roles, where roles is a name of
        relation on user model.

        Will also keep the relation field in separate dictionary so we can later
        extract field names and owner models.

        """
        for related in self.select_related:
            model_cls = self.model_cls
            related_split = related.split("__")
            related_str = ""
            for relation in related_split:
                previous_related_str = f"{related_str}__" if related_str else ""
                new_related_str = previous_related_str + relation
                field = model_cls.ormar_config.model_fields[relation]
                field = cast("ForeignKeyField", field)
                prefix_name = self._handle_through_fields_and_prefix(
                    model_cls=model_cls,
                    field=field,
                    previous_related_str=previous_related_str,
                    relation=relation,
                )

                self._prefixes[prefix_name] = new_related_str
                self._fields[prefix_name] = field
                model_cls = field.to
                related_str = new_related_str

    def _handle_through_fields_and_prefix(
        self,
        model_cls: type["Model"],
        field: "ForeignKeyField",
        previous_related_str: str,
        relation: str,
    ) -> str:
        """
        Registers through models for m2m relations and switches prefix for
        the one linking from through model to target model.

        For other relations returns current model name + relation name as prefix.
        Nested relations are a chain of relation names with __ in between.

        :param model_cls: model of current relation
        :type model_cls: type["Model"]
        :param field: field with relation
        :type field: ForeignKeyField
        :param previous_related_str: concatenated chain linked with "__"
        :type previous_related_str: str
        :param relation: name of the current relation in chain
        :type relation: str
        :return: name of prefix to populate
        :rtype: str
        """
        prefix_name = f"{model_cls.get_name()}_{relation}"
        if field.is_multi:
            through_name = field.through.get_name()
            if not self.exclude_through:
                self._fields[prefix_name] = field
                new_through_str = previous_related_str + through_name
                self._prefixes[prefix_name] = new_through_str
            prefix_name = f"{through_name}_{field.get_relation_name()}"
        return prefix_name
