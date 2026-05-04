from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Mapping,
    Union,
    cast,
)

from ormar.models.excludable import ExcludableItems
from ormar.models.mixins.relation_mixin import RelationMixin

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model

    IntStr = Union[int, str]
    AbstractSetIntStr = AbstractSet[IntStr]
    MappingIntStrAny = Mapping[IntStr, Any]


class ExcludableMixin(RelationMixin):
    """
    Used to include/exclude given set of fields on models during load and dict() calls.
    """

    if TYPE_CHECKING:  # pragma: no cover
        from ormar import Model
        from ormar.models import ModelRow

    @staticmethod
    def _populate_pk_column(
        model: Union[type["Model"], type["ModelRow"]],
        columns: list[str],
        use_alias: bool = False,
    ) -> list[str]:
        """
        Adds primary key column/alias (depends on use_alias flag) to list of
        column names that are selected.

        :param model: model on columns are selected
        :type model: type["Model"]
        :param columns: list of columns names
        :type columns: list[str]
        :param use_alias: flag to set if aliases or field names should be used
        :type use_alias: bool
        :return: list of columns names with pk column in it
        :rtype: list[str]
        """
        pk_alias = (
            model.get_column_alias(model.ormar_config.pkname)
            if use_alias
            else model.ormar_config.pkname
        )
        if pk_alias not in columns:
            columns.append(pk_alias)
        return columns

    @classmethod
    def own_table_columns(
        cls,
        model: Union[type["Model"], type["ModelRow"]],
        excludable: ExcludableItems,
        alias: str = "",
        use_alias: bool = False,
        add_pk_columns: bool = True,
    ) -> list[str]:
        """
        Returns list of aliases or field names for given model.
        Aliases/names switch is use_alias flag.

        If provided only fields included in fields will be returned.
        If provided fields in exclude_fields will be excluded in return.

        Primary key field is always added and cannot be excluded (will be added anyway).

        :param add_pk_columns: flag if add primary key - always yes if ormar parses data
        :type add_pk_columns: bool
        :param alias: relation prefix
        :type alias: str
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :param model: model on columns are selected
        :type model: type["Model"]
        :param use_alias: flag if aliases or field names should be used
        :type use_alias: bool
        :return: list of column field names or aliases
        :rtype: list[str]
        """
        model_excludable = excludable.get(model_cls=model, alias=alias)  # type: ignore
        columns = [
            model.get_column_name_from_alias(col.name) if not use_alias else col.name
            for col in model.ormar_config.table.columns
        ]
        field_names = [
            model.get_column_name_from_alias(col.name)
            for col in model.ormar_config.table.columns
        ]
        if model_excludable.include:
            columns = [
                col
                for col, name in zip(columns, field_names)
                if model_excludable.is_included(name)
            ]
        if model_excludable.exclude:
            columns = [
                col
                for col, name in zip(columns, field_names)
                if not model_excludable.is_excluded(name)
            ]

        # always has to return pk column for ormar to work
        if add_pk_columns:
            columns = cls._populate_pk_column(
                model=model, columns=columns, use_alias=use_alias
            )

        return columns

    @classmethod
    def _update_excluded_with_related(cls, exclude: Union[set, dict, None]) -> set:
        """
        Used during generation of the dict().
        To avoid cyclical references and max recurrence limit nested models have to
        exclude related models that are not mandatory.

        For a main model (not nested) only nullable related field names are added to
        exclusion, for nested models all related models are excluded.

        :param exclude: set/dict with fields to exclude
        :type exclude: Union[set, dict, None]
        :return: set or dict with excluded fields added.
        :rtype: Union[set, dict]
        """
        exclude = exclude or set()
        related_set = cls.extract_related_names()
        if isinstance(exclude, set):
            exclude = {s for s in exclude}
            exclude = exclude.union(related_set)
        elif isinstance(exclude, dict):
            # relations are handled in ormar - take only own fields (ellipsis in dict)
            exclude = {k for k, v in exclude.items() if v is Ellipsis}
            exclude = exclude.union(related_set)
        return exclude

    @classmethod
    def _update_excluded_with_pks_and_through(
        cls, exclude: set, exclude_primary_keys: bool, exclude_through_models: bool
    ) -> set:
        """
        Updates excluded names with name of pk column if exclude flag is set.

        :param exclude: set of names to exclude
        :type exclude: set
        :param exclude_primary_keys: flag if the primary keys should be excluded
        :type exclude_primary_keys: bool
        :return: set updated with pk if flag is set
        :rtype: set
        """
        if exclude_primary_keys:
            exclude.add(cls.ormar_config.pkname)
        if exclude_through_models:
            exclude = exclude.union(cls.extract_through_names())
        return exclude

    @classmethod
    def get_names_to_exclude(cls, excludable: ExcludableItems, alias: str) -> set:
        """
        Returns a set of models field names that should be explicitly excluded
        during model initialization.

        Those fields will be set to None to avoid ormar/pydantic setting default
        values on them. They should be returned as None in any case.

        Used in parsing data from database rows that construct Models by initializing
        them with dicts constructed from those db rows.

        :param alias: alias of current relation
        :type alias: str
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :return: set of field names that should be excluded
        :rtype: set
        """
        model = cast(type["Model"], cls)
        model_excludable = excludable.get(model_cls=model, alias=alias)
        fields_names = cls.extract_db_own_fields()
        if model_excludable.include:
            fields_to_keep = model_excludable.include.intersection(fields_names)
        else:
            fields_to_keep = fields_names

        fields_to_exclude = fields_names - fields_to_keep

        if model_excludable.exclude:
            fields_to_exclude = fields_to_exclude.union(
                model_excludable.exclude.intersection(fields_names)
            )
        fields_to_exclude = fields_to_exclude - {cls.ormar_config.pkname}

        return fields_to_exclude
