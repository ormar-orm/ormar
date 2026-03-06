from typing import TYPE_CHECKING

import ormar_rust_utils


class AliasMixin:
    """
    Used to translate field names into database column names.
    """

    if TYPE_CHECKING:  # pragma: no cover
        from ormar.models.ormar_config import OrmarConfig

        ormar_config: OrmarConfig

    _alias_to_field_map: dict[str, str]
    _field_to_alias_map: dict[str, str]

    @classmethod
    def _build_alias_cache(cls) -> None:
        """
        Build and cache alias mappings for this model class.
        Builds two dicts:
        - _field_to_alias_map: field_name -> db_alias
        - _alias_to_field_map: db_alias -> field_name (reverse)
        """
        field_to_alias = {}
        for field_name, field in cls.ormar_config.model_fields.items():
            alias = field.get_alias()
            if alias:
                field_to_alias[field_name] = alias
        cls._field_to_alias_map = field_to_alias
        cls._alias_to_field_map = ormar_rust_utils.build_reverse_alias_map(
            field_to_alias
        )

    @classmethod
    def get_column_alias(cls, field_name: str) -> str:
        """
        Returns db alias (column name in db) for given ormar field.
        For fields without alias field name is returned.
        :param field_name: name of the field to get alias from
        :type field_name: str
        :return: alias (db name) if set, otherwise passed name
        :rtype: str
        """
        try:
            return cls._field_to_alias_map.get(field_name, field_name)
        except AttributeError:
            cls._build_alias_cache()
            return cls._field_to_alias_map.get(field_name, field_name)

    @classmethod
    def get_column_name_from_alias(cls, alias: str) -> str:
        """
        Returns ormar field name for given db alias (column name in db).
        If field do not have alias it's returned as is.
        :param alias:
        :type alias: str
        :return: field name if set, otherwise passed alias (db name)
        :rtype: str
        """
        try:
            return cls._alias_to_field_map.get(alias, alias)
        except AttributeError:
            cls._build_alias_cache()
            return cls._alias_to_field_map.get(alias, alias)

    @classmethod
    def translate_columns_to_aliases(cls, new_kwargs: dict) -> dict:
        """
        Translates dictionary of model fields changing field names into aliases.
        If field has no alias the field name remains intact.
        Only fields present in the dictionary are translated.
        :param new_kwargs: dict with fields names and their values
        :type new_kwargs: dict
        :return: dict with aliases and their values
        :rtype: dict
        """
        try:
            field_to_alias = cls._field_to_alias_map
        except AttributeError:
            cls._build_alias_cache()
            field_to_alias = cls._field_to_alias_map
        for field_name in list(new_kwargs.keys()):
            alias = field_to_alias.get(field_name)
            if alias and alias != field_name:
                new_kwargs[alias] = new_kwargs.pop(field_name)
        return new_kwargs

    @classmethod
    def translate_aliases_to_columns(cls, new_kwargs: dict) -> dict:
        """
        Translates dictionary of model fields changing aliases into field names.
        If field has no alias the alias is already a field name.
        Only fields present in the dictionary are translated.
        :param new_kwargs: dict with aliases and their values
        :type new_kwargs: dict
        :return: dict with fields names and their values
        :rtype: dict
        """
        try:
            alias_to_field = cls._alias_to_field_map
        except AttributeError:
            cls._build_alias_cache()
            alias_to_field = cls._alias_to_field_map
        for key in list(new_kwargs.keys()):
            field_name = alias_to_field.get(key)
            if field_name and field_name != key:
                new_kwargs[field_name] = new_kwargs.pop(key)
        return new_kwargs
