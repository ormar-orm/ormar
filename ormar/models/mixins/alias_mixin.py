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
    _column_to_field_map: dict[str, str]
    _multi_column_fields: tuple[tuple[str, tuple[str, ...]], ...]

    @classmethod
    def _build_alias_cache(cls) -> None:
        """
        Build and cache alias mappings for this model class.
        Builds:
        - _field_to_alias_map: field_name -> db_alias
        - _alias_to_field_map: db_alias -> field_name (reverse)
        - _column_to_field_map: every table column -> field storing it (the
          columns of a foreign key stored in several columns map to it too)
        - _multi_column_fields: (field_name, columns) of such foreign keys
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
        column_to_field = dict(cls._alias_to_field_map)
        multi_column_fields = []
        for field_name, field in cls.ormar_config.model_fields.items():
            if not field.is_relation or field.virtual or field.is_multi:
                continue
            columns = field.column_aliases()
            if len(columns) > 1:
                multi_column_fields.append((field_name, columns))
                column_to_field.update(dict.fromkeys(columns, field_name))
        cls._column_to_field_map = column_to_field
        cls._multi_column_fields = tuple(multi_column_fields)

    @classmethod
    def reset_column_caches(cls) -> None:
        """
        Drop the cached column mappings of the model, e.g. after forward
        references are resolved and a foreign key's columns are known.
        """
        for name in (
            "_field_to_alias_map",
            "_alias_to_field_map",
            "_column_to_field_map",
            "_multi_column_fields",
            "_table_column_pairs",
        ):
            if name in cls.__dict__:
                delattr(cls, name)

    @classmethod
    def get_field_name_of_column(cls, column: str) -> str:
        """
        Returns the ormar field storing a table column, also for the columns
        of a foreign key stored in several columns.

        :param column: column name in the database
        :type column: str
        :return: field name, the column itself if no field is aliased to it
        :rtype: str
        """
        try:
            return cls._column_to_field_map.get(column, column)
        except AttributeError:
            cls._build_alias_cache()
            return cls._column_to_field_map.get(column, column)

    @classmethod
    def translate_row_to_fields(cls, row: dict) -> dict:
        """
        Translates a database row of the model's table into field values: the
        columns of a foreign key stored in several columns become one tuple
        (``None`` when any of them is null).

        :param row: column values by column name
        :type row: dict
        :return: field values by field name
        :rtype: dict
        """
        values = cls.translate_aliases_to_columns(row)
        for field_name, columns in cls._multi_column_fields:
            parts = tuple(values.pop(column, None) for column in columns)
            values[field_name] = None if None in parts else parts
        return values

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
    def get_column_aliases(cls, field_name: str) -> tuple[str, ...]:
        """
        Returns db column names of given ormar field: one for most fields,
        several for a foreign key stored in several columns.

        :param field_name: name of the field
        :type field_name: str
        :return: column names of the field
        :rtype: tuple[str, ...]
        """
        field = cls.ormar_config.model_fields.get(field_name)
        if field is None:
            return (cls.get_column_alias(field_name=field_name),)
        return field.column_aliases()

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
        except AttributeError:  # pragma: nocover
            cls._build_alias_cache()
            alias_to_field = cls._alias_to_field_map
        for key in list(new_kwargs.keys()):
            field_name = alias_to_field.get(key)
            if field_name and field_name != key:
                new_kwargs[field_name] = new_kwargs.pop(key)
        return new_kwargs
