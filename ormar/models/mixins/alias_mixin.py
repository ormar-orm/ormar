from typing import TYPE_CHECKING, Any, NamedTuple, cast

import ormar_rust_utils

from ormar.exceptions import ModelPersistenceError
from ormar.fields.link import ColumnReader, column_reader

if TYPE_CHECKING:  # pragma: no cover
    from ormar.fields import ForeignKeyField
    from ormar.models import Model


class SharedKey(NamedTuple):
    """
    Where the value of a column several fields share is kept in a model dict.

    :ivar kept: key the column's value is kept under (the plain field's name
        when one stores it, else the first field's key)
    :ivar column: the shared column
    """

    kept: str
    column: str


class SharedPlainField(NamedTuple):
    """
    A plain field storing a column foreign keys share, and where those
    foreign keys hold the column's value.

    :ivar field: name of the plain field
    :ivar sources: ``(foreign key name, key of the column in its
        RelationLink.values_for)`` of every foreign key sharing the column, in
        declaration order
    """

    field: str
    sources: tuple[tuple[str, str], ...]


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
    _column_owners_map: dict[str, tuple[str, ...]]
    _shared_value_keys: dict[str, SharedKey]
    _shared_plain_fields: tuple[SharedPlainField, ...]
    _column_readers: dict[str, "ColumnReader"]

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
        owners: dict[str, list[str]] = {}
        for field_name, field in cls.ormar_config.model_fields.items():
            if field.virtual or field.is_multi:
                continue
            columns = field.column_aliases()
            for column in columns:
                owners.setdefault(column, []).append(field_name)
            if field.is_relation and len(columns) > 1:
                multi_column_fields.append((field_name, columns))
                for column in columns:
                    column_to_field.setdefault(column, field_name)
        cls._column_to_field_map = column_to_field
        cls._multi_column_fields = tuple(multi_column_fields)
        cls._column_owners_map = {
            column: tuple(names) for column, names in owners.items()
        }
        cls._shared_value_keys = cls._build_shared_value_keys(owners=owners)
        cls._shared_plain_fields = cls._build_shared_plain_fields(owners=owners)

    @classmethod
    def _build_shared_value_keys(
        cls, owners: dict[str, list[str]]
    ) -> dict[str, SharedKey]:
        """
        For every column stored by several fields, map the key each of them
        stores its value under in a model dict (the field name, or the column
        for a foreign key stored in several columns) to where the column's
        value is kept.

        :param owners: fields storing each column
        :type owners: dict[str, list[str]]
        :return: ``{value key: SharedKey}``
        :rtype: dict[str, SharedKey]
        """
        model_fields = cls.ormar_config.model_fields
        shared: dict[str, SharedKey] = {}
        for column, names in owners.items():
            if len(names) == 1:
                continue
            keys = [
                column if len(model_fields[name].column_aliases()) > 1 else name
                for name in names
            ]
            plain = [name for name in names if not model_fields[name].is_relation]
            kept = plain[0] if plain else keys[0]
            shared.update({key: SharedKey(kept=kept, column=column) for key in keys})
        return shared

    @classmethod
    def _build_shared_plain_fields(
        cls, owners: dict[str, list[str]]
    ) -> tuple[SharedPlainField, ...]:
        """
        :param owners: fields storing each column
        :type owners: dict[str, list[str]]
        :return: the plain fields storing a column foreign keys share
        :rtype: tuple[SharedPlainField, ...]
        """
        model_fields = cls.ormar_config.model_fields
        plain_fields = []
        for column, names in owners.items():
            plain = [name for name in names if not model_fields[name].is_relation]
            if len(names) == 1 or not plain:
                continue
            sources = tuple(
                (
                    name,
                    column if len(model_fields[name].column_aliases()) > 1 else name,
                )
                for name in names
                if model_fields[name].is_relation
            )
            plain_fields.append(SharedPlainField(field=plain[0], sources=sources))
        return tuple(plain_fields)

    @classmethod
    def reset_column_caches(cls) -> None:
        """
        Drop the cached column mappings of the model and the layout of its
        primary key, e.g. after forward references are resolved and a foreign
        key's columns are known.
        """
        for name in (
            "_field_to_alias_map",
            "_alias_to_field_map",
            "_column_to_field_map",
            "_multi_column_fields",
            "_column_owners_map",
            "_shared_value_keys",
            "_shared_plain_fields",
            "_table_column_pairs",
            "_column_readers",
        ):
            if name in cls.__dict__:
                delattr(cls, name)
        cls.ormar_config.pk.cached_layout = None

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
        columns_read: set[str] = set()
        for field_name, columns in cls._multi_column_fields:
            parts = tuple(values.get(column) for column in columns)
            values[field_name] = None if None in parts else parts
            columns_read.update(columns)
        model_fields = cls.ormar_config.model_fields
        for column in columns_read:
            if column not in model_fields:
                values.pop(column, None)
        return values

    @classmethod
    def get_field_names_of_column(cls, column: str) -> tuple[str, ...]:
        """
        :param column: column name in the database
        :type column: str
        :return: every ormar field storing the column, in declaration order
            (several when fields share it)
        :rtype: tuple[str, ...]
        """
        try:
            owners = cls._column_owners_map
        except AttributeError:
            cls._build_alias_cache()
            owners = cls._column_owners_map
        return owners.get(column) or (cls.get_field_name_of_column(column=column),)

    @classmethod
    def column_reader(cls, column: str) -> "ColumnReader":
        """
        :param column: column name in the database
        :type column: str
        :return: cached reader of the column's raw value out of an instance of
            the model, its key-only model or a dict of its fields (see
            ``ormar.fields.link.column_reader``)
        :rtype: ColumnReader
        """
        readers = cls.__dict__.get("_column_readers")
        if readers is None:
            readers = {}
            cls._column_readers = readers
        reader = readers.get(column)
        if reader is None:
            reader = column_reader(model=cast("type[Model]", cls), column=column)
            readers[column] = reader
        return reader

    @classmethod
    def shared_value_keys(cls) -> dict[str, SharedKey]:
        """
        :return: for the keys of columns several fields share, the key the
            column's value is kept under and the column (see
            ``_build_shared_value_keys``)
        :rtype: dict[str, SharedKey]
        """
        try:
            return cls._shared_value_keys
        except AttributeError:
            cls._build_alias_cache()
            return cls._shared_value_keys

    @classmethod
    def fill_shared_plain_fields(cls, kwargs: dict[str, Any]) -> None:
        """
        Fill an unset plain field storing a column foreign keys share from
        the first of those foreign keys holding a value for the column, so a
        new model's plain field, ``model_dump()`` and ``pk`` agree before it
        is saved.

        :param kwargs: keyword arguments of a model being initialized, with
            relations already expanded, updated in place
        :type kwargs: dict[str, Any]
        """
        try:
            plain_fields = cls._shared_plain_fields
        except AttributeError:
            cls._build_alias_cache()
            plain_fields = cls._shared_plain_fields
        model_fields = cls.ormar_config.model_fields
        for plain in plain_fields:
            if kwargs.get(plain.field) is not None:
                continue
            for field_name, key in plain.sources:
                related = kwargs.get(field_name)
                if related is None:
                    continue
                link = cast("ForeignKeyField", model_fields[field_name]).link
                value = link.values_for(related=related)[key]
                if value is not None:
                    kwargs[plain.field] = value
                    break

    @classmethod
    def value_keys(cls, field_name: str) -> tuple[str, ...]:
        """
        :param field_name: name of a field stored in the model's table
        :type field_name: str
        :return: keys the field's values are kept under in a model dict about
            to be written: its name, or its columns for a foreign key stored
            in several columns; a shared column under the key it is kept under
        :rtype: tuple[str, ...]
        """
        shared = cls.shared_value_keys()
        field = cls.ormar_config.model_fields[field_name]
        columns = field.column_aliases()
        keys = columns if field.is_relation and len(columns) > 1 else (field_name,)
        return tuple(shared[key].kept if key in shared else key for key in keys)

    @classmethod
    def merge_relation_values(
        cls, target: dict, relation_values: dict[str, dict[str, Any]]
    ) -> None:
        """
        Put the values foreign keys store (``RelationLink.values_for``) into a
        model dict about to be written. The foreign keys' own entries are
        dropped from the dict first, then their values are merged in the
        order the fields are declared, so the result never depends on the
        order the foreign keys were processed in.

        A column several fields share keeps one value: a ``None`` is not
        written to it while another of its fields holds a value (a shared
        column is cleared only when every foreign key storing it is cleared
        and no plain field stores it), an unset plain field sharing the
        column is filled in, and two different values are an error.

        :raises ModelPersistenceError: if a shared column gets two values
        :param target: model dict being prepared, updated in place
        :type target: dict
        :param relation_values: values of each foreign key by key, by foreign
            key name
        :type relation_values: dict[str, dict[str, Any]]
        """
        for field_name in relation_values:
            target.pop(field_name, None)
        shared = cls.shared_value_keys()
        if not shared:
            for values in relation_values.values():
                target.update(values)
            return
        cleared: dict[str, int] = {}
        for field_name in cls.ormar_config.model_fields:
            field_values = relation_values.get(field_name)
            if field_values is None:
                continue
            for key, value in field_values.items():
                if key not in shared:
                    target[key] = value
                    continue
                kept, column = shared[key]
                current = target.get(kept)
                if value is None:
                    cleared[kept] = cleared.get(kept, 0) + 1
                    continue
                if current is None:
                    target[kept] = value
                elif value != current:
                    raise ModelPersistenceError(
                        f"{cls.__name__}.{field_name} stores {value!r} in column "
                        f"{column}, which another field sets to {current!r}."
                    )
        cls.clear_shared_columns(target=target, cleared=cleared)

    @classmethod
    def clear_shared_columns(cls, target: dict, cleared: dict[str, int]) -> None:
        """
        Write ``None`` to a shared column no plain field stores when every key
        storing it was cleared (e.g. two nullable foreign keys sharing a
        ``tenant_id`` both set to ``None``).

        :param target: model dict being prepared, updated in place
        :type target: dict
        :param cleared: number of ``None`` values given per kept key
        :type cleared: dict[str, int]
        """
        if not cleared:
            return
        shared = cls.shared_value_keys()
        model_fields = cls.ormar_config.model_fields
        for kept, count in cleared.items():
            stored_by_plain = (
                kept in model_fields and not model_fields[kept].is_relation
            )
            sharing = sum(1 for value in shared.values() if value.kept == kept)
            if not stored_by_plain and target.get(kept) is None and count == sharing:
                target[kept] = None

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
