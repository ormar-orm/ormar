"""
Column-level description of a relation: which columns of one table connect to
which columns of the next table.

A ``RelationLink`` is the only place that decides how two models are joined,
filtered against each other and how a relation's key value is read, so joins,
prefetch and relation proxies never derive the columns themselves.
"""

import operator
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping, cast

from pydantic import BaseModel

if TYPE_CHECKING:  # pragma: no cover
    from ormar.fields import BaseField, ForeignKeyField
    from ormar.models import Model


def qualified_fk_reference(target: type["Model"], column_alias: str) -> str:
    """
    Builds the dotted reference string passed to ``sqlalchemy.ForeignKey``.

    SQLAlchemy expects ``"schema.table.column"`` for cross-schema foreign keys
    and ``"table.column"`` when the target is in the default schema.

    :param target: target ormar Model
    :type target: type["Model"]
    :param column_alias: alias of the referenced column on the target table
    :type column_alias: str
    :return: dotted reference string
    :rtype: str
    """
    config = target.ormar_config
    prefix = f"{config.schema}." if config.schema else ""
    return f"{prefix}{config.tablename}.{column_alias}"


def key_value(parts: tuple[Any, ...]) -> Any:
    """
    Pack the parts of a key into its value: the part itself for a key of one
    column, a tuple otherwise.

    :param parts: key parts in key order
    :type parts: tuple[Any, ...]
    :return: key value
    :rtype: Any
    """
    return parts[0] if len(parts) == 1 else parts


def key_parts(value: Any, size: int) -> tuple[Any, ...]:
    """
    Unpack a key value into its parts, the inverse of ``key_value``.

    :param value: key value
    :type value: Any
    :param size: number of key columns
    :type size: int
    :return: key parts in key order
    :rtype: tuple[Any, ...]
    """
    return (value,) if size == 1 else tuple(value)


ColumnReader = Callable[[Any], Any]


def column_value(model: type["Model"], data: Any, column: str) -> Any:
    """
    Read the value one of a model's columns stores out of an instance of the
    model, its key-only pydantic model or a dict of its fields (see
    ``column_reader``).

    :param model: model the column belongs to
    :type model: type[Model]
    :param data: instance of the model, its key-only model or a dict of its
        fields
    :type data: Any
    :param column: database column of the model
    :type column: str
    :return: raw value of the column, ``None`` when not set
    :rtype: Any
    """
    return model.column_reader(column=column)(data)


def column_reader(model: type["Model"], column: str) -> ColumnReader:
    """
    Build the reader of one of a model's columns. A column stored by a
    relation is read out of the related value (a model, a dict of its fields
    or a raw key), recursively, so a key holding foreign keys is read column
    by column like any other::

        Track, "album" -> track.album.id
        Membership, "member_login" -> membership.member.login
        {"member": (1, "alice")}, "member_login" -> "alice"

    A column several fields share is read from the first one holding a value.

    :param model: model the column belongs to
    :type model: type[Model]
    :param column: database column of the model
    :type column: str
    :return: callable reading the column's raw value out of an instance of the
        model, its key-only model or a dict of its fields (``None`` when not
        set)
    :rtype: ColumnReader
    """
    model_fields = model.ormar_config.model_fields
    readers = [
        field_reader(name=name, field=model_fields[name], column=column)
        for name in model.get_field_names_of_column(column=column)
    ]
    if len(readers) == 1:
        return readers[0]

    def read_first_set(data: Any) -> Any:
        for reader in readers:
            value = reader(data)
            if value is not None:
                return value
        return None

    return read_first_set


def field_reader(name: str, field: "BaseField", column: str) -> ColumnReader:
    """
    :param name: name of the field storing the column
    :type name: str
    :param field: the field
    :type field: BaseField
    :param column: database column the field stores
    :type column: str
    :return: callable reading the column's raw value out of the field's value
        (see ``column_reader``)
    :rtype: ColumnReader
    """
    if not field.is_relation:

        def read_plain(data: Any) -> Any:
            if isinstance(data, dict):
                return data.get(name)
            return getattr(data, name, None)

        return read_plain
    link = cast("ForeignKeyField", field).link
    position = link.source_columns().index(column)
    single = len(link.pairs) == 1
    target = link.target
    target_column = link.pairs[position].target_column
    # resolved on first use: a self-referencing key reaches its own reader
    related: list[ColumnReader] = []

    def read_relation(data: Any) -> Any:
        value = data.get(name) if isinstance(data, dict) else getattr(data, name, None)
        if value is None:
            return None
        if isinstance(value, (BaseModel, dict)):
            if not related:
                related.append(target.column_reader(column=target_column))
            return related[0](value)
        return value if single else tuple(value)[position]

    return read_relation


@dataclass(frozen=True)
class ColumnPair:
    """
    One column of the source table and the column of the target table it
    matches.

    :ivar source_field: ormar field on the source model (the relation field
        itself on the side holding the foreign key)
    :ivar source_column: database column on the source table
    :ivar target_field: ormar field on the target model
    :ivar target_column: database column on the target table
    """

    source_field: str
    source_column: str
    target_field: str
    target_column: str

    def mirrored(self) -> "ColumnPair":
        """
        :return: the same pair seen from the target side
        :rtype: ColumnPair
        """
        return ColumnPair(
            source_field=self.target_field,
            source_column=self.target_column,
            target_field=self.source_field,
            target_column=self.source_column,
        )


@dataclass(frozen=True)
class RelationLink:
    """
    Ordered column pairs connecting a source model's table to a target model's
    table, e.g. ``tracks.album_ref -> albums.id`` for ``Track.album``.

    :ivar source: model the relation starts from
    :ivar target: model the relation leads to
    :ivar pairs: matched columns, in key order of the referenced key
    """

    source: type["Model"]
    target: type["Model"]
    pairs: tuple[ColumnPair, ...]

    def source_fields(self) -> tuple[str, ...]:
        """
        :return: ormar field names on the source side
        :rtype: tuple[str, ...]
        """
        return tuple(pair.source_field for pair in self.pairs)

    def source_columns(self) -> tuple[str, ...]:
        """
        :return: database columns on the source side
        :rtype: tuple[str, ...]
        """
        return tuple(pair.source_column for pair in self.pairs)

    def target_fields(self) -> tuple[str, ...]:
        """
        :return: ormar field names on the target side
        :rtype: tuple[str, ...]
        """
        return tuple(pair.target_field for pair in self.pairs)

    def target_columns(self) -> tuple[str, ...]:
        """
        :return: database columns on the target side
        :rtype: tuple[str, ...]
        """
        return tuple(pair.target_column for pair in self.pairs)

    def mirrored(self) -> "RelationLink":
        """
        :return: the same link walked from the target to the source
        :rtype: RelationLink
        """
        return RelationLink(
            source=self.target,
            target=self.source,
            pairs=tuple(pair.mirrored() for pair in self.pairs),
        )

    def key_of(self, model: "Model") -> Any:
        """
        Read the value a source model holds for this link, column by column
        (see ``column_value``)::

            Track.album link, track.album == Album(id=4) -> 4
            Ledger.entries link, ledger.tenant == Tenant(id=4) -> (4, 1)

        :param model: instance of the source model
        :type model: Model
        :return: the value, a tuple for links of several columns
        :rtype: Any
        """
        readers = self.source_readers
        if len(readers) == 1:
            return readers[0](model)
        return tuple([read(model) for read in readers])

    def values_for(self, related: Any) -> dict[str, Any]:
        """
        Turn a related value into the source-side values that store it, keyed
        by ``source_keys()``::

            Track.album link: Album(id=2) | {"id": 2} | 2 -> {"album": 2}
            OrderLine.order link: Order(1, 7) -> {"ord_tenant": 1, "ord_no": 7}

        A related model without its key yields ``None`` values; callers decide
        whether that is an error.

        :param related: related model (or its key-only pydantic model), dict of
            its fields, raw key value or ``None``
        :type related: Any
        :return: values by source field name
        :rtype: dict[str, Any]
        """
        if related is None:
            parts: tuple[Any, ...] = (None,) * len(self.pairs)
        elif isinstance(related, (BaseModel, dict)):
            parts = tuple([read(related) for read in self.target_readers])
        else:
            parts = key_parts(value=related, size=len(self.pairs))
        return dict(zip(self.source_keys(), parts))

    @cached_property
    def source_readers(self) -> tuple[ColumnReader, ...]:
        """
        :return: readers of the source columns (see ``column_reader``)
        :rtype: tuple[ColumnReader, ...]
        """
        return tuple(
            self.source.column_reader(column=pair.source_column) for pair in self.pairs
        )

    @cached_property
    def target_readers(self) -> tuple[ColumnReader, ...]:
        """
        :return: readers of the target columns (see ``column_reader``)
        :rtype: tuple[ColumnReader, ...]
        """
        return tuple(
            self.target.column_reader(column=pair.target_column) for pair in self.pairs
        )

    def source_keys(self) -> tuple[str, ...]:
        """
        :return: keys the source side stores the link's values under in a model
            dict: the relation field for a single column, the columns otherwise
        :rtype: tuple[str, ...]
        """
        if len(self.pairs) == 1:
            return (self.pairs[0].source_field,)
        return self.source_columns()

    def key_getter(
        self, *, side: Literal["source", "target"], prefix: str = ""
    ) -> Callable[[Mapping[str, Any]], Any]:
        """
        Build a reader of this link's value out of a database row.

        :param side: which table's columns the row holds
        :type side: Literal["source", "target"]
        :param prefix: table prefix of the row's column labels
            (``<prefix>_<column>``), empty for unprefixed columns
        :type prefix: str
        :return: reader returning the value, a tuple for links of several
            columns
        :rtype: Callable[[Mapping[str, Any]], Any]
        """
        columns = self.source_columns() if side == "source" else self.target_columns()
        label_prefix = f"{prefix}_" if prefix else ""
        return operator.itemgetter(*(label_prefix + column for column in columns))
