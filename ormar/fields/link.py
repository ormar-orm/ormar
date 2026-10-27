"""
Column-level description of a relation: which columns of one table connect to
which columns of the next table.

A ``RelationLink`` is the only place that decides how two models are joined,
filtered against each other and how a relation's key value is read, so joins,
prefetch and relation proxies never derive the columns themselves.
"""

import operator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Literal, Mapping

import ormar

if TYPE_CHECKING:  # pragma: no cover
    from ormar.models import Model


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
        Read the value a source model holds for this link. A related model on
        the source side is replaced by its value of the target field::

            Track.album link, track.album == Album(id=4) -> 4

        :param model: instance of the source model
        :type model: Model
        :return: the value, a tuple for links of several columns
        :rtype: Any
        """
        values = []
        for pair in self.pairs:
            value = getattr(model, pair.source_field)
            if isinstance(value, ormar.Model):
                value = getattr(value, pair.target_field)
            values.append(value)
        return values[0] if len(values) == 1 else tuple(values)

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
