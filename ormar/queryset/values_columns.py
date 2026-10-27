"""
Maps prefixed columns of a raw query to the relation string keys returned by
``values()`` and ``values_list()``.
"""

from dataclasses import dataclass
from operator import itemgetter
from typing import TYPE_CHECKING, Any, Optional, cast

from ormar.relations.aliases import alias_for

if TYPE_CHECKING:  # pragma: no cover
    from ormar import ForeignKeyField, Model
    from ormar.models.excludable import ExcludableItems


@dataclass(frozen=True)
class ValuesColumns:
    """
    Columns of a raw query returned by ``values()`` / ``values_list()``.

    :ivar names: returned name (relation string) by query column label; the
        columns of a foreign key stored in several columns share one name
    :ivar key_parts: target field by query column label, for the columns of
        foreign keys stored in several columns
    """

    names: dict[str, str]
    key_parts: dict[str, str]


@dataclass(frozen=True)
class ReturnedColumn:
    """
    A field of the query columns resolved for ``values()``.

    :ivar name: name of the field
    :ivar part: target field of the column, for a foreign key stored in
        several columns
    """

    name: str
    part: Optional[str] = None


def resolve_column(
    model_cls: type["Model"], column: str, allowed: set[str]
) -> Optional[ReturnedColumn]:
    """
    :param model_cls: model of the table the column belongs to
    :type model_cls: type[Model]
    :param column: database column name
    :type column: str
    :param allowed: names of the fields to return
    :type allowed: set[str]
    :return: the field the column stores, ``None`` if it isn't returned
    :rtype: Optional[ReturnedColumn]
    """
    field_name = model_cls.get_field_name_of_column(column=column)
    if field_name not in allowed:
        return None
    field = model_cls.ormar_config.model_fields.get(field_name)
    if field is None or not field.is_relation or field.virtual or field.is_multi:
        return ReturnedColumn(name=field_name)
    pairs = cast("ForeignKeyField", field).link.pairs
    if len(pairs) == 1:
        return ReturnedColumn(name=field_name)
    part = next(pair.target_field for pair in pairs if pair.source_column == column)
    return ReturnedColumn(name=field_name, part=part)


def allowed_columns(
    model_cls: type["Model"],
    excludable: "ExcludableItems",
    alias: str,
    is_excluded: bool,
) -> set[str]:
    """
    Returns the columns of a joined table that should be returned to the user,
    without the primary key columns added only to construct models.

    :param model_cls: model of the joined table
    :type model_cls: type["Model"]
    :param excludable: structure of fields to include and exclude
    :type excludable: ExcludableItems
    :param alias: alias of the joined table
    :type alias: str
    :param is_excluded: flag if the relation is excluded on its parent model
    :type is_excluded: bool
    :return: set of column names
    :rtype: set[str]
    """
    if is_excluded:
        return set()
    return set(
        model_cls.own_table_columns(
            model=model_cls, excludable=excludable, alias=alias, add_pk_columns=False
        )
    )


def joined_table_columns(
    model_cls: type["Model"],
    select_related: list[str],
    excludable: "ExcludableItems",
    exclude_through: bool,
) -> dict[str, tuple[str, type["Model"], set[str]]]:
    """
    Maps the alias of each joined table to the relation string used to label
    its columns and the set of its columns that should be returned.

    Through tables of many to many relations are labelled with the through
    model name following the parent relation string.
    Relations excluded on their parent model keep their alias with no columns,
    so their columns are not mistaken for columns of the main model.

    :param model_cls: main model of the query
    :type model_cls: type["Model"]
    :param select_related: list of related relation strings joined in the query
    :type select_related: list[str]
    :param excludable: structure of fields to include and exclude
    :type excludable: ExcludableItems
    :param exclude_through: flag if through models should be excluded
    :type exclude_through: bool
    :return: dictionary of table alias to label and allowed columns
    :rtype: dict[str, tuple[str, set[str]]]
    """
    tables: dict[str, tuple[str, type["Model"], set[str]]] = dict()
    for related in select_related:
        current_model, parent_str = model_cls, ""
        for relation in related.split("__"):
            field = cast(
                "ForeignKeyField", current_model.ormar_config.model_fields[relation]
            )
            related_str = f"{parent_str}__{relation}" if parent_str else relation
            parent_excludable = excludable.get(field.owner, alias=alias_for(parent_str))
            if field.is_multi and not exclude_through:
                through_name = field.through.get_name()
                through_alias = alias_for(f"{related_str}__{through_name}")
                tables[through_alias] = (
                    f"{parent_str}__{through_name}" if parent_str else through_name,
                    field.through,
                    allowed_columns(
                        model_cls=field.through,
                        excludable=excludable,
                        alias=through_alias,
                        is_excluded=parent_excludable.is_excluded(through_name),
                    ),
                )
            alias = alias_for(related_str)
            tables[alias] = (
                related_str,
                field.to,
                allowed_columns(
                    model_cls=field.to,
                    excludable=excludable,
                    alias=alias,
                    is_excluded=parent_excludable.is_excluded(field.name),
                ),
            )
            current_model, parent_str = field.to, related_str
    return tables


def values_column_map(
    model_cls: type["Model"],
    select_related: list[str],
    excludable: "ExcludableItems",
    exclude_through: bool,
    column_names: list[str],
) -> ValuesColumns:
    """
    Maps prefixed columns of a raw query to relation strings
    (relation names connected with dunders), skipping excluded columns.

    Sample: translates: "t1a2b3c_name" -> into: "posts__user__name"

    :param model_cls: main model of the query
    :type model_cls: type["Model"]
    :param select_related: list of related relation strings joined in the query
    :type select_related: list[str]
    :param excludable: structure of fields to include and exclude
    :type excludable: ExcludableItems
    :param exclude_through: flag if through models should be excluded
    :type exclude_through: bool
    :param column_names: list of column names with prefixes from query
    :type column_names: list[str]
    :return: dictionary of column name to resolved name
    :rtype: dict[str, str]
    """
    tables = joined_table_columns(
        model_cls=model_cls,
        select_related=select_related,
        excludable=excludable,
        exclude_through=exclude_through,
    )
    own_columns = set(
        model_cls.own_table_columns(
            model=model_cls, excludable=excludable, add_pk_columns=False
        )
    )
    names: dict[str, str] = dict()
    key_parts: dict[str, str] = dict()
    for column_name in column_names:
        prefix, _, column = column_name.partition("_")
        if prefix in tables:
            label, table_model, columns = tables[prefix]
            resolved = resolve_column(
                model_cls=table_model, column=column, allowed=columns
            )
            name_prefix = f"{label}__"
        else:
            resolved = resolve_column(
                model_cls=model_cls, column=column_name, allowed=own_columns
            )
            name_prefix = ""
        if resolved is not None:
            names[column_name] = f"{name_prefix}{resolved.name}"
            if resolved.part is not None:
                key_parts[column_name] = resolved.part
    return ValuesColumns(names=names, key_parts=key_parts)


def extract_values(rows: list[Any], columns: ValuesColumns, as_dict: bool) -> list[Any]:
    """
    Extracts the mapped columns of each row, in the order of the query columns,
    as a dictionary keyed by relation string or as a tuple of values.

    Sample: with names {"t1a2b3c_name": "posts__user__name"} a row
    {"id": 1, "t1a2b3c_name": "a"} becomes {"posts__user__name": "a"} or ("a",)

    A foreign key stored in several columns is one value: a dict of its
    target fields (``{"tenant_id": 1, "number": 7}``) or, in tuples, a tuple
    (``(1, 7)``), ``None`` when any of its columns is null.

    Rows of a query whose labels are already the returned names, with no
    column skipped, are copied as a whole.

    :param rows: database rows
    :type rows: list[Any]
    :param columns: returned columns of the query
    :type columns: ValuesColumns
    :param as_dict: return dictionaries instead of tuples
    :type as_dict: bool
    :return: extracted values
    :rtype: list[Any]
    """
    if columns.key_parts:
        return extract_grouped_values(rows=rows, columns=columns, as_dict=as_dict)
    labels = list(columns.names)
    names = list(columns.names.values())
    if as_dict and labels == names == list(rows[0].keys()):
        return [dict(row) for row in rows]
    if len(labels) > 1:
        values = list(map(itemgetter(*labels), rows))
    else:  # itemgetter of a single label returns a bare value, not a tuple
        values = [tuple(row[label] for label in labels) for row in rows]
    if not as_dict:
        return values
    return [dict(zip(names, row_values)) for row_values in values]


def extract_grouped_values(
    rows: list[Any], columns: ValuesColumns, as_dict: bool
) -> list[Any]:
    """
    Extract values of a query returning foreign keys stored in several
    columns, combining their columns into one value (see ``extract_values``).

    :param rows: database rows
    :type rows: list[Any]
    :param columns: returned columns of the query
    :type columns: ValuesColumns
    :param as_dict: return dictionaries instead of tuples
    :type as_dict: bool
    :return: extracted values
    :rtype: list[Any]
    """
    groups: dict[str, list[str]] = dict()
    for label, name in columns.names.items():
        groups.setdefault(name, []).append(label)
    result = []
    for row in rows:
        values = [
            group_value(row=row, labels=labels, columns=columns, as_dict=as_dict)
            for labels in groups.values()
        ]
        result.append(dict(zip(groups, values)) if as_dict else tuple(values))
    return result


def group_value(
    row: Any, labels: list[str], columns: ValuesColumns, as_dict: bool
) -> Any:
    """
    :param row: database row
    :type row: Any
    :param labels: query column labels of one returned name
    :type labels: list[str]
    :param columns: returned columns of the query
    :type columns: ValuesColumns
    :param as_dict: shape a foreign key stored in several columns as a dict
    :type as_dict: bool
    :return: the value of the returned name
    :rtype: Any
    """
    if labels[0] not in columns.key_parts:
        return row[labels[0]]
    parts = tuple(row[label] for label in labels)
    if None in parts:
        return None
    if as_dict:
        return {columns.key_parts[label]: part for label, part in zip(labels, parts)}
    return parts
