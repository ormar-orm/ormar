"""
Maps prefixed columns of a raw query to the relation string keys returned by
``values()`` and ``values_list()``.
"""

from typing import TYPE_CHECKING, cast

from ormar.relations.aliases import alias_for

if TYPE_CHECKING:  # pragma: no cover
    from ormar import ForeignKeyField, Model
    from ormar.models.excludable import ExcludableItems


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
) -> dict[str, tuple[str, set[str]]]:
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
    tables: dict[str, tuple[str, set[str]]] = dict()
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
) -> dict[str, str]:
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
    column_map: dict[str, str] = dict()
    for column_name in column_names:
        prefix, _, column = column_name.partition("_")
        if prefix in tables:
            label, columns = tables[prefix]
            if column in columns:
                column_map[column_name] = f"{label}__{column}"
        elif column_name in own_columns:
            column_map[column_name] = column_name
    return column_map
