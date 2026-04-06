import collections.abc
import copy
from typing import TYPE_CHECKING, Any, Optional, Union

if TYPE_CHECKING:  # pragma no cover
    from ormar import BaseField, Model

try:
    from typing import Literal  # type: ignore
except ImportError:  # pragma: no cover
    from typing_extensions import Literal  # type: ignore


def check_node_not_dict_or_not_last_node(
    part: str, is_last: bool, current_level: Any
) -> bool:
    """
    Checks if given name is not present in the current level of the structure.
    Checks if given name is not the last name in the split list of parts.
    Checks if the given name in current level is not a dictionary.

    All those checks verify if there is a need for deeper traversal.

    :param part:
    :type part: str
    :param is_last: flag to check if last element
    :type is_last: bool
    :param current_level: current level of the traversed structure
    :type current_level: Any
    :return: result of the check
    :rtype: bool
    """
    return (part not in current_level and not is_last) or (
        part in current_level and not isinstance(current_level[part], dict)
    )


def translate_list_to_dict(  # noqa: CCR001
    list_to_trans: Union[list, set], default: Any = ...
) -> dict:
    """
    Splits the list of strings by '__' and converts them to dictionary with nested
    models grouped by parent model. That way each model appears only once in the whole
    dictionary and children are grouped under parent name.

    Default required key ise Ellipsis like in pydantic.

    :param list_to_trans: input list
    :type list_to_trans: Union[list, set]
    :param default: value to use as a default value
    :type default: Any
    :param is_order: flag if change affects order_by clauses are they require special
    default value with sort order.
    :type is_order: bool
    :return: converted to dictionary input list
    :rtype: dict
    """
    new_dict: dict = dict()
    for path in list_to_trans:
        current_level = new_dict
        parts = path.split("__")
        def_val: Any = copy.deepcopy(default)
        for ind, part in enumerate(parts):
            is_last = ind == len(parts) - 1
            if check_node_not_dict_or_not_last_node(
                part=part, is_last=is_last, current_level=current_level
            ):
                current_level[part] = dict()
            elif part not in current_level:
                current_level[part] = def_val
            current_level = current_level[part]
    return new_dict


def convert_set_to_required_dict(set_to_convert: set) -> dict:
    """
    Converts set to dictionary of required keys.
    Required key is Ellipsis.

    :param set_to_convert: set to convert to dict
    :type set_to_convert: set
    :return: set converted to dict of ellipsis
    :rtype: dict
    """
    new_dict = dict()
    for key in set_to_convert:
        new_dict[key] = Ellipsis
    return new_dict


def update(current_dict: Any, updating_dict: Any) -> dict:  # noqa: CCR001
    """
    Update one dict with another but with regard for nested keys.

    That way nested sets are unionised, dicts updated and
    only other values are overwritten.

    :param current_dict: dict to update
    :type current_dict: dict[str, ellipsis]
    :param updating_dict: dict with values to update
    :type updating_dict: dict
    :return: combination of both dicts
    :rtype: dict
    """
    if current_dict is Ellipsis:
        current_dict = dict()
    for key, value in updating_dict.items():
        if isinstance(value, collections.abc.Mapping):
            old_key = current_dict.get(key, {})
            if isinstance(old_key, set):
                old_key = convert_set_to_required_dict(old_key)
            current_dict[key] = update(old_key, value)
        elif isinstance(value, set) and isinstance(current_dict.get(key), set):
            current_dict[key] = current_dict.get(key).union(value)
        else:
            current_dict[key] = value
    return current_dict


def subtract_dict(current_dict: Any, updating_dict: Any) -> dict:  # noqa: CCR001
    """
    Update one dict with another but with regard for nested keys.

    That way nested sets are unionised, dicts updated and
    only other values are overwritten.

    :param current_dict: dict to update
    :type current_dict: dict[str, ellipsis]
    :param updating_dict: dict with values to update
    :type updating_dict: dict
    :return: combination of both dicts
    :rtype: dict
    """
    for key, value in updating_dict.items():
        old_key = current_dict.get(key, {})
        new_value: Optional[Union[dict, set]] = None
        if not old_key:
            continue
        if isinstance(value, set) and isinstance(old_key, set):
            new_value = old_key.difference(value)
        elif isinstance(value, (set, collections.abc.Mapping)) and isinstance(
            old_key, (set, collections.abc.Mapping)
        ):
            value = (
                convert_set_to_required_dict(value)
                if not isinstance(value, collections.abc.Mapping)
                else value
            )
            old_key = (
                convert_set_to_required_dict(old_key)
                if not isinstance(old_key, collections.abc.Mapping)
                else old_key
            )
            new_value = subtract_dict(old_key, value)

        if new_value:
            current_dict[key] = new_value
        else:
            current_dict.pop(key, None)
    return current_dict


def update_dict_from_list(curr_dict: dict, list_to_update: Union[list, set]) -> dict:
    """
    Converts the list into dictionary and later performs special update, where
    nested keys that are sets or dicts are combined and not overwritten.

    :param curr_dict: dict to update
    :type curr_dict: dict
    :param list_to_update: list with values to update the dict
    :type list_to_update: list[str]
    :return: updated dict
    :rtype: dict
    """
    updated_dict = copy.copy(curr_dict)
    dict_to_update = translate_list_to_dict(list_to_update)
    update(updated_dict, dict_to_update)
    return updated_dict


def get_relationship_alias_model_and_str(
    source_model: type["Model"], related_parts: list
) -> tuple[str, type["Model"], str, bool]:
    """
    Walks the relation to retrieve the actual model on which the clause should be
    constructed, extracts alias based on last relation leading to target model.
    :param related_parts: list of related names extracted from string
    :type related_parts: Union[list, list[str]]
    :param source_model: model from which relation starts
    :type source_model: type[Model]
    :return: table prefix, target model and relation string
    :rtype: tuple[str, type["Model"], str]
    """
    table_prefix = ""
    is_through = False
    target_model = source_model
    previous_model = target_model
    previous_models = [target_model]
    manager = target_model.ormar_config.alias_manager
    for relation in related_parts[:]:
        related_field = target_model.ormar_config.model_fields[relation]

        if related_field.is_through:
            previous_model, relation, is_through = _process_through_field(
                related_parts=related_parts,
                relation=relation,
                related_field=related_field,
                previous_model=previous_model,
                previous_models=previous_models,
            )
        if related_field.is_multi:
            previous_model = related_field.through
            relation = related_field.default_target_field_name()  # type: ignore
        table_prefix = manager.resolve_relation_alias(
            from_model=previous_model, relation_name=relation
        )
        target_model = related_field.to
        previous_model = target_model
        if not is_through:
            previous_models.append(previous_model)
    relation_str = "__".join(related_parts)

    return table_prefix, target_model, relation_str, is_through


def _process_through_field(
    related_parts: list,
    relation: Optional[str],
    related_field: "BaseField",
    previous_model: type["Model"],
    previous_models: list[type["Model"]],
) -> tuple[type["Model"], Optional[str], bool]:
    """
    Helper processing through models as they need to be treated differently.

    :param related_parts: split relation string
    :type related_parts: list[str]
    :param relation: relation name
    :type relation: str
    :param related_field: field with relation declaration
    :type related_field: "ForeignKeyField"
    :param previous_model: model from which relation is coming
    :type previous_model: type["Model"]
    :param previous_models: list of already visited models in relation chain
    :type previous_models: list[type["Model"]]
    :return: previous_model, relation, is_through
    :rtype: tuple[type["Model"], str, bool]
    """
    is_through = True
    related_parts.remove(relation)
    through_field = related_field.owner.ormar_config.model_fields[
        related_field.related_name or ""
    ]
    if len(previous_models) > 1 and previous_models[-2] == through_field.to:
        previous_model = through_field.to
        relation = through_field.related_name
    else:
        relation = related_field.related_name
    return previous_model, relation, is_through


def _int_limit_offset(key: int) -> Tuple[Literal[1], int]:
    """
    Returned the `Limit` & `Offset` Calculated for Integer Object.

    :param key: integer number in `__getitem__`
    :type key: int
    :return: limit_count, offset
    :rtype: Tuple[Optional[int], Optional[int]]
    """

    if key < 0:
        raise ValueError("Negative indexing is not supported.")

    return 1, key


def _slice_limit_offset(key: slice) -> Tuple[Optional[int], Optional[int]]:
    """
    Returned the `Limit` & `Offset` Calculated for Slice Object.

    :param key: slice object in `__getitem__`
    :type key: slice
    :return: limit_count, offset
    :rtype: Tuple[Optional[int], Optional[int]]
    """

    if key.step is not None and key.step != 1:
        raise ValueError(f"{key.step} steps are not supported, only one.")

    start, stop = key.start is not None, key.stop is not None
    if (start and key.start < 0) or (stop and key.stop < 0):
        raise ValueError("The selected range is not valid.")

    limit_count: Optional[int] = max(key.stop - (key.start or 0), 0) if stop else None
    return limit_count, key.start


def get_limit_offset(key: Union[int, slice]) -> Tuple[Optional[int], Optional[int]]:
    """Utility to Select Limit Offset Function by `key` Type Slice or Integer"""

    func = _int_limit_offset if isinstance(key, int) else _slice_limit_offset
    return func(key=key)  # type: ignore
