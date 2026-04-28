import collections.abc
import copy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable, Optional, Union

from ormar.exceptions import QueryDefinitionError

if TYPE_CHECKING:  # pragma no cover
    from ormar import BaseField, Model


PathParts = tuple[str, ...]


def build_flatten_map(paths: Iterable[PathParts]) -> dict:
    """
    Build a nested-Ellipsis dict from pre-split flatten path tuples — the
    runtime representation threaded through ``model_dump`` recursion.

    Example::

        paths = [
            ("author", "name"),
            ("author", "email"),
            ("category", "title"),
        ]
        build_flatten_map(paths) == {
            "author": {"name": ..., "email": ...},
            "category": {"title": ...},
        }

    :param paths: iterable of tuple paths (each path already split on ``__``)
    :type paths: Iterable[PathParts]
    :return: nested dict where leaves are ``...``
    :rtype: dict
    """
    result: dict = {}
    for parts in paths:
        current = result
        last = len(parts) - 1
        for i, part in enumerate(parts):
            if i == last:
                current[part] = ...
            else:
                current = current.setdefault(part, {})
    return result


def extract_access_chains(value: Any) -> Any:
    """
    Unwrap ``FieldAccessor`` inputs (or lists that contain them) into their
    underlying dunder-path strings so downstream parsers see a uniform shape.
    Anything that isn't an accessor (or list of accessors) is returned
    unchanged — sets, tuples, dicts, and plain strings all pass through.

    :param value: user input for a relation-spec method (``select_related``,
        ``prefetch_related``, ``flatten_fields``)
    :type value: Any
    :return: a dunder string, a list with each accessor replaced by its chain,
        or the original value unchanged
    :rtype: Any
    """
    # Late import to avoid circular dependency with queryset package.
    from ormar.queryset.field_accessor import FieldAccessor

    if isinstance(value, FieldAccessor):
        return [value._access_chain]
    if isinstance(value, list):
        return [
            item._access_chain if isinstance(item, FieldAccessor) else item
            for item in value
        ]
    return value


@dataclass(frozen=True)
class SliceBounds:
    """
    Normalized pagination parameters produced by :func:`normalize_slice`.

    ``reverse=True`` signals that the ORDER BY directions should be flipped
    at query build time and the fetched list reversed in memory so the
    caller still sees rows in the original ordering. This is how negative
    indices and negative slice bounds are emulated without a ``COUNT(*)``
    round-trip.

    :ivar limit: row count for ``LIMIT``; ``None`` means "no limit"
    :vartype limit: Optional[int]
    :ivar offset: row count for ``OFFSET``; always non-negative
    :vartype offset: int
    :ivar reverse: whether the query ordering must be flipped and the
        result list reversed in memory
    :vartype reverse: bool
    """

    limit: Optional[int]
    offset: int
    reverse: bool


def normalize_slice(key: Union[int, slice]) -> SliceBounds:
    """
    Top-level dispatcher: turns a Python integer index or slice into a
    :class:`SliceBounds` suitable for ``QuerySet``.

    Delegates to a dedicated helper for each shape. Any shape that would
    require a ``COUNT(*)`` round-trip (``step != 1``, a bare ``[:-N]``, or
    mixed-sign bounds) raises ``QueryDefinitionError``.

    Examples::

        5           → SliceBounds(limit=1, offset=5,  reverse=False)
        slice(2, 8) → SliceBounds(limit=6, offset=2,  reverse=False)
        slice(-3, None) → SliceBounds(limit=3, offset=0, reverse=True)

    :param key: integer or slice passed to ``QuerySet.__getitem__``
    :type key: int | slice
    :return: normalized slice parameters
    :rtype: SliceBounds
    """
    if isinstance(key, int):
        return _int_to_limit_offset(key)
    if isinstance(key, slice):
        return _slice_to_limit_offset(key)
    raise QueryDefinitionError(
        f"QuerySet indices must be integers or slices, not {type(key).__name__}."
    )


def _int_to_limit_offset(key: int) -> SliceBounds:
    """
    Handles ``Model.objects[N]`` and ``Model.objects[-N]`` — a single-row
    pick from the head or tail of the result. Negative indices are emulated
    by flipping the ORDER BY and offsetting ``|N|-1`` rows from the end.

    Examples::

        [0]  → SliceBounds(limit=1, offset=0, reverse=False)
        [5]  → SliceBounds(limit=1, offset=5, reverse=False)
        [-1] → SliceBounds(limit=1, offset=0, reverse=True)
        [-3] → SliceBounds(limit=1, offset=2, reverse=True)

    :param key: non-negative index from the head, or negative from the tail
    :type key: int
    :return: ``SliceBounds(limit=1, offset=..., reverse=...)``
    :rtype: SliceBounds
    """
    if key >= 0:
        return SliceBounds(limit=1, offset=key, reverse=False)
    return SliceBounds(limit=1, offset=-key - 1, reverse=True)


def _slice_to_limit_offset(key: slice) -> SliceBounds:
    """
    Dispatches a ``slice`` object to the shape-specific helper: open on both
    ends, open-start, open-stop, or fully bounded. Validates ``step`` up
    front because it is the only constraint that applies to every shape.

    Examples::

        [:]     → SliceBounds(limit=None, offset=0, reverse=False)
        [:8]    → _slice_head(8)
        [3:]    → _slice_tail(3)
        [2:8]   → _slice_range(2, 8)
        [::2]   → raises QueryDefinitionError (step != 1)

    :param key: Python slice passed to ``__getitem__``
    :type key: slice
    :return: normalized slice parameters
    :rtype: SliceBounds
    """
    if key.step is not None and key.step != 1:
        raise QueryDefinitionError(f"Slice step {key.step} is not supported, only 1.")

    start, stop = key.start, key.stop
    if start is None and stop is None:
        return SliceBounds(limit=None, offset=0, reverse=False)
    if start is None:
        return _slice_head(stop)
    if stop is None:
        return _slice_tail(start)
    return _slice_range(start, stop)


def _slice_head(stop: int) -> SliceBounds:
    """
    Handles ``Model.objects[:N]`` — the first ``N`` rows of the result set.
    Negative ``stop`` would mean "everything except the last ``|N|`` rows",
    which needs a ``COUNT(*)`` to resolve, so it is rejected.

    Examples::

        [:5]  → SliceBounds(limit=5, offset=0, reverse=False)
        [:0]  → SliceBounds(limit=0, offset=0, reverse=False)
        [:-2] → raises QueryDefinitionError

    :param stop: upper bound from the slice
    :type stop: int
    :return: ``SliceBounds(limit=stop, offset=0, reverse=False)``
    :rtype: SliceBounds
    """
    if stop < 0:
        raise QueryDefinitionError(
            "Negative slice stop without a start requires an explicit count; "
            "use .count() with .offset()/.limit() instead."
        )
    return SliceBounds(limit=stop, offset=0, reverse=False)


def _slice_tail(start: int) -> SliceBounds:
    """
    Handles ``Model.objects[N:]`` and ``Model.objects[-N:]`` — everything
    from ``N`` onwards, or the last ``|N|`` rows. The negative case is
    emulated by flipping the ORDER BY and taking the first ``|N|`` rows.

    Examples::

        [3:]  → SliceBounds(limit=None, offset=3, reverse=False)
        [-5:] → SliceBounds(limit=5,    offset=0, reverse=True)
        [-1:] → SliceBounds(limit=1,    offset=0, reverse=True)

    :param start: lower bound from the slice
    :type start: int
    :return: normalized slice parameters; ``limit`` is ``None`` for the
        positive case (no upper bound)
    :rtype: SliceBounds
    """
    if start < 0:
        return SliceBounds(limit=-start, offset=0, reverse=True)
    return SliceBounds(limit=None, offset=start, reverse=False)


def _slice_range(start: int, stop: int) -> SliceBounds:
    """
    Handles ``Model.objects[A:B]`` with both bounds set. Mixed-sign bounds
    (e.g. ``[3:-2]``) need a ``COUNT(*)`` to resolve so they are rejected;
    otherwise the sign of the bounds picks the forward or reverse variant.

    Examples::

        [2:8]   → _forward_range(2, 8)
        [-5:-2] → _reverse_range(-5, -2)
        [3:-2]  → raises QueryDefinitionError (mixed sign)

    :param start: lower bound from the slice
    :type start: int
    :param stop: upper bound from the slice
    :type stop: int
    :return: normalized slice parameters
    :rtype: SliceBounds
    """
    start_neg = start < 0
    stop_neg = stop < 0
    if start_neg != stop_neg:
        raise QueryDefinitionError(
            "Mixed positive and negative slice bounds are not supported "
            "without an explicit count; use .count() with "
            ".offset()/.limit() instead."
        )
    if start_neg:
        return _reverse_range(start, stop)
    return _forward_range(start, stop)


def _forward_range(start: int, stop: int) -> SliceBounds:
    """
    Handles ``[A:B]`` with ``A, B >= 0``. Out-of-order bounds (``A > B``)
    clamp to an empty result to match Python list semantics, rather than
    emitting a nonsensical negative ``LIMIT``.

    Examples::

        [2:8] → SliceBounds(limit=6, offset=2, reverse=False)
        [0:3] → SliceBounds(limit=3, offset=0, reverse=False)
        [8:2] → SliceBounds(limit=0, offset=8, reverse=False)  # empty

    :param start: non-negative lower bound
    :type start: int
    :param stop: non-negative upper bound
    :type stop: int
    :return: ``SliceBounds(limit=max(stop-start, 0), offset=start,
        reverse=False)``
    :rtype: SliceBounds
    """
    return SliceBounds(limit=max(stop - start, 0), offset=start, reverse=False)


def _reverse_range(start: int, stop: int) -> SliceBounds:
    """
    Handles ``[-A:-B]`` (both bounds negative). Out-of-order bounds
    (``start >= stop``, e.g. ``[-2:-5]``) clamp to empty; otherwise the
    slice is emulated by flipping the ORDER BY, offsetting ``|stop|`` rows
    from the end, and limiting to ``|start| - |stop|`` rows.

    Examples::

        [-5:-2] → SliceBounds(limit=3, offset=2, reverse=True)
        [-3:-1] → SliceBounds(limit=2, offset=1, reverse=True)
        [-2:-5] → SliceBounds(limit=0, offset=0, reverse=True)  # empty

    :param start: negative lower bound
    :type start: int
    :param stop: negative upper bound
    :type stop: int
    :return: ``SliceBounds(..., reverse=True)``
    :rtype: SliceBounds
    """
    if start >= stop:
        return SliceBounds(limit=0, offset=0, reverse=True)
    return SliceBounds(limit=stop - start, offset=-stop, reverse=True)


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
