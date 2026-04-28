from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Optional, Union

from ormar.exceptions import QueryDefinitionError
from ormar.queryset.utils import (
    PathParts,
    build_flatten_map,
    get_relationship_alias_model_and_str,
    translate_list_to_dict,
)

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model


Slot = Literal["include", "exclude", "flatten"]


def skip_ellipsis(
    items: Union[set, dict, None],
    key: str,
    default: Any = None,
) -> Union[set, dict, None]:
    """
    Descend one level into an include/exclude dict by ``key``, returning
    ``default`` when the lookup yields ``Ellipsis``. Ellipsis at this position
    means "all fields below" rather than a concrete set/dict to recurse into.

    :param items: current include/exclude value
    :type items: Union[set, dict, None]
    :param key: key for nested relations to check
    :type key: str
    :param default: value returned when the lookup yields Ellipsis
    :type default: Any
    :return: nested value of the items
    :rtype: Union[set, dict, None]
    """
    result = items.get(key, {}) if isinstance(items, dict) else items
    return result if result is not Ellipsis else default


def convert_all(items: Union[set, dict, None]) -> Union[set, dict, None]:
    """
    Convert pydantic ``__all__`` special index into the ormar form, which does
    not support index-based exclusions.

    :param items: current include/exclude value
    :type items: Union[set, dict, None]
    :return: items with ``__all__`` unwrapped if present
    :rtype: Union[set, dict, None]
    """
    if isinstance(items, dict) and "__all__" in items:
        return items.get("__all__")
    return items


def normalize_to_dict(items: Union[set, dict, None]) -> Optional[dict]:
    """
    Convert a set form of include/exclude into its dict equivalent, leaving
    dicts and ``None`` unchanged.

    :param items: include/exclude value in any user-accepted form
    :type items: Union[set, dict, None]
    :return: dict form of items, or ``None``
    :rtype: Optional[dict]
    """
    return translate_list_to_dict(items) if isinstance(items, set) else items


def filter_not_excluded_fields(
    fields: Union[list, set],
    include: Optional[dict],
    exclude: Optional[dict],
) -> list:
    """
    Apply include / exclude dicts to a flat field list, returning the field
    names that survive filtering. Mirrors the rules used during ``model_dump``
    serialization.

    :param fields: candidate field names
    :type fields: Union[list, set]
    :param include: fields to include
    :type include: Optional[dict]
    :param exclude: fields to exclude
    :type exclude: Optional[dict]
    :return: list of field names that pass the include/exclude filter
    :rtype: list
    """
    fields = [*fields] if not isinstance(fields, list) else fields
    if include:
        fields = [field for field in fields if field in include]
    if exclude:
        fields = [
            field
            for field in fields
            if field not in exclude
            or (
                exclude.get(field) is not Ellipsis and exclude.get(field) != {"__all__"}
            )
        ]
    return fields


class FlattenMap:
    """
    Nested-dict view of flatten directives with Ellipsis at leaves. Wraps the
    ``Optional[dict]`` shape produced by :class:`ExcludableItems` so the few
    operations that consume it (leaf check, descent, validation against
    include/exclude) live alongside the data.

    A truthy instance means at least one relation will be rendered as its
    primary key.
    """

    __slots__ = ("_data", "_flatten_all")

    def __init__(self, data: Optional[dict] = None, flatten_all: bool = False) -> None:
        self._data = data
        self._flatten_all = flatten_all

    @property
    def data(self) -> Optional[dict]:
        """
        Underlying nested-Ellipsis dict, or ``None`` when only ``flatten_all``
        drives behavior. Exposed primarily for inspection and tests.

        :return: stored nested-Ellipsis dict, or None
        :rtype: Optional[dict]
        """
        return self._data

    def is_field_flattened(self, field: str) -> bool:
        """
        Decide if a relation field should be rendered as its primary key.
        ``flatten_all`` wins globally; otherwise an Ellipsis leaf at ``field``
        triggers flattening.

        :param field: name of the relation field
        :type field: str
        :return: True if the field should be rendered as a pk value
        :rtype: bool
        """
        if self._flatten_all:
            return True
        return self._data is not None and self._data.get(field) is Ellipsis

    def descend(self, field: str) -> Optional["FlattenMap"]:
        """
        Return the flatten sub-map for the nested model reached via ``field``,
        or ``None`` when there is no sub-map. Callers must check
        ``is_field_flattened`` first — ``flatten_all`` instances short-circuit
        there before reaching ``descend``.

        :param field: relation name to descend into
        :type field: str
        :return: nested FlattenMap or None
        :rtype: Optional[FlattenMap]
        """
        value = self._data.get(field) if self._data else None
        if isinstance(value, dict):
            return FlattenMap(data=value)
        return None

    def check_vs_selector(
        self,
        selector: Union[set, dict, None],
        selector_kind: str,
        path: str = "",
    ) -> None:
        """
        Walk the flatten map alongside an include/exclude dict and raise when
        a flattened leaf has sub-field selection on its target.

        ``flatten_all`` alone never produces a conflict because there is no
        explicit per-relation map to compare against.

        :param selector: current level of include / exclude nested dict
        :type selector: Union[set, dict, None]
        :param selector_kind: "include" or "exclude" - used in error message
        :type selector_kind: str
        :param path: dunder path accumulator for the error message
        :type path: str
        """
        if not self._data:
            return
        walk_flatten_vs_selector(self._data, selector, selector_kind, path)

    def __bool__(self) -> bool:
        return self._flatten_all or bool(self._data)


def walk_flatten_vs_selector(
    flatten_map: dict,
    selector: Union[set, dict, None],
    selector_kind: str,
    path: str,
) -> None:
    """
    Recursive worker behind :meth:`FlattenMap.check_vs_selector` — kept at
    module level so the recursion stays on a plain dict and does not allocate
    intermediate :class:`FlattenMap` instances.

    :param flatten_map: current level of the flatten nested dict
    :type flatten_map: dict
    :param selector: current level of include / exclude nested dict
    :type selector: Union[set, dict, None]
    :param selector_kind: "include" or "exclude" - used in error message
    :type selector_kind: str
    :param path: dunder path accumulator for the error message
    :type path: str
    """
    if not selector or not isinstance(selector, dict):
        return
    for key, value in flatten_map.items():
        sel = selector.get(key)
        full_path = f"{path}__{key}" if path else key
        if value is Ellipsis:
            conflicts: Optional[set] = None
            if isinstance(sel, dict):
                conflicts = set(sel.keys()) - {...}
            elif isinstance(sel, set):
                conflicts = {item for item in sel if item is not ...}
            if conflicts:
                raise QueryDefinitionError(
                    f"Flatten conflict: relation '{full_path}' is flattened "
                    f"but {selector_kind} specifies children "
                    f"{sorted(conflicts)}. A flattened relation renders only "
                    f"its primary key and cannot have children selected."
                )
        elif isinstance(value, dict):
            walk_flatten_vs_selector(
                flatten_map=value,
                selector=sel,
                selector_kind=selector_kind,
                path=full_path,
            )


@dataclass
class Excludable:
    """
    Class that keeps sets of fields to include, exclude, and flatten for a single
    model at a given alias/relation level.
    """

    include: set = field(default_factory=set)
    exclude: set = field(default_factory=set)
    flatten: set = field(default_factory=set)

    def get_copy(self) -> "Excludable":
        """
        Return copy of self to avoid in place modifications.

        :return: copy of self with copied sets
        :rtype: ormar.models.excludable.Excludable
        """
        _copy = self.__class__()
        _copy.include = {x for x in self.include}
        _copy.exclude = {x for x in self.exclude}
        _copy.flatten = {x for x in self.flatten}
        return _copy

    def set_values(self, value: set, slot: Slot) -> None:
        """
        Appends the data to the chosen slot (include/exclude/flatten).

        :param value: set of values to add
        :type value: set
        :param slot: which set to add the values to
        :type slot: Slot
        """
        current_value = getattr(self, slot)
        current_value.update(value)
        setattr(self, slot, current_value)

    def is_included(self, key: str) -> bool:
        """
        Check if field in included (in set or set is {...}).

        :param key: key to check
        :type key: str
        :return: result of the check
        :rtype: bool
        """
        return (... in self.include or key in self.include) if self.include else True

    def is_excluded(self, key: str) -> bool:
        """
        Check if field in excluded (in set or set is {...}).

        :param key: key to check
        :type key: str
        :return: result of the check
        :rtype: bool
        """
        return (... in self.exclude or key in self.exclude) if self.exclude else False

    def is_flattened(self, key: str) -> bool:
        """
        Check if relation is flattened (in set or set is {...}).

        :param key: relation name to check
        :type key: str
        :return: result of the check
        :rtype: bool
        """
        return (... in self.flatten or key in self.flatten) if self.flatten else False


class ExcludableItems:
    """
    Keeps a dictionary of Excludables by alias + model_name keys
    to allow quick lookup by nested models without need to travers
    deeply nested dictionaries and passing include/exclude around.
    """

    def __init__(self) -> None:
        self.items: dict[str, Excludable] = dict()
        self._flatten_paths: set[PathParts] = set()
        self._flatten_map_cache: Optional[FlattenMap] = None

    @classmethod
    def from_excludable(cls, other: "ExcludableItems") -> "ExcludableItems":
        """
        Copy passed ExcludableItems to avoid inplace modifications.

        :param other: other excludable items to be copied
        :type other: ormar.models.excludable.ExcludableItems
        :return: copy of other
        :rtype: ormar.models.excludable.ExcludableItems
        """
        new_excludable = cls()
        for key, value in other.items.items():
            new_excludable.items[key] = value.get_copy()
        new_excludable._flatten_paths = set(other._flatten_paths)
        return new_excludable

    def include_entry_count(self) -> int:
        """
        Returns count of include items inside.
        """
        count = 0
        for key in self.items.keys():
            count += len(self.items[key].include)
        return count

    def flatten_map(self) -> Optional[FlattenMap]:
        """
        Return a :class:`FlattenMap` over the stored flatten paths, built
        lazily on first call and cached for reuse. The cache is invalidated
        whenever ``_set_slot`` adds a new flatten path. Returns ``None`` when
        no flatten paths are stored, so callers can short-circuit.

        :return: FlattenMap wrapping the nested-Ellipsis dict, or ``None``
            when no flatten paths are stored
        :rtype: Optional[FlattenMap]
        """
        if not self._flatten_paths:
            return None
        if self._flatten_map_cache is None:
            self._flatten_map_cache = FlattenMap(
                data=build_flatten_map(self._flatten_paths)
            )
        return self._flatten_map_cache

    def get(self, model_cls: type["Model"], alias: str = "") -> Excludable:
        """
        Return Excludable for given model and alias.

        :param model_cls: target model to check
        :type model_cls: ormar.models.metaclass.ModelMetaclass
        :param alias: table alias from relation manager
        :type alias: str
        :return: Excludable for given model and alias
        :rtype: ormar.models.excludable.Excludable
        """
        key = f"{alias + '_' if alias else ''}{model_cls.get_name(lower=True)}"
        excludable = self.items.get(key)
        if not excludable:
            excludable = Excludable()
            self.items[key] = excludable
        return excludable

    def build(
        self,
        items: Union[list[str], str, tuple[str], set[str], dict],
        model_cls: type["Model"],
        slot: Slot = "include",
    ) -> None:
        """
        Receives the one of the types of items and parses them as to achieve
        a end situation with one excludable per alias/model in relation.

        Each excludable has three sets of values - include, exclude, and flatten.

        :param items: values to be included, excluded or flattened
        :type items: Union[list[str], str, tuple[str], set[str], dict]
        :param model_cls: source model from which relations are constructed
        :type model_cls: ormar.models.metaclass.ModelMetaclass
        :param slot: which slot to write parsed values into
        :type slot: Slot
        """
        if isinstance(items, str):
            items = {items}

        if isinstance(items, dict):
            self._traverse_dict(
                values=items,
                source_model=model_cls,
                model_cls=model_cls,
                slot=slot,
            )
        else:
            items = set(items)
            nested_items = set(x for x in items if "__" in x)
            items.difference_update(nested_items)
            if items:
                self._set_slot(
                    items=items,
                    model_cls=model_cls,
                    slot=slot,
                )
            if nested_items:
                self._traverse_list(values=nested_items, model_cls=model_cls, slot=slot)

        if slot == "flatten":
            self._validate_flatten_prefix_collisions()

    def _set_slot(
        self,
        items: set,
        model_cls: type["Model"],
        slot: Slot,
        alias: str = "",
        path_parts: PathParts = (),
    ) -> None:
        """
        Sets set of values to be stored for the given slot on the key that
        corresponds to the passed model + alias.

        :param items: items to write
        :type items: set
        :param model_cls: target model on which the items are stored
        :type model_cls: type[Model]
        :param slot: which slot to write to
        :type slot: Slot
        :param alias: table alias from relation manager
        :type alias: str
        :param path_parts: tuple of dunder path segments leading to this model
        :type path_parts: PathParts
        """
        if slot == "flatten":
            self._validate_flatten_leaves(
                items=items, model_cls=model_cls, path_parts=path_parts
            )
            for item in items:
                self._flatten_paths.add(path_parts + (item,))
            self._flatten_map_cache = None

        key = f"{alias + '_' if alias else ''}{model_cls.get_name(lower=True)}"
        excludable = self.items.setdefault(key, Excludable())
        excludable.set_values(value=items, slot=slot)

    def _traverse_dict(  # noqa: CFQ002
        self,
        values: dict,
        source_model: type["Model"],
        model_cls: type["Model"],
        slot: Slot,
        path_parts: PathParts = (),
        alias: str = "",
    ) -> None:
        """
        Goes through dict of nested values and construct/update Excludables.

        :param values: items to include/exclude/flatten
        :type values: dict
        :param source_model: source model from which relations are constructed
        :type source_model: ormar.models.metaclass.ModelMetaclass
        :param model_cls: model reached via ``path_parts``
        :type model_cls: ormar.models.metaclass.ModelMetaclass
        :param slot: which slot to write into
        :type slot: Slot
        :param path_parts: tuple of dunder path segments leading to ``model_cls``
        :type path_parts: PathParts
        :param alias: alias of relation
        :type alias: str
        """
        self_fields = set()
        for key, value in values.items():
            if value is ...:
                self_fields.add(key)
            elif isinstance(value, set):
                nested_parts = path_parts + (key,)
                table_prefix, target_model, _, _ = get_relationship_alias_model_and_str(
                    source_model=source_model,
                    related_parts=list(nested_parts),
                )
                self._set_slot(
                    items=value,
                    model_cls=target_model,
                    slot=slot,
                    alias=table_prefix,
                    path_parts=nested_parts,
                )
            else:
                nested_parts = path_parts + (key,)
                table_prefix, target_model, _, _ = get_relationship_alias_model_and_str(
                    source_model=source_model,
                    related_parts=list(nested_parts),
                )
                self._traverse_dict(
                    values=value,
                    source_model=source_model,
                    model_cls=target_model,
                    slot=slot,
                    path_parts=nested_parts,
                    alias=table_prefix,
                )
        if self_fields:
            self._set_slot(
                items=self_fields,
                model_cls=model_cls,
                slot=slot,
                alias=alias,
                path_parts=path_parts,
            )

    def _traverse_list(
        self, values: set[str], model_cls: type["Model"], slot: Slot
    ) -> None:
        """
        Consume a set of dunder-style paths (``"a__b__c"``) and write each leaf
        to the Excludable for its target model/alias. Each path is split once
        into a tuple and threaded through ``_set_slot`` — no further string
        joins happen downstream.

        :param values: set of dunder-style path strings
        :type values: set[str]
        :param model_cls: source model from which relations are resolved
        :type model_cls: type[Model]
        :param slot: which slot to write into
        :type slot: Slot
        """
        for dunder_path in values:
            parts = tuple(dunder_path.split("__"))
            table_prefix, target_model, _, _ = get_relationship_alias_model_and_str(
                source_model=model_cls,
                related_parts=list(parts[:-1]),
            )
            self._set_slot(
                items={parts[-1]},
                model_cls=target_model,
                slot=slot,
                alias=table_prefix,
                path_parts=parts[:-1],
            )

    @staticmethod
    def _validate_flatten_leaves(
        items: set, model_cls: type["Model"], path_parts: PathParts
    ) -> None:
        """
        Ensure every leaf addressed by a flatten spec is a real relation on the
        target model (not a scalar column and not a through model).

        :param items: set of leaf names being flattened on ``model_cls``
        :type items: set
        :param model_cls: target model on which leaves must resolve to relations
        :type model_cls: type[Model]
        :param path_parts: tuple of segments leading to ``model_cls`` (only
            joined for error messages, never parsed)
        :type path_parts: PathParts
        """
        model_fields = model_cls.ormar_config.model_fields
        for item in items:
            related_field = model_fields.get(item)
            if related_field is None:
                raise QueryDefinitionError(
                    f"Unknown relation '{item}' on model "
                    f"{model_cls.get_name(lower=False)} in flatten_fields path "
                    f"'{join_path(path_parts, item)}'."
                )
            if getattr(related_field, "is_through", False):
                raise QueryDefinitionError(
                    f"Cannot flatten through model '{item}' at path "
                    f"'{join_path(path_parts, item)}'. Flatten the many-to-many "
                    f"relation itself instead."
                )
            if not getattr(related_field, "is_relation", False):
                raise QueryDefinitionError(
                    f"flatten_fields target '{join_path(path_parts, item)}' is "
                    f"not a relation on model {model_cls.get_name(lower=False)}. "
                    f"Only foreign keys, many-to-many, and reverse relations can "
                    f"be flattened."
                )

    def _validate_flatten_prefix_collisions(self) -> None:
        """
        Ensure no flatten path is a strict ancestor of another. Flattening a
        relation replaces it with a scalar PK, so a deeper flatten through the
        same chain is unreachable.

        Works directly on tuple paths — sorted so each prefix's descendants
        follow it immediately, enabling an early break once the prefix no
        longer matches.

        :raises QueryDefinitionError: on any prefix collision
        """
        paths = sorted(self._flatten_paths)
        for i, short in enumerate(paths):
            short_len = len(short)
            for longer in paths[i + 1 :]:
                if longer[:short_len] != short:
                    break
                raise QueryDefinitionError(
                    f"Conflicting flatten directives: "
                    f"'{join_path(short)}' is flattened to its primary key, "
                    f"so nested flatten '{join_path(longer)}' is unreachable."
                )

    def validate_flatten_vs_excludable(self, source_model: type["Model"]) -> None:
        """
        Ensure no flattened relation has sub-field include/exclude on its target.
        Whole-relation include/exclude at the parent level is allowed; only
        sub-field selection on the flattened child is flagged.

        :param source_model: model from which flatten paths are rooted
        :type source_model: type[Model]
        :raises QueryDefinitionError: when a flattened relation has sub-field
            include/exclude on its target model
        """
        for parts in self._flatten_paths:
            table_prefix, target_model, _, _ = get_relationship_alias_model_and_str(
                source_model=source_model,
                related_parts=list(parts),
            )
            child_key = (
                f"{table_prefix + '_' if table_prefix else ''}"
                f"{target_model.get_name(lower=True)}"
            )
            child = self.items.get(child_key)
            if not child:
                continue
            conflicts = (child.include | child.exclude) - {...}
            if conflicts:
                raise QueryDefinitionError(
                    f"Flatten conflict: relation '{join_path(parts)}' is "
                    f"flattened but include/exclude specifies children "
                    f"{sorted(conflicts)}. A flattened relation renders only "
                    f"its primary key and cannot have children selected."
                )


def join_path(parts: PathParts, tail: Optional[str] = None) -> str:
    """
    Build a user-facing dunder path string from a pre-split tuple, optionally
    appending one extra segment. Used only for error messages — the rest of
    the module keeps paths as tuples.

    :param parts: pre-split path segments
    :type parts: PathParts
    :param tail: optional additional segment to append
    :type tail: Optional[str]
    :return: dunder-joined path string
    :rtype: str
    """
    if tail is None:
        return "__".join(parts)
    return "__".join(parts + (tail,)) if parts else tail
