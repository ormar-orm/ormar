"""
Prototype of relation traversal without a precomputed per-class relation map.

``model_dump()``, ``save_related()`` and ``select_all(follow=True)`` need to know
which relations to follow so the bidirectional relations of ormar do not loop.
Instead of the cached ``_relation_tree()`` of the class they can use a
``PathRelationMap``: a view of one node of the relation tree that is computed
from the models on the current path only, while the loaded instances are
walked. Nothing is cached on the class and nothing outlives the walk.

The mode is selected with the ``ORMAR_RELATION_TRAVERSAL`` environment variable
(``map``, the default, ``instance`` or ``instance_cached``) or with
``set_relation_traversal``.
"""

import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ormar.models.relation_graph import RelationGraph

if TYPE_CHECKING:  # pragma: no cover
    from ormar.models.mixins.relation_mixin import RelationMixin

RELATION_TRAVERSAL_MODES = ("map", "instance", "instance_cached")
DEFAULT_FOLLOW_DEPTH = 3


class RelationTraversalSettings:
    """
    Holds the relation traversal mode of the process::

        RelationTraversalSettings.mode  # -> "map"
    """

    mode: str = os.environ.get("ORMAR_RELATION_TRAVERSAL", "map")


def set_relation_traversal(mode: str) -> None:
    """
    Selects how relations are traversed by ``model_dump``, ``save_related``,
    ``get_pydantic`` and ``select_all(follow=True)``::

        set_relation_traversal(mode="instance")

    :param mode: ``map`` for the cached relation tree, ``instance`` for the
        path based traversal of the loaded instances, ``instance_cached`` for
        the same traversal keeping the walked nodes on the class
    :type mode: str
    :raises ValueError: on an unknown mode
    """
    if mode not in RELATION_TRAVERSAL_MODES:
        raise ValueError(f"Unknown relation traversal mode: {mode}")
    RelationTraversalSettings.mode = mode


def instance_traversal_enabled() -> bool:
    """
    Checks if the path based instance traversal is enabled.

    :return: True in ``instance`` and ``instance_cached`` mode
    :rtype: bool
    """
    return RelationTraversalSettings.mode != "map"


def visited_paths_cached() -> bool:
    """
    Checks if the nodes of the path based traversal are kept on the class
    between dumps (``instance_cached`` mode): the part of the relation tree
    that was actually walked is then built once, at the cost of memory that
    grows with the number of distinct paths dumped.

    :return: True in ``instance_cached`` mode
    :rtype: bool
    """
    return RelationTraversalSettings.mode == "instance_cached"


@dataclass(frozen=True)
class RelationIndex:
    """
    Followable relations of one model class: the target of every relation by
    name (in name order) and the relation names by target model.
    """

    targets: dict[str, Any]
    names_by_target: dict[Any, tuple[str, ...]]


def relation_index(model: type["RelationMixin"]) -> RelationIndex:
    """
    Returns the followable relations of a model, leaving out relations to
    unresolved forward references. Kept on the class (one entry per relation)
    until the relation graph changes::

        relation_index(model=Book).targets  # -> {"author": Author, "tags": Tag}

    :param model: model class
    :type model: type[RelationMixin]
    :return: relation targets of the model
    :rtype: RelationIndex
    """
    cached = model.__dict__.get("_relation_index")
    if cached is not None and cached[0] == RelationGraph.version:
        return cached[1]
    fields = model.ormar_config.model_fields
    targets = {
        relation: fields[relation].to
        for relation in sorted(model.extract_related_names())
        if not fields[relation].has_unresolved_forward_refs()
    }
    names_by_target: dict[Any, tuple[str, ...]] = {}
    for relation, target in targets.items():
        names_by_target[target] = (*names_by_target.get(target, ()), relation)
    index = RelationIndex(targets=targets, names_by_target=names_by_target)
    model._relation_index = (RelationGraph.version, index)  # type: ignore[attr-defined]
    return index


class PathRelationMap(Mapping[str, Any]):
    """
    One node of the relation tree of a model, computed from the model classes
    on the current path: a relation is followed unless its target model is
    already on the path above the model (a direct self relation is followed
    once) or it points to an unresolved forward reference. It answers the
    questions the dump and save code asks a relation map (``relation in
    node``, ``node[relation]``, ``bool(node)``) without building the tree; a
    child node is created only when the walk descends into a loaded related
    instance, and a leaf is ``...`` like in the cached tree::

        node = PathRelationMap(model=Author, ancestors=())
        "books" in node  # -> True
        node["books"]  # -> PathRelationMap(author->book), without "author"
    """

    __slots__ = ("model", "ancestors", "followed", "children")

    def __init__(
        self,
        model: type["RelationMixin"],
        ancestors: tuple[type["RelationMixin"], ...],
    ) -> None:
        self.model = model
        self.ancestors = ancestors
        index = relation_index(model=model)
        blocked = [
            relation
            for ancestor in ancestors
            for relation in index.names_by_target.get(ancestor, ())
        ]
        # the relations of the class are shared when nothing is cut off and
        # never modified
        followed = index.targets
        if blocked:
            followed = dict(followed)
            for relation in blocked:
                followed.pop(relation, None)
        self.followed = followed
        self.children: dict[str, Any] = {}

    def __contains__(self, relation: object) -> bool:
        return relation in self.followed

    def __getitem__(self, relation: str) -> Any:
        child = self.children.get(relation)
        if child is None:
            node = PathRelationMap(
                model=self.followed[relation], ancestors=(*self.ancestors, self.model)
            )
            child = node if node.followed else Ellipsis
            self.children[relation] = child
        return child

    def get(self, relation: str, default: Any = None) -> Any:
        """
        Returns the child node of a relation or ``default`` when the relation
        is not followed.

        :param relation: relation name
        :type relation: str
        :param default: value returned for a relation that is not followed
        :type default: Any
        :return: child node or default
        :rtype: Any
        """
        return self[relation] if relation in self.followed else default

    def __iter__(self) -> Iterator[str]:
        return iter(self.followed)

    def __len__(self) -> int:
        return len(self.followed)

    def __bool__(self) -> bool:
        return bool(self.followed)

    def __repr__(self) -> str:  # pragma: no cover
        path = "->".join(model.get_name() for model in (*self.ancestors, self.model))
        return f"PathRelationMap({path})"


def relation_paths(
    model: type["RelationMixin"],
    max_depth: int,
    ancestors: tuple[type["RelationMixin"], ...] = (),
) -> list[str]:
    """
    Returns the ``select_related`` paths of every relation reachable from a
    model, at most ``max_depth`` relations deep, with the same rule as
    ``PathRelationMap``. Only the deepest paths are listed, as
    ``select_related`` loads the models along a path anyway::

        relation_paths(model=Book, max_depth=2)  # -> ["author__country", "tags"]

    :param model: model class to start from
    :type model: type[RelationMixin]
    :param max_depth: number of relations to follow at most
    :type max_depth: int
    :param ancestors: model classes on the path above the model
    :type ancestors: tuple[type[RelationMixin], ...]
    :return: relation paths in dunder notation
    :rtype: list[str]
    """
    paths: list[str] = []
    if max_depth <= 0:
        return paths
    for relation, target in PathRelationMap(
        model=model, ancestors=ancestors
    ).followed.items():
        nested = relation_paths(
            model=target, max_depth=max_depth - 1, ancestors=(*ancestors, model)
        )
        paths.extend(f"{relation}__{path}" for path in nested)
        if not nested:
            paths.append(relation)
    return paths
