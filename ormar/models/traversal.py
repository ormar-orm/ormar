"""
Relation traversal without a precomputed per-class relation map.

``model_dump()``, ``save_related()`` and ``select_all(follow=True)`` need to know
which relations to follow so the bidirectional relations of ormar do not loop.
Instead of the cached ``_relation_tree()`` of the class they use a
``PathRelationMap``: one node of the relation tree, computed from the models on
the current path only, while the loaded instances are walked. Each class keeps a
bounded number of shared nodes.

The mode is selected with the ``ORMAR_RELATION_TRAVERSAL`` environment variable
(``instance``, the default, or the deprecated ``map``) or with
``set_relation_traversal``. The ``map`` mode is removed in ormar 0.31.0.
"""

import os
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ormar.models.relation_graph import RelationGraph
from ormar.warnings import OrmarDeprecationWarning

if TYPE_CHECKING:  # pragma: no cover
    from ormar.models.mixins.relation_mixin import RelationMixin

RELATION_TRAVERSAL_ENV = "ORMAR_RELATION_TRAVERSAL"
RELATION_TRAVERSAL_MODES = ("map", "instance")
DEFAULT_RELATION_TRAVERSAL = "instance"
DEFAULT_FOLLOW_DEPTH = 5


def validate_relation_traversal(mode: str, stacklevel: int) -> str:
    """
    Checks a relation traversal mode and warns when the deprecated ``map``
    mode is selected::

        validate_relation_traversal(mode="instance", stacklevel=2)  # -> "instance"

    :param mode: ``map`` or ``instance``
    :type mode: str
    :param stacklevel: stack level of the warning, pointing at the caller
    :type stacklevel: int
    :return: the mode
    :rtype: str
    :raises ValueError: on an unknown mode
    """
    if mode not in RELATION_TRAVERSAL_MODES:
        raise ValueError(
            f"Unknown relation traversal mode: {mode}, "
            f"use one of {', '.join(RELATION_TRAVERSAL_MODES)}"
        )
    if mode == "map":
        warnings.warn(
            OrmarDeprecationWarning(
                "The 'map' relation traversal (precomputed relation tree) is "
                "deprecated, the default 'instance' traversal follows the same "
                "relations without it",
                since=(0, 30),
                expected_removal=(0, 31),
            ),
            stacklevel=stacklevel + 1,
        )
    return mode


def relation_traversal_from_environment() -> str:
    """
    Returns the relation traversal mode set in the ``ORMAR_RELATION_TRAVERSAL``
    environment variable, ``instance`` when it is not set.

    :return: relation traversal mode
    :rtype: str
    :raises ValueError: on an unknown mode
    """
    return validate_relation_traversal(
        mode=os.environ.get(RELATION_TRAVERSAL_ENV, DEFAULT_RELATION_TRAVERSAL),
        stacklevel=2,
    )


class RelationTraversalSettings:
    """
    Holds the relation traversal mode of the process::

        RelationTraversalSettings.mode  # -> "instance"
    """

    mode: str = relation_traversal_from_environment()


def set_relation_traversal(mode: str) -> None:
    """
    Selects how relations are traversed by ``model_dump``, ``save_related``,
    ``get_pydantic`` and ``select_all(follow=True)``. The ``map`` mode is
    deprecated and kept for one release as a transitional switch::

        set_relation_traversal(mode="instance")

    :param mode: ``instance`` for the path based traversal of the loaded
        instances, ``map`` for the deprecated cached relation tree
    :type mode: str
    :raises ValueError: on an unknown mode
    """
    RelationTraversalSettings.mode = validate_relation_traversal(
        mode=mode, stacklevel=2
    )


def instance_traversal_enabled() -> bool:
    """
    Checks if the path based instance traversal is enabled.

    :return: True in ``instance`` mode
    :rtype: bool
    """
    return RelationTraversalSettings.mode == "instance"


PATH_NODE_CACHE_SIZE = 256
NO_ANCESTORS: frozenset = frozenset()


@dataclass(frozen=True)
class RelationIndex:
    """
    Followable relations of one model class: the target of every relation by
    name (in name order), the relation names by target model, and the bounded
    cache of the class's ``PathRelationMap`` nodes by the set of models above.
    """

    targets: dict[str, Any]
    names_by_target: dict[Any, tuple[str, ...]]
    nodes: dict[frozenset, "PathRelationMap"]
    forward: frozenset[str]


def relation_index(model: type["RelationMixin"]) -> RelationIndex:
    """
    Returns the followable relations of a model, leaving out relations to
    unresolved forward references. Kept on the class until the relation graph
    changes::

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
    index = RelationIndex(
        targets=targets,
        names_by_target=names_by_target,
        nodes={},
        forward=frozenset(
            relation for relation in targets if fields[relation].is_valid_uni_relation()
        ),
    )
    model._relation_index = (RelationGraph.version, index)  # type: ignore[attr-defined]
    return index


class PathRelationMap(dict):
    """
    One node of the relation tree of a model, decided from the set of model
    classes on the path above it: a relation is followed unless its target
    model is in that set (a direct self relation is followed once) or it
    points to an unresolved forward reference. It answers what the dump and
    save code asks a relation map (``relation in node``, ``node[relation]``,
    ``bool(node)``) without building the tree, and a leaf is ``...`` like in
    the cached tree.

    A node depends only on (model, models above), so nodes are shared: each
    class keeps at most ``PATH_NODE_CACHE_SIZE`` of them (oldest dropped
    first) and the dump of a row only looks them up::

        node = PathRelationMap.for_path(model=Author, ancestors=NO_ANCESTORS)
        "books" in node  # -> True
        node["books"]  # -> node of Book below Author, without "author"
    """

    __slots__ = ("model", "ancestors", "child_ancestors")

    def __init__(self, model: type["RelationMixin"], ancestors: frozenset) -> None:
        # the dict maps a followed relation to the node cache of its target,
        # so ``in``, ``bool`` and ``len`` run at plain dict speed
        super().__init__(
            (relation, relation_index(model=target).nodes)
            for relation, target in relation_index(model=model).targets.items()
            if target not in ancestors
        )
        self.model = model
        self.ancestors = ancestors
        self.child_ancestors = ancestors | {model}

    @classmethod
    def for_path(
        cls, model: type["RelationMixin"], ancestors: frozenset
    ) -> "PathRelationMap":
        """
        Returns the shared node of a model below the given set of models.

        :param model: model class of the node
        :type model: type[RelationMixin]
        :param ancestors: model classes on the path above the node
        :type ancestors: frozenset
        :return: node
        :rtype: PathRelationMap
        """
        nodes = relation_index(model=model).nodes
        node = nodes.get(ancestors)
        if node is None:
            node = cls.store(nodes=nodes, node=cls(model=model, ancestors=ancestors))
        return node

    @staticmethod
    def store(
        nodes: dict[frozenset, "PathRelationMap"], node: "PathRelationMap"
    ) -> "PathRelationMap":
        """
        Puts a node into the bounded node cache of its class, dropping the
        oldest one when the cache is full.

        :param nodes: node cache of the class
        :type nodes: dict[frozenset, PathRelationMap]
        :param node: new node
        :type node: PathRelationMap
        :return: the node
        :rtype: PathRelationMap
        """
        if len(nodes) >= PATH_NODE_CACHE_SIZE:
            del nodes[next(iter(nodes))]
        nodes[node.ancestors] = node
        return node

    def __getitem__(self, relation: str) -> Any:
        nodes = dict.__getitem__(self, relation)
        child = nodes.get(self.child_ancestors)
        if child is None:
            target = relation_index(model=self.model).targets[relation]
            child = self.store(
                nodes=nodes,
                node=PathRelationMap(model=target, ancestors=self.child_ancestors),
            )
        return child if child else Ellipsis

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
        return self[relation] if relation in self else default

    def items(self) -> Any:
        """
        :return: followed relation names with their child nodes
        :rtype: Iterator[tuple[str, Any]]
        """
        return ((relation, self[relation]) for relation in self)

    def values(self) -> Any:
        """
        :return: child nodes of the followed relations
        :rtype: Iterator[Any]
        """
        return (self[relation] for relation in self)

    def __eq__(self, other: object) -> bool:
        # the values are node caches, comparing them would walk the caches
        return self is other

    __hash__ = None  # type: ignore[assignment]

    def __repr__(self) -> str:  # pragma: no cover
        above = ",".join(sorted(model.get_name() for model in self.ancestors))
        return f"PathRelationMap({self.model.get_name()} below {{{above}}})"


def relation_paths(
    model: type["RelationMixin"],
    max_depth: int,
    reverse_depth: int = 0,
    ancestors: frozenset = NO_ANCESTORS,
) -> list[str]:
    """
    Returns the ``select_related`` paths reachable from a model, at most
    ``max_depth`` relations deep, with the same rule as ``PathRelationMap``.
    Forward foreign keys (one parent per row) are followed at every level,
    reverse and many to many relations (which multiply rows) only in the
    first ``reverse_depth`` levels. Only the deepest paths are listed, as
    ``select_related`` loads the models along a path anyway::

        relation_paths(model=Chapter, max_depth=5)  # -> ["book__author__country"]
        relation_paths(model=Book, max_depth=1, reverse_depth=1)  # + "tags"

    :param model: model class to start from
    :type model: type[RelationMixin]
    :param max_depth: number of relations to follow at most
    :type max_depth: int
    :param reverse_depth: number of first levels that follow reverse and many
        to many relations too
    :type reverse_depth: int
    :param ancestors: model classes on the path above the model
    :type ancestors: frozenset
    :return: relation paths in dunder notation
    :rtype: list[str]
    """
    paths: list[str] = []
    if max_depth <= 0:
        return paths
    index = relation_index(model=model)
    for relation in PathRelationMap.for_path(model=model, ancestors=ancestors):
        if reverse_depth <= 0 and relation not in index.forward:
            continue
        nested = relation_paths(
            model=index.targets[relation],
            max_depth=max_depth - 1,
            reverse_depth=reverse_depth - 1,
            ancestors=ancestors | {model},
        )
        paths.extend(f"{relation}__{path}" for path in nested)
        if not nested:
            paths.append(relation)
    return paths
