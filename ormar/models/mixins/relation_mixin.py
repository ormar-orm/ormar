from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Callable, Optional, cast

from ormar import BaseField, ForeignKeyField
from ormar.models.relation_graph import RelationGraph
from ormar.models.traversal import (
    DEFAULT_FOLLOW_DEPTH,
    PathRelationMap,
    instance_traversal_enabled,
    relation_paths,
    visited_paths_cached,
)
from ormar.models.traversible import NodeList


def build_relation_tree(model: type["RelationMixin"]) -> dict[str, Any]:
    """
    Builds the relation tree of a model: the nested dict of the relation paths
    of ``_iterate_related_models``, walked the same way (relations in name
    order, a relation skipped when its target model is already on the path,
    relations to unresolved forward references left out).

    Like ``_iterate_related_models`` the walk expands a ``(model, relation)``
    pair once and reuses that subtree wherever the pair appears again, but it
    puts the same dict object at every such place instead of spelling out
    every path, so the tree grows with the number of relations, not with the
    (exponential) number of paths::

        build_relation_tree(model=Book)  # -> {"author": {"country": ...}, "tags": ...}

    :param model: model class to build the tree for
    :type model: type[RelationMixin]
    :return: nested dict of relation names, ``...`` at the leaves
    :rtype: dict[str, Any]
    """
    return expand_relation_subtree(model=model, ancestors=(), expanded={})


def expand_relation_subtree(
    model: type["RelationMixin"],
    ancestors: tuple[type["RelationMixin"], ...],
    expanded: dict[str, Any],
) -> dict[str, Any]:
    """
    Returns the relations below one model of the relation tree walk.

    :param model: model class of the current node
    :type model: type[RelationMixin]
    :param ancestors: model classes on the path above the current node
    :type ancestors: tuple[type[RelationMixin], ...]
    :param expanded: subtrees already built, by ``<model name>_<relation>``
    :type expanded: dict[str, Any]
    :return: relation name to its subtree, ``...`` when nothing is below it
    :rtype: dict[str, Any]
    """
    subtree: dict[str, Any] = {}
    path = (*ancestors, model)
    for relation in sorted(model.extract_related_names()):
        field = model.ormar_config.model_fields[relation]
        # a relation to a forward reference joins the tree once resolved
        if field.has_unresolved_forward_refs() or field.to in ancestors:
            continue
        relation_key = f"{model.get_name()}_{relation}"
        if relation_key not in expanded:
            expanded[relation_key] = (
                expand_relation_subtree(
                    model=field.to, ancestors=path, expanded=expanded
                )
                or Ellipsis
            )
        subtree[relation] = expanded[relation_key]
    return subtree


class RelationMixin:
    """
    Used to return relation fields/names etc. from given model
    """

    if TYPE_CHECKING:  # pragma no cover
        from ormar.models.ormar_config import OrmarConfig

        ormar_config: OrmarConfig
        __relation_map__: Optional[list[str]]
        __relation_tree__: Optional[dict]
        __relation_graph_version__: int
        _related_names: Optional[set]
        _through_names: Optional[set]
        _related_fields: Optional[list]
        _related_field_map: Optional[dict[str, ForeignKeyField]]
        get_name: Callable

    @classmethod
    def extract_db_own_fields(cls) -> set:
        """
        Returns only fields that are stored in the own database table, exclude all
        related fields.
        :return: set of model fields with relation fields excluded
        :rtype: set
        """
        try:
            return cls._db_own_fields  # type: ignore[attr-defined]
        except AttributeError:
            related_names = cls.extract_related_names()
            self_fields = {
                name
                for name in cls.ormar_config.model_fields.keys()
                if name not in related_names
            }
            cls._db_own_fields = self_fields  # type: ignore[attr-defined]
            return self_fields

    @classmethod
    def extract_related_fields(cls) -> list["ForeignKeyField"]:
        """
        Returns list of ormar Fields for all relations declared on a model.
        List is cached in cls._related_fields for quicker access.

        :return: list of related fields
        :rtype: list
        """
        if cls._related_fields is not None:
            return cls._related_fields

        related_fields = []
        for name in cls.extract_related_names().union(cls.extract_through_names()):
            related_fields.append(
                cast("ForeignKeyField", cls.ormar_config.model_fields[name])
            )
        cls._related_fields = related_fields

        return related_fields

    @classmethod
    def extract_related_field_map(cls) -> dict[str, "ForeignKeyField"]:
        """
        Returns the relation fields of a model by name, built once per model
        and shared by the ``RelationsManager`` of every instance, which must
        not modify it. Cached in cls._related_field_map and reset together
        with cls._related_fields::

            Book.extract_related_field_map()  # -> {"author": <ForeignKeyField>}

        :return: relation fields by name
        :rtype: dict[str, ForeignKeyField]
        """
        field_map = cls._related_field_map
        if field_map is None:
            field_map = {field.name: field for field in cls.extract_related_fields()}
            cls._related_field_map = field_map
        return field_map

    @classmethod
    def extract_through_names(cls) -> set[str]:
        """
        Extracts related fields through names which are shortcuts to through models.

        :return: set of related through fields names
        :rtype: set
        """
        if cls._through_names is not None:
            return cls._through_names

        related_names = set()
        for name, field in cls.ormar_config.model_fields.items():
            if isinstance(field, BaseField) and field.is_through:
                related_names.add(name)

        cls._through_names = related_names
        return related_names

    @classmethod
    def extract_related_names(cls) -> set[str]:
        """
        Returns list of fields names for all relations declared on a model.
        List is cached in cls._related_names for quicker access.

        :return: set of related fields names
        :rtype: set
        """
        if cls._related_names is not None:
            return cls._related_names

        related_names = set()
        for name, field in cls.ormar_config.model_fields.items():
            if (
                isinstance(field, BaseField)
                and field.is_relation
                and not field.is_through
                and not field.skip_field
            ):
                related_names.add(name)
        cls._related_names = related_names

        return related_names

    @classmethod
    def _extract_db_related_names(cls) -> set:
        """
        Returns only fields that are stored in the own database table, exclude
        related fields that are not stored as foreign keys on given model.
        :return: set of model fields with non fk relation fields excluded
        :rtype: set
        """
        related_names = cls.extract_related_names()
        related_names = {
            name
            for name in related_names
            if cls.ormar_config.model_fields[name].is_valid_uni_relation()
        }
        return related_names

    @classmethod
    def _iterate_related_models(  # noqa: CCR001
        cls,
        node_list: Optional[NodeList] = None,
        parsed_map: Optional[dict] = None,
        source_relation: Optional[str] = None,
        recurrent: bool = False,
    ) -> list[str]:
        """
        Iterates related models recursively to extract relation strings of
        nested not visited models.

        The top level result is cached on the class until the relation graph
        changes (see ``RelationGraph``).

        :return: list of relation strings to be passed to select_related
        :rtype: list[str]
        """
        if not node_list:
            cls._drop_stale_relation_maps()
            if cls.__relation_map__ is not None:
                return cls.__relation_map__
            node_list = NodeList()
            parsed_map = dict()
            current_node = node_list.add(node_class=cls)
        else:
            current_node = node_list[-1]
        relations = sorted(cls.extract_related_names())
        processed_relations: list[str] = []
        for relation in relations:
            field = cls.ormar_config.model_fields[relation]
            # a relation to a forward reference joins the paths once resolved
            if field.has_unresolved_forward_refs():
                continue
            if not current_node.visited(relation):
                target_model = field.to
                node_list.add(
                    node_class=target_model,
                    relation_name=relation,
                    parent_node=current_node,
                )
                relation_key = f"{cls.get_name()}_{relation}"
                parsed_map = cast(dict, parsed_map)
                deep_relations = parsed_map.get(relation_key)
                if not deep_relations:
                    deep_relations = target_model._iterate_related_models(
                        source_relation=relation,
                        node_list=node_list,
                        recurrent=True,
                        parsed_map=parsed_map,
                    )
                    parsed_map[relation_key] = deep_relations
                processed_relations.extend(deep_relations)

        result = cls._get_final_relations(processed_relations, source_relation)
        if not recurrent:
            cls.__relation_map__ = result
        return result

    @classmethod
    def _relation_tree(cls) -> dict:
        """
        Returns the relation paths of ``_iterate_related_models`` as a nested
        dict, the default relation map of ``model_dump``, ``save_related`` and
        ``get_pydantic``. Built once per model class without the list of paths
        (see ``build_relation_tree``) and cached until the relation graph
        changes. The tree is shared between calls and a repeated subtree is
        one dict object at every place it appears, so it must not be
        modified::

            Book._iterate_related_models()  # -> ["author__country", "tags"]
            Book._relation_tree()  # -> {"author": {"country": ...}, "tags": ...}

        :return: nested dict of relation names
        :rtype: dict
        """
        cls._drop_stale_relation_maps()
        tree = cls.__relation_tree__
        if tree is None:
            tree = build_relation_tree(model=cls)
            cls.__relation_tree__ = tree
        return tree

    @classmethod
    def _default_relation_map(cls) -> Mapping[str, Any]:
        """
        Returns the default relation map of ``model_dump`` and ``save_related``:
        the cached ``_relation_tree()`` or, in ``instance`` traversal mode, a
        ``PathRelationMap`` that decides from the path of the walk (kept on the
        class in ``instance_cached`` mode)::

            Author._default_relation_map()  # -> PathRelationMap(author)

        :return: relation map to follow
        :rtype: Mapping[str, Any]
        """
        if instance_traversal_enabled():
            if not visited_paths_cached():
                return PathRelationMap(model=cls, ancestors=())
            cached = cls.__dict__.get("_path_root")
            if cached is None or cached[0] != RelationGraph.version:
                cached = (
                    RelationGraph.version,
                    PathRelationMap(model=cls, ancestors=()),
                )
                cls._path_root = cached  # type: ignore[attr-defined]
            return cached[1]
        return cls._relation_tree()

    @classmethod
    def _follow_relation_paths(cls, max_depth: Optional[int] = None) -> list[str]:
        """
        Returns the relation paths loaded by ``select_all(follow=True)`` and
        ``load_all(follow=True)``. Without ``max_depth`` the cached
        ``_iterate_related_models()`` paths are used, or in ``instance``
        traversal mode the paths up to ``DEFAULT_FOLLOW_DEPTH`` relations deep::

            Book._follow_relation_paths(max_depth=1)  # -> ["author", "tags"]

        :param max_depth: number of relations to follow at most
        :type max_depth: Optional[int]
        :return: relation paths in dunder notation
        :rtype: list[str]
        """
        if max_depth is None and not instance_traversal_enabled():
            return cls._iterate_related_models()
        return relation_paths(
            model=cls,
            max_depth=max_depth if max_depth is not None else DEFAULT_FOLLOW_DEPTH,
        )

    @classmethod
    def _drop_stale_relation_maps(cls) -> None:
        """
        Drops the cached relation paths and tree of the model when a relation
        was registered on any model since they were built.
        """
        if cls.__relation_graph_version__ != RelationGraph.version:
            cls.__relation_map__ = None
            cls.__relation_tree__ = None
            cls.__relation_graph_version__ = RelationGraph.version

    @staticmethod
    def _get_final_relations(
        processed_relations: list, source_relation: Optional[str]
    ) -> list[str]:
        """
        Helper method to prefix nested relation strings with current source relation

        :param processed_relations: list of already processed relation str
        :type processed_relations: list[str]
        :param source_relation: name of the current relation
        :type source_relation: str
        :return: list of relation strings to be passed to select_related
        :rtype: list[str]
        """
        if processed_relations:
            final_relations = [
                f"{source_relation + '__' if source_relation else ''}{relation}"
                for relation in processed_relations
            ]
        else:
            final_relations = [source_relation] if source_relation else []
        return final_relations
