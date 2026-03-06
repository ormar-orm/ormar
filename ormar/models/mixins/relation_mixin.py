from typing import TYPE_CHECKING, Callable, Optional, cast

from ormar import BaseField, ForeignKeyField
from ormar.models.traversible import NodeList


class RelationMixin:
    """
    Used to return relation fields/names etc. from given model
    """

    if TYPE_CHECKING:  # pragma no cover
        from ormar.models.ormar_config import OrmarConfig

        ormar_config: OrmarConfig
        __relation_map__: Optional[list[str]]
        _related_names: Optional[set]
        _through_names: Optional[set]
        _related_fields: Optional[list]
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

        :return: list of relation strings to be passed to select_related
        :rtype: list[str]
        """
        if not node_list:
            if cls.__relation_map__:
                return cls.__relation_map__
            node_list = NodeList()
            parsed_map = dict()
            current_node = node_list.add(node_class=cls)
        else:
            current_node = node_list[-1]
        relations = sorted(cls.extract_related_names())
        processed_relations: list[str] = []
        for relation in relations:
            if not current_node.visited(relation):
                target_model = cls.ormar_config.model_fields[relation].to
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
