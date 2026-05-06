from typing import TYPE_CHECKING, Optional, Sequence, Union
from weakref import proxy

from ormar.relations.relation import Relation, RelationType
from ormar.relations.utils import get_relations_sides_and_names

if TYPE_CHECKING:  # pragma no cover
    from ormar.fields import BaseField, ForeignKeyField
    from ormar.models import Model, NewBaseModel


class RelationsManager:
    """
    Manages relations on a Model, each Model has it's own instance.
    """

    def __init__(
        self,
        related_fields: Optional[list["ForeignKeyField"]] = None,
        owner: Optional["Model"] = None,
    ) -> None:
        self.owner = proxy(owner)
        self._related_fields = related_fields or []
        # ``_field_map`` lets ``_get`` build a ``Relation`` lazily by name.
        # Holding only the field reference (not a constructed Relation) is
        # what skips the per-FK Relation/RelationProxy/QuerysetProxy
        # allocation tree on every ``Model.__init__``.
        self._field_map: dict[str, "ForeignKeyField"] = {
            field.name: field for field in self._related_fields
        }
        self._related_names = list(self._field_map)
        self._relations: dict[str, Relation] = dict()

    def __contains__(self, item: str) -> bool:
        """
        Checks if relation with given name is already registered.

        :param item: name of attribute
        :type item: str
        :return: result of the check
        :rtype: bool
        """
        return item in self._related_names

    def clear(self) -> None:
        for relation in self._relations.values():
            relation.clear()

    def get(self, name: str) -> Optional[Union["Model", Sequence["Model"]]]:
        """
        Returns the related model/models if relation is set.
        Actual call is delegated to Relation instance registered under relation name.

        :param name: name of the relation
        :type name: str
        :return: related model or list of related models if set
        :rtype: Optional[Union[Model, list[Model]]
        """
        relation = self._get(name)
        if relation is not None:
            return relation.get()
        return None  # pragma nocover

    @staticmethod
    def add(parent: "Model", child: "Model", field: "ForeignKeyField") -> None:
        """
        Adds relation on both sides -> meaning on both child and parent models.
        One side of the relation is always weakref proxy to avoid circular refs.

        Based on the side from which relation is added and relation name actual names
        of parent and child relations are established. The related models are registered
        on both ends.

        :param parent: parent model on which relation should be registered
        :type parent: Model
        :param child: child model to register
        :type child: Model
        :param field: field with relation definition
        :type field: ForeignKeyField
        """
        parent, child, child_name, to_name = get_relations_sides_and_names(
            field, parent, child
        )

        # print('adding parent', parent.get_name(), child.get_name(), child_name)
        parent_relation = parent._orm._get(child_name)
        if parent_relation:
            parent_relation.add(child)  # type: ignore

        # print('adding child', child.get_name(), parent.get_name(), to_name)
        child_relation = child._orm._get(to_name)
        if child_relation:
            child_relation.add(parent)

    def remove(
        self, name: str, child: Union["NewBaseModel", type["NewBaseModel"]]
    ) -> None:
        """
        Removes given child from relation with given name.
        Since you can have many relations between two models you need to pass a name
        of relation from which you want to remove the child.

        :param name: name of the relation
        :type name: str
        :param child: child to remove from relation
        :type child: Union[Model, type[Model]]
        """
        relation = self._get(name)
        if relation:
            relation.remove(child)

    @staticmethod
    def remove_parent(
        item: Union["NewBaseModel", type["NewBaseModel"]], parent: "Model", name: str
    ) -> None:
        """
        Removes given parent from relation with given name.
        Since you can have many relations between two models you need to pass a name
        of relation from which you want to remove the parent.

        :param item: model with parent registered
        :type item: Union[Model, type[Model]]
        :param parent: parent Model
        :type parent: Model
        :param name: name of the relation
        :type name: str
        """
        relation_name = item.ormar_config.model_fields[name].get_related_name()
        item._orm.remove(name, parent)
        parent._orm.remove(relation_name, item)

    def _get(self, name: str) -> Optional[Relation]:
        """
        Return the ``Relation`` for ``name``, building it on first access.

        Relations are constructed lazily so that ``Model.__init__`` does
        not allocate a ``Relation`` (and, transitively, ``RelationProxy`` /
        ``QuerysetProxy``) for every declared FK on every instance — most
        of which are never read on row-materialization paths.

        :param name: name of the relation
        :type name: str
        :return: existing or freshly constructed Relation, or None if the
            name does not correspond to a declared relation
        :rtype: ormar.relations.relation.Relation
        """
        relation = self._relations.get(name)
        if relation is not None:
            return relation
        field = self._field_map.get(name)
        if field is None:
            return None
        relation = Relation(
            manager=self,
            type_=self._get_relation_type(field),
            field_name=field.name,
            to=field.to,
            through=getattr(field, "through", None),
        )
        self._relations[name] = relation
        return relation

    def _get_relation_type(self, field: "BaseField") -> RelationType:
        """
        Returns type of the relation declared on a field.

        :param field: field with relation declaration
        :type field: BaseField
        :return: type of the relation defined on field
        :rtype: RelationType
        """
        if field.is_multi:
            return RelationType.MULTIPLE
        if field.is_through:
            return RelationType.THROUGH
        return RelationType.PRIMARY if not field.virtual else RelationType.REVERSE
