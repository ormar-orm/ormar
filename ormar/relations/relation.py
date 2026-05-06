from enum import Enum
from typing import TYPE_CHECKING, Generic, Optional, TypeVar, Union, cast

import ormar  # noqa I100
from ormar.exceptions import RelationshipInstanceError  # noqa I100
from ormar.relations.relation_proxy import RelationProxy

if TYPE_CHECKING:  # pragma no cover
    from ormar.models import Model, NewBaseModel, T
    from ormar.relations import RelationsManager
else:
    T = TypeVar("T", bound="Model")


class RelationType(Enum):
    """
    Different types of relations supported by ormar:

    *  ForeignKey = PRIMARY
    *  reverse ForeignKey = REVERSE
    *  ManyToMany = MULTIPLE
    """

    PRIMARY = 1
    REVERSE = 2
    MULTIPLE = 3
    THROUGH = 4


class Relation(Generic[T]):
    """
    Keeps related Models and handles adding/removing of the children.
    """

    def __init__(
        self,
        manager: "RelationsManager",
        type_: RelationType,
        field_name: str,
        to: type["T"],
        through: Optional[type["Model"]] = None,
    ) -> None:
        """
        Initialize the Relation and keep the related models either as instances of
        passed Model, or as a RelationProxy which is basically a list of models with
        some special behavior, as it exposes QuerySetProxy and allows querying the
        related models already pre filtered by parent model.

        :param manager: reference to relation manager
        :type manager: RelationsManager
        :param type_: type of the relation
        :type type_: RelationType
        :param field_name: name of the relation field
        :type field_name: str
        :param to: model to which relation leads to
        :type to: type[Model]
        :param through: model through which relation goes for m2m relations
        :type through: type[Model]
        """
        self.manager = manager
        self._owner: "Model" = manager.owner
        self._type: RelationType = type_
        self._to_remove: set = set()
        self.to: type["T"] = to
        self._through = through
        self.field_name: str = field_name
        # ``RelationProxy`` is built lazily on the first reverse/m2m use
        # (``add`` / ``get`` / ``_clean_related``) to avoid the per-model
        # allocation when the relation is never read.
        self.related_models: Optional[Union[RelationProxy, "Model"]] = None

    def _ensure_proxy(self) -> RelationProxy:
        """
        Materialize and cache the ``RelationProxy`` for reverse/m2m relations
        on first use. Safe to call multiple times — subsequent calls return
        the cached proxy.

        :return: the relation's ``RelationProxy``
        :rtype: RelationProxy
        """
        proxy = self.related_models
        if not isinstance(proxy, RelationProxy):
            proxy = RelationProxy(
                relation=self,
                type_=self._type,
                to=self.to,
                field_name=self.field_name,
            )
            self.related_models = proxy
        return proxy

    def clear(self) -> None:
        if self._type in (RelationType.PRIMARY, RelationType.THROUGH):
            self.related_models = None
            self._owner.__dict__[self.field_name] = None
        elif self.related_models is not None:
            related_models = cast("RelationProxy", self.related_models)
            related_models._clear()
            self._owner.__dict__[self.field_name] = None

    @property
    def through(self) -> type["Model"]:
        if not self._through:  # pragma: no cover
            raise RelationshipInstanceError("Relation does not have through model!")
        return self._through

    def _clean_related(self) -> None:
        """
        Removes dead weakrefs from RelationProxy.
        """
        cleaned_data = [
            x
            for i, x in enumerate(self.related_models)  # type: ignore
            if i not in self._to_remove
        ]
        proxy = RelationProxy(
            relation=self,
            type_=self._type,
            to=self.to,
            field_name=self.field_name,
            data_=cleaned_data,
        )
        self.related_models = proxy
        self._owner.__dict__[self.field_name] = proxy
        self._to_remove = set()

    def _find_existing(
        self, child: Union["NewBaseModel", type["NewBaseModel"]]
    ) -> Optional[int]:
        """
        Find child model in RelationProxy if exists.

        :param child: child model to find
        :type child: Model
        :return: index of child in RelationProxy
        :rtype: Optional[ind]
        """
        if not isinstance(self.related_models, RelationProxy):  # pragma nocover
            raise ValueError("Cannot find existing models in parent relation type")

        if child not in self.related_models:
            return None
        else:
            # We need to clear the weakrefs that don't point to anything anymore
            # There's an assumption here that if some of the related models
            # went out of scope, then they all did, so we can just check the first one
            try:
                self.related_models[0].__repr__.__self__
                return self.related_models.index(child)
            except ReferenceError:
                missing = self.related_models._get_list_of_missing_weakrefs()
                self._to_remove.update(missing)
            return self.related_models.index(child)

    def add(self, child: "Model") -> None:
        """
        Adds child Model to relation, either sets child as related model or adds
        it to the list in RelationProxy depending on relation type.

        For reverse / many-to-many relations the ``RelationProxy`` itself is
        stored under ``_owner.__dict__[relation_name]``, so a single O(1)
        membership check on the proxy's hash cache covers both the relation
        bookkeeping and the pydantic-visible ``__dict__`` slot — no parallel
        list, no second linear scan.

        :param child: model to add to relation
        :type child: Model
        """
        relation_name = self.field_name
        if self._type in (RelationType.PRIMARY, RelationType.THROUGH):
            self.related_models = child
            self._owner.__dict__[relation_name] = child
            return
        proxy = self._ensure_proxy()
        # ``_find_existing`` is the membership check *plus* a dead-weakref
        # probe — when a hash collision lands on a stale entry we need that
        # probe to populate ``_to_remove`` so the next ``get()`` triggers
        # ``_clean_related``.
        if self._find_existing(child) is None:
            proxy.append(child)
            self._owner.__dict__[relation_name] = proxy

    def remove(self, child: Union["NewBaseModel", type["NewBaseModel"]]) -> None:
        """
        Removes child Model from relation, either sets None as related model or removes
        it from the list in RelationProxy depending on relation type.

        :param child: model to remove from relation
        :type child: Model
        """
        relation_name = self.field_name
        if self._type == RelationType.PRIMARY:
            if self.related_models == child:
                self.related_models = None
                del self._owner.__dict__[relation_name]
        else:
            position = self._find_existing(child)
            if position is not None:
                self.related_models.pop(position)  # type: ignore

    def get(self) -> Optional[Union[list["Model"], "Model"]]:
        """
        Return the related model or models from RelationProxy.

        For reverse / many-to-many relations the ``RelationProxy`` is
        materialized on first read so callers always see a list-like
        return value (even when no children have been registered yet).

        :return: related model/models if set
        :rtype: Optional[Union[list[Model], Model]]
        """
        if self._to_remove:
            self._clean_related()
        if self.related_models is None and self._type in (
            RelationType.REVERSE,
            RelationType.MULTIPLE,
        ):
            return self._ensure_proxy()
        return self.related_models

    def __repr__(self) -> str:  # pragma no cover
        if self._to_remove:
            self._clean_related()
        return str(self.related_models)
