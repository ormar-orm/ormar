from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar

from typing_extensions import SupportsIndex

import ormar
from ormar.exceptions import NoMatch, RelationshipInstanceError
from ormar.relations.querysetproxy import QuerysetProxy

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model, RelationType
    from ormar.models import T
    from ormar.queryset import QuerySet
    from ormar.relations import Relation
else:
    T = TypeVar("T", bound="Model")


_QUERYSET_PROXY_METHODS = frozenset({"count", "clear"})


class RelationProxy(Generic[T], list[T]):
    """
    Proxy of the Relation that is a list with special methods.
    """

    def __init__(
        self,
        relation: "Relation",
        type_: "RelationType",
        to: type["T"],
        field_name: str,
        data_: Any = None,
    ) -> None:
        self.relation: "Relation[T]" = relation
        self.type_: "RelationType" = type_
        self.field_name = field_name
        self._owner: "Model" = self.relation.manager.owner
        self.queryset_proxy: QuerysetProxy[T] = QuerysetProxy[T](
            relation=self.relation, to=to, type_=type_
        )
        self._related_field_name: Optional[str] = None

        self._relation_cache: dict[int, int] = {}

        validated_data = []
        if data_ is not None:
            idx = 0
            for d in data_:
                try:
                    self._relation_cache[d.__hash__()] = idx
                    validated_data.append(d)
                    idx += 1
                except ReferenceError:
                    pass
        super().__init__(validated_data or ())

    @property
    def related_field_name(self) -> str:
        """
        On first access calculates the name of the related field, later stored in
        _related_field_name property.

        :return: name of the related field
        :rtype: str
        """
        if self._related_field_name:
            return self._related_field_name
        owner_field = self._owner.ormar_config.model_fields[self.field_name]
        self._related_field_name = owner_field.get_related_name()

        return self._related_field_name

    def __getitem__(self, item: Any) -> "T":  # type: ignore
        return super().__getitem__(item)

    def append(self, item: "T") -> None:
        """
        Appends an item to the list in place

        :param item: The generic item of the list
        :type item: T
        """
        idx = len(self)
        self._relation_cache[item.__hash__()] = idx
        super().append(item)

    def update_cache(self, prev_hash: int, new_hash: int) -> None:
        """
        Updates the cache from the old hash to the new one.
        This maintains the index cache, which allows O(1) indexing and
        existence checks

        :param prev_hash: The hash to update
        :type prev_hash: int
        :param prev_hash: The new hash to update to
        :type new_hash: int
        """
        try:
            idx = self._relation_cache.pop(prev_hash)
            self._relation_cache[new_hash] = idx
        except KeyError:
            pass

    def index(self, item: T, *args: Any) -> int:
        """
        Gets the index of the item in the list

        :param item: The item to get the index of
        :type item: "T"
        """
        return self._relation_cache[item.__hash__()]

    def _get_list_of_missing_weakrefs(self) -> set[int]:
        """
        Iterates through the list and checks for weakrefs.

        :return: The set of missing weakref indices
        :rtype: set[int]
        """
        to_remove = set()
        for ind, relation_child in enumerate(self[:]):
            try:
                relation_child.__repr__.__self__  # type: ignore
            except ReferenceError:  # pragma no cover
                to_remove.add(ind)

        return to_remove

    def pop(self, index: SupportsIndex = 0) -> T:
        """
        Pops the index off the list and returns it. By default,
        it pops off the element at index 0.
        This also clears the value from the relation cache.

        :param index: The index to pop
        :type index: SupportsIndex
        :return: The item at the provided index
        :rtype: "T"
        """
        item = self[index]

        # Try to delete it, but do it a long way
        # if weakly-referenced thing doesn't exist
        try:
            self._relation_cache.pop(item.__hash__())
        except ReferenceError:
            for hash_, idx in self._relation_cache.items():
                if idx == index:
                    self._relation_cache.pop(hash_)
                    break

        index_int = int(index)
        for idx in range(index_int + 1, len(self)):
            self._relation_cache[self[idx].__hash__()] -= 1

        return super().pop(index)

    def __contains__(self, item: object) -> bool:
        """
        Checks whether the item exists in self. This relies
        on the relation cache, which is a hashmap of values
        in the list. It runs in O(1) time.

        :param item: The item to check if the list contains
        :type item: object
        """
        try:
            return item.__hash__() in self._relation_cache
        except ReferenceError:
            return False

    def __getattribute__(self, item: str) -> Any:
        """
        Since some QuerySetProxy methods overwrite builtin list methods we
        catch calls to them and delegate it to QuerySetProxy instead.

        :param item: name of attribute
        :type item: str
        :return: value of attribute
        :rtype: Any
        """
        if item in _QUERYSET_PROXY_METHODS:
            self._initialize_queryset()
            return getattr(self.queryset_proxy, item)
        return super().__getattribute__(item)

    def __getattr__(self, item: str) -> Any:
        """
        Delegates calls for non existing attributes to QuerySetProxy.

        :param item: name of attribute/method
        :type item: str
        :return: method from QuerySetProxy if exists
        :rtype: method
        """
        self._initialize_queryset()
        return getattr(self.queryset_proxy, item)

    def _clear(self) -> None:
        self._relation_cache.clear()
        super().clear()

    def _initialize_queryset(self) -> None:
        """
        Initializes the QuerySetProxy if not yet initialized.
        """
        if not self._check_if_queryset_is_initialized():
            self.queryset_proxy.queryset = self._set_queryset()

    def _check_if_queryset_is_initialized(self) -> bool:
        """
        Checks if the QuerySetProxy is already set and ready.
        :return: result of the check
        :rtype: bool
        """
        return (
            hasattr(self.queryset_proxy, "queryset")
            and self.queryset_proxy.queryset is not None
        )

    def _check_if_model_saved(self) -> None:
        """
        Verifies if the parent model of the relation has been already saved.
        Otherwise QuerySetProxy cannot filter by parent primary key.
        """
        pk_value = self._owner.pk
        if not pk_value:
            raise RelationshipInstanceError(
                "You cannot query relationships from unsaved model."
            )

    def _set_queryset(self) -> "QuerySet[T]":
        """
        Creates new QuerySet with relation model and pre filters it with currents
        parent model primary key, so all queries by definition are already related
        to the parent model only, without need for user to filter them.

        :return: initialized QuerySet
        :rtype: QuerySet
        """
        related_field_name = self.related_field_name
        pkname = self._owner.get_column_alias(self._owner.ormar_config.pkname)
        self._check_if_model_saved()
        kwargs = {f"{related_field_name}__{pkname}": self._owner.pk}
        queryset = (
            ormar.QuerySet(
                model_cls=self.relation.to, proxy_source_model=self._owner.__class__
            )
            .select_related(related_field_name)
            .filter(**kwargs)
        )
        return queryset

    async def remove(  # type: ignore
        self, item: "T", keep_reversed: bool = True
    ) -> None:
        """
        Removes the related from relation with parent.

        Through models are automatically deleted for m2m relations.

        For reverse FK relations keep_reversed flag marks if the reversed models
        should be kept or deleted from the database too (False means that models
        will be deleted, and not only removed from relation).

        :param item: child to remove from relation
        :type item: Model
        :param keep_reversed: flag if the reversed model should be kept or deleted too
        :type keep_reversed: bool
        """
        if item not in self:
            raise NoMatch(
                f"Object {self._owner.get_name()} has no "
                f"{item.get_name()} with given primary key!"
            )
        await self._owner.signals.pre_relation_remove.send(
            sender=self._owner.__class__,
            instance=self._owner,
            child=item,
            relation_name=self.field_name,
        )

        index_to_remove = self._relation_cache[item.__hash__()]
        self.pop(index_to_remove)

        relation_name = self.related_field_name
        relation = item._orm._get(relation_name)
        # if relation is None:  # pragma nocover
        #     raise ValueError(
        #         f"{self._owner.get_name()} does not have relation {relation_name}"
        #     )
        if relation:
            relation.remove(self._owner)
        self.relation.remove(item)
        if self.type_ == ormar.RelationType.MULTIPLE:
            await self.queryset_proxy.delete_through_instance(item)
        else:
            if keep_reversed:
                setattr(item, relation_name, None)
                await item.update()
            else:
                await item.delete()
        await self._owner.signals.post_relation_remove.send(
            sender=self._owner.__class__,
            instance=self._owner,
            child=item,
            relation_name=self.field_name,
        )

    async def add(self, item: "T", **kwargs: Any) -> None:
        """
        Adds child model to relation.

        For ManyToMany relations through instance is automatically created.

        :param kwargs: dict of additional keyword arguments for through instance
        :type kwargs: Any
        :param item: child to add to relation
        :type item: Model
        """
        new_idx = len(self) if item not in self else self.index(item)
        relation_name = self.related_field_name
        await self._owner.signals.pre_relation_add.send(
            sender=self._owner.__class__,
            instance=self._owner,
            child=item,
            relation_name=self.field_name,
            passed_kwargs=kwargs,
        )
        self._check_if_model_saved()
        if self.type_ == ormar.RelationType.MULTIPLE:
            await self.queryset_proxy.create_through_instance(item, **kwargs)
            setattr(self._owner, self.field_name, item)
        else:
            setattr(item, relation_name, self._owner)
            await item.upsert()
        self._relation_cache[item.__hash__()] = new_idx
        await self._owner.signals.post_relation_add.send(
            sender=self._owner.__class__,
            instance=self._owner,
            child=item,
            relation_name=self.field_name,
            passed_kwargs=kwargs,
        )
