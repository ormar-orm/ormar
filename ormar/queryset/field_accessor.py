from typing import TYPE_CHECKING, Any, Optional, cast

from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions import OrderAction
from ormar.queryset.actions.filter_action import METHODS_TO_OPERATORS
from ormar.queryset.clause import FilterGroup, NullsOrdering
from ormar.queryset.expressions import Expression, F

if TYPE_CHECKING:  # pragma: no cover
    from ormar import BaseField, Model


class FieldAccessor(Expression):
    """
    Helper to access ormar fields directly from Model class also for nested
    models attributes.

    An accessor of a non-relation field is a column expression as well:
    ``Slot.taken < Slot.capacity``, ``update(taken=Slot.taken + 1)``.
    """

    def __init__(
        self,
        source_model: type["Model"],
        field: Optional["BaseField"] = None,
        model: Optional[type["Model"]] = None,
        access_chain: str = "",
        is_key: bool = False,
    ) -> None:
        self._source_model = source_model
        self._field = field
        self._model = model
        self._access_chain = access_chain
        self._is_key = is_key

    def __getattr__(self, item: str) -> Any:
        """
        Accessor return new accessor for each field and nested models.
        Thanks to that operator overload is possible to use in filter.

        :param item: attribute name
        :type item: str
        :return: FieldAccessor for field or nested model
        :rtype: ormar.queryset.field_accessor.FieldAccessor
        """
        if (
            object.__getattribute__(self, "_field")
            and item == object.__getattribute__(self, "_field").name
        ):
            return self._field

        if item == "pk" and object.__getattribute__(self, "_model"):
            return key_accessor(
                source_model=self._source_model,
                model=cast(type["Model"], self._model),
                prefix=self._access_chain + "__",
            )

        if (
            object.__getattribute__(self, "_model")
            and item
            in object.__getattribute__(self, "_model").ormar_config.model_fields
        ):
            field = cast("Model", self._model).ormar_config.model_fields[item]
            if field.is_relation:
                return FieldAccessor(
                    source_model=self._source_model,
                    model=field.to,
                    access_chain=self._access_chain + f"__{item}",
                )
            else:
                return FieldAccessor(
                    source_model=self._source_model,
                    field=field,
                    access_chain=self._access_chain + f"__{item}",
                )
        return object.__getattribute__(self, item)  # pragma: no cover

    def _check_field(self) -> None:
        if self._field or self._is_key:
            return
        field = self._source_model.ormar_config.model_fields.get(self._access_chain)
        if field is not None and not field.virtual and not field.is_multi:
            return
        raise AttributeError("Cannot filter by Model, you need to provide model name")

    def _node(self) -> F:
        """
        :raises QueryDefinitionError: for relation and composite key accessors
        :return: the field as an ``F`` node, which survives the deep copies of
            prefetch querysets
        :rtype: F
        """
        if self._field is None:
            raise QueryDefinitionError(
                "Only non-relation fields can be used in expressions"
            )
        return F(path=self._access_chain)

    def _select_operator(self, op: str, other: Any) -> FilterGroup:
        self._check_field()
        filter_kwg = {self._access_chain + f"__{METHODS_TO_OPERATORS[op]}": other}
        return FilterGroup(**filter_kwg)

    def __eq__(self, other: Any) -> FilterGroup:  # type: ignore
        """
        overloaded to work as sql `column = <VALUE>`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="__eq__", other=other)

    def __ne__(self, other: Any) -> FilterGroup:  # type: ignore
        """
        overloaded to work as sql `column != <VALUE>` (`IS NOT NULL` for None)

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="__ne__", other=other)

    def __ge__(self, other: Any) -> FilterGroup:
        """
        overloaded to work as sql `column >= <VALUE>`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="__ge__", other=other)

    def __gt__(self, other: Any) -> FilterGroup:
        """
        overloaded to work as sql `column > <VALUE>`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="__gt__", other=other)

    def __le__(self, other: Any) -> FilterGroup:
        """
        overloaded to work as sql `column <= <VALUE>`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="__le__", other=other)

    def __lt__(self, other: Any) -> FilterGroup:
        """
        overloaded to work as sql `column < <VALUE>`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="__lt__", other=other)

    def __mod__(self, other: Any) -> FilterGroup:
        """
        overloaded to work as sql `column LIKE '%<VALUE>%'`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="__mod__", other=other)

    def __lshift__(self, other: Any) -> FilterGroup:
        """
        overloaded to work as sql `column IN (<VALUE1>, <VALUE2>,...)`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="in", other=other)

    def __rshift__(self, other: Any) -> FilterGroup:
        """
        overloaded to work as sql `column IS NULL`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="isnull", other=True)

    def in_(self, other: Any) -> FilterGroup:
        """
        works as sql `column IN (<VALUE1>, <VALUE2>,...)`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="in", other=other)

    def iexact(self, other: Any) -> FilterGroup:
        """
        works as sql `column = <VALUE>` case-insensitive

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="iexact", other=other)

    def contains(self, other: Any) -> FilterGroup:
        """
        works as sql `column LIKE '%<VALUE>%'`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="contains", other=other)

    def icontains(self, other: Any) -> FilterGroup:
        """
        works as sql `column LIKE '%<VALUE>%'` case-insensitive

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="icontains", other=other)

    def startswith(self, other: Any) -> FilterGroup:
        """
        works as sql `column LIKE '<VALUE>%'`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="startswith", other=other)

    def istartswith(self, other: Any) -> FilterGroup:
        """
        works as sql `column LIKE '%<VALUE>'` case-insensitive

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="istartswith", other=other)

    def endswith(self, other: Any) -> FilterGroup:
        """
        works as sql `column LIKE '%<VALUE>'`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="endswith", other=other)

    def iendswith(self, other: Any) -> FilterGroup:
        """
        works as sql `column LIKE '%<VALUE>'` case-insensitive

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="iendswith", other=other)

    def isnull(self, other: Any) -> FilterGroup:
        """
        works as sql `column IS NULL` or `IS NOT NULL`

        :param other: value to check agains operator
        :type other: str
        :return: FilterGroup for operator
        :rtype: ormar.queryset.clause.FilterGroup
        """
        return self._select_operator(op="isnull", other=other)

    def asc(self, nulls_ordering: Optional[NullsOrdering] = None) -> OrderAction:
        """
        works as sql `column asc`

        :param nulls_ordering: optional nulls placement, `NullsOrdering.FIRST`
            or `NullsOrdering.LAST`
        :type nulls_ordering: Optional[NullsOrdering]
        :raises ValueError: if `nulls_ordering` is not a `NullsOrdering` member
        :return: OrderGroup for operator
        :rtype: ormar.queryset.actions.OrderGroup
        """
        nulls_value = self._coerce_nulls_ordering(nulls_ordering)
        return OrderAction(
            order_str=self._access_chain,
            model_cls=self._source_model,
            nulls_ordering=nulls_value,
        )

    def desc(self, nulls_ordering: Optional[NullsOrdering] = None) -> OrderAction:
        """
        works as sql `column desc`

        :param nulls_ordering: optional nulls placement, `NullsOrdering.FIRST`
            or `NullsOrdering.LAST`
        :type nulls_ordering: Optional[NullsOrdering]
        :raises ValueError: if `nulls_ordering` is not a `NullsOrdering` member
        :return: OrderGroup for operator
        :rtype: ormar.queryset.actions.OrderGroup
        """
        nulls_value = self._coerce_nulls_ordering(nulls_ordering)
        return OrderAction(
            order_str="-" + self._access_chain,
            model_cls=self._source_model,
            nulls_ordering=nulls_value,
        )

    @staticmethod
    def _coerce_nulls_ordering(
        nulls_ordering: Optional[NullsOrdering],
    ) -> Optional[str]:
        if nulls_ordering is None:
            return None
        if not isinstance(nulls_ordering, NullsOrdering):
            raise ValueError("Invalid option for ordering nulls values.")
        return nulls_ordering.value


def key_accessor(
    source_model: type["Model"], model: type["Model"], prefix: str
) -> FieldAccessor:
    """
    Accessor for the primary key of ``model`` reached from ``source_model``
    through ``prefix``, used by ``Model.pk`` and ``Model.relation.pk``.

    A single column key resolves to the accessor of its field. A composite key
    resolves to a key accessor whose ``==`` and ``in_()`` / ``<<`` filters
    compile to row-value comparisons like ``filter(pk=...)`` and
    ``filter(pk__in=...)``.

    :param source_model: model the filter is built for
    :type source_model: type[Model]
    :param model: model owning the primary key
    :type model: type[Model]
    :param prefix: relation access chain leading to ``model``, ``""`` for the
        source model itself
    :type prefix: str
    :return: accessor of the key
    :rtype: FieldAccessor
    """
    key = model.ormar_config.pk
    if key.is_composite:
        return FieldAccessor(
            source_model=source_model, access_chain=f"{prefix}pk", is_key=True
        )
    field = model.ormar_config.model_fields[key.fields[0]]
    return FieldAccessor(
        source_model=source_model, field=field, access_chain=prefix + field.name
    )
