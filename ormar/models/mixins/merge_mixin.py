from typing import TYPE_CHECKING, Optional, cast

import ormar_rust_utils

import ormar
from ormar.models.excludable import skip_ellipsis
from ormar.queryset.utils import translate_list_to_dict

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model
    from ormar.models.excludable import ExcludableItems


class MergeModelMixin:
    """
    Used to merge models instances returned by database,
    but already initialized to ormar Models.keys

    Models can duplicate during joins when parent model has multiple child rows,
    in the end all parent (main) models should be unique.
    """

    @classmethod
    def _recursive_add(cls, model_group: list["Model"]) -> list["Model"]:
        """
        Instead of accumulating the model additions one by one, this recursively adds
        the models. E.G.
        [1, 2, 3, 4].accumulate_add() would give [3, 3, 4], then [6, 4], then [10]
        where this method looks like
        [1, 2, 3, 4].recursive_add() gives [[3], [7]], [10]
        It's the same number of adds, but it gives better O(N) performance on sublists
        """
        if len(model_group) <= 1:
            return model_group

        added_values = []
        iterable_group = iter(model_group)
        for model in iterable_group:
            next_model = next(iterable_group, None)
            if next_model is not None:
                combined = cls.merge_two_instances(next_model, model)
            else:
                combined = model
            added_values.append(combined)

        return cls._recursive_add(added_values)

    @classmethod
    def merge_instances_list(
        cls,
        result_rows: list["Model"],
        *,
        excludable: "ExcludableItems",
    ) -> list["Model"]:
        """
        Merges a list of models into list of unique models.

        Models can duplicate during joins when parent model has multiple child rows,
        in the end all parent (main) models should be unique.

        Each merged top-level model also has the queryset's ``ExcludableItems``
        attached via the ``__ormar_excludable__`` slot so a bare ``model_dump()``
        on the result can resolve the (lazily cached) flatten map without going
        back through the queryset.

        :param result_rows: list of already initialized Models with child models
        populated, each instance is one row in db and some models can duplicate
        :type result_rows: list["Model"]
        :param excludable: ExcludableItems to attach to every merged top-level
            instance via the ``__ormar_excludable__`` slot
        :type excludable: ExcludableItems
        :return: list of merged models where each main model is unique
        :rtype: list["Model"]
        """
        merged_rows: list["Model"] = []

        if result_rows:
            pks = [model.pk for model in result_rows]
            index_groups = ormar_rust_utils.group_by_pk(pks)
            for group_indices in index_groups:
                # Single-row groups are the common case for queries with no
                # parent duplication (``Model.objects.all()`` and similar);
                # skip the wrapper list and the no-op ``_recursive_add`` call.
                if len(group_indices) == 1:
                    model = result_rows[group_indices[0]]
                else:
                    model = cls._recursive_add([result_rows[i] for i in group_indices])[
                        0
                    ]
                object.__setattr__(model, "__ormar_excludable__", excludable)
                merged_rows.append(model)

        return merged_rows

    @classmethod
    def merge_two_instances(
        cls, one: "Model", other: "Model", relation_map: Optional[dict] = None
    ) -> "Model":
        """
        Merges current (other) Model and previous one (one) and returns the current
        Model instance with data merged from previous one.

        If needed it's calling itself recurrently and merges also children models.

        :param relation_map: map of models relations to follow
        :type relation_map: dict
        :param one: previous model instance
        :type one: Model
        :param other: current model instance
        :type other: Model
        :return: current Model instance with data merged from previous one.
        :rtype: Model
        """
        relation_map = (
            relation_map
            if relation_map is not None
            else translate_list_to_dict(one._iterate_related_models())
        )
        for field_name in relation_map:
            current_field = getattr(one, field_name)
            other_value = getattr(other, field_name, [])
            if isinstance(current_field, list):
                value_to_set = cls._merge_items_lists(
                    field_name=field_name,
                    current_field=current_field,
                    other_value=other_value,
                    relation_map=relation_map,
                )
                setattr(other, field_name, value_to_set)
            elif (
                isinstance(current_field, ormar.Model)
                and isinstance(other_value, ormar.Model)
                and current_field.pk == other_value.pk
            ):
                setattr(
                    other,
                    field_name,
                    cls.merge_two_instances(
                        current_field,
                        other_value,
                        relation_map=cast(
                            Optional[dict],
                            skip_ellipsis(relation_map, field_name, default=dict()),
                        ),
                    ),
                )
        other.set_save_status(True)
        return other

    @classmethod
    def _merge_items_lists(
        cls,
        field_name: str,
        current_field: list,
        other_value: list,
        relation_map: Optional[dict],
    ) -> list:
        """
        Takes two list of nested models and process them going deeper
        according with the map.

        If model from one's list is in other -> they are merged with relations
        to follow passed from map.

        If one's model is not in other it's simply appended to the list.

        :param field_name: name of the current relation field
        :type field_name: str
        :param current_field: list of nested models from one model
        :type current_field: list[Model]
        :param other_value: list of nested models from other model
        :type other_value: list[Model]
        :param relation_map: map of relations to follow
        :type relation_map: dict
        :return: merged list of models
        :rtype: list[Model]
        """
        current_pks = [getattr(m, "pk", None) for m in current_field]
        other_pks = [getattr(m, "pk", None) for m in other_value]
        plan = ormar_rust_utils.plan_merge_items_lists(current_pks, other_pks)
        value_to_set = list(other_value)
        nested_relation_map = cast(
            Optional[dict],
            skip_ellipsis(relation_map, field_name, default=dict()),
        )
        for cur_idx, other_idx in plan:
            cur_item = current_field[cur_idx]
            if other_idx is not None:
                # ``other_idx`` is the destination slot the Rust planner
                # already identified — write the merged instance there in
                # place rather than rebuilding ``value_to_set`` with a pk
                # filter (which was O(N) per match).
                value_to_set[other_idx] = cls.merge_two_instances(
                    cur_item,
                    cast("Model", other_value[other_idx]),
                    relation_map=nested_relation_map,
                )
            else:
                value_to_set.append(cur_item)
        return value_to_set
