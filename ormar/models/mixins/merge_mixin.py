from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model
    from ormar.models.excludable import ExcludableItems


class MergeModelMixin:
    """
    Used to collect models instances returned by database,
    but already initialized to ormar Models.

    Rows of one model returned by a join are materialized into a single shared
    instance (see ``RowRegistry``), so the same main model can repeat in the
    list of materialized rows; in the end all main models should be unique.
    """

    @classmethod
    def merge_instances_list(
        cls,
        result_rows: list["Model"],
        *,
        excludable: "ExcludableItems",
    ) -> list["Model"]:
        """
        Collapses a list of materialized rows into a list of unique models,
        keeping the order of first occurrence.

        Each top-level model also has the queryset's ``ExcludableItems``
        attached via the ``__ormar_excludable__`` slot so a bare ``model_dump()``
        on the result can resolve the (lazily cached) flatten map without going
        back through the queryset.

        :param result_rows: list of models materialized from consecutive rows,
            where rows of the same main model resolve to the same instance
        :type result_rows: list["Model"]
        :param excludable: ExcludableItems to attach to every top-level
            instance via the ``__ormar_excludable__`` slot
        :type excludable: ExcludableItems
        :return: list of unique main models
        :rtype: list["Model"]
        """
        merged_rows: list["Model"] = []
        seen: set[int] = set()
        for model in result_rows:
            if id(model) in seen:
                continue
            seen.add(id(model))
            object.__setattr__(model, "__ormar_excludable__", excludable)
            merged_rows.append(model)
        return merged_rows
