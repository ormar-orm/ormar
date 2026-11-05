"""
``Prefetch`` attaches a queryset to a prefetched relation, so the related models
are filtered, ordered and narrowed to selected fields while they are loaded::

    Author.objects.prefetch_related(
        Prefetch("books", queryset=Book.objects.filter(year__gte=2000))
    )
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from ormar.queryset.queryset import QuerySet


@dataclass(frozen=True)
class Prefetch:
    """
    A relation to prefetch together with the queryset its models are loaded
    with.

    :ivar relation: relation path, the same ``__`` separated string
        ``prefetch_related`` accepts
    :ivar queryset: queryset of the related model; its filters, excludes,
        ordering and ``fields`` / ``exclude_fields`` apply to the prefetched
        models
    """

    relation: str
    queryset: "QuerySet[Any]"
