"""
Per-query identity map used when materializing joined database rows into models.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from ormar.models import Model, NewBaseModel


RegistryKey = tuple[Any, str, Any]


class RowRegistry:
    """
    Per-query identity map used while materializing joined rows.

    A model reached through the same relation path with the same pk is built
    once and reused by every row it appears in, instead of being constructed
    per row and merged afterwards. Models below a many-to-many edge are scoped
    to their parent, because the through instance of that link is stored on
    the child. The registry lives only for one result set.

    :ivar instances: built models keyed by ``(scope, table_prefix, pk)``
    :ivar subtree_has_multi: memo of whether the relations selected below a
        ``(model_cls, table_prefix)`` position contain a reverse or
        many-to-many relation that later rows can still extend
    """

    __slots__ = ("instances", "subtree_has_multi")

    def __init__(self) -> None:
        """
        Initialize an empty registry.
        """
        self.instances: dict[RegistryKey, "Model"] = {}
        self.subtree_has_multi: dict[tuple[type, str], bool] = {}


def selects_multi_relation(
    model_cls: type["NewBaseModel"], related_models: Any
) -> bool:
    """
    Check if selected relations below a model contain a reverse foreign key
    or a many-to-many relation, which are the only relations that a repeated
    row of an already built model can still add new children to.

    :param model_cls: model from which the selected relations start
    :type model_cls: type[NewBaseModel]
    :param related_models: selected relations as list or nested dict
    :type related_models: Union[list, dict]
    :return: True if any selected relation on any level is multi or virtual
    :rtype: bool
    """
    model_fields = model_cls.ormar_config.model_fields
    for related in related_models:
        field = model_fields[related]
        if field.is_multi or field.virtual:
            return True
        if isinstance(related_models, dict) and related_models[related]:
            if selects_multi_relation(field.to, related_models[related]):
                return True
    return False
