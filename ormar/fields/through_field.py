import sys
from typing import TYPE_CHECKING, Any, Optional, Union

from ormar.fields.base import BaseField
from ormar.fields.foreign_key import ForeignKeyField
from ormar.fields.relation_kind import THROUGH, RelationKind

if TYPE_CHECKING:  # pragma no cover
    from pydantic.typing import ForwardRef

    from ormar import Model

    if sys.version_info < (3, 7):
        ToType = type[Model]
    else:
        ToType = Union[type[Model], ForwardRef]


def Through(  # noqa CFQ002
    to: "ToType",
    *,
    name: Optional[str] = None,
    related_name: Optional[str] = None,
    **kwargs: Any,
) -> Any:
    """
    Despite a name it's a function that returns constructed ThroughField.
    It's a special field populated only for m2m relations.
    Accepts number of relation setting parameters as well as all BaseField ones.

    :param to: target related ormar Model
    :type to: Model class
    :param name: name of the database field - later called alias
    :type name: str
    :param related_name: name of reversed FK relation populated for you on to model
    :type related_name: str
    It is for reversed FK and auto generated FK on through model in Many2Many relations.
    :param kwargs: all other args to be populated by BaseField
    :type kwargs: Any
    :return: ormar ForeignKeyField with relation to selected model
    :rtype: ForeignKeyField
    """
    nullable = kwargs.pop("nullable", False)
    owner = kwargs.pop("owner", None)
    namespace = dict(
        __type__=to,
        to=to,
        through=None,
        alias=name,
        name=kwargs.pop("real_name", None),
        related_name=related_name,
        virtual=True,
        owner=owner,
        nullable=nullable,
        unique=False,
        column_type=None,
        primary_key=False,
        index=False,
        default=None,
        server_default=None,
        is_relation=True,
        is_through=True,
    )

    Field = type("Through", (ThroughField, BaseField), {})
    return Field(**namespace)


class ThroughField(ForeignKeyField):  # type: ignore[misc]
    """
    Field class used to access ManyToMany model through model.
    """

    @property
    def kind(self) -> RelationKind:
        """
        :return: ``THROUGH``, the field leading from a model to its link table
        :rtype: RelationKind
        """
        return THROUGH
