"""
Key-only models: instances of a related model with only the fields a relation
references (its primary key) set, built when a row holds a foreign key value
but the related model itself was not selected.
"""

import string
from random import choices
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel, create_model

if TYPE_CHECKING:  # pragma: no cover
    from ormar.models import Model


def key_only_model_type(
    *, target: type["Model"], fields: tuple[str, ...]
) -> type[BaseModel]:
    """
    Build the pydantic model used to validate a relation value given as a
    key-only model, with one field per referenced field of the target.

    :param target: model the relation leads to
    :type target: type[Model]
    :param fields: referenced fields of the target
    :type fields: tuple[str, ...]
    :return: pydantic model holding only the referenced fields
    :rtype: type[BaseModel]
    """
    suffix = "".join(choices(string.ascii_uppercase, k=6)).lower()
    model_fields = target.ormar_config.model_fields
    definitions: dict[str, Any] = {
        field: (model_fields[field].__type__, None) for field in fields
    }
    return create_model(  # type: ignore[call-overload, no-any-return]
        f"PkOnly{target.get_name(lower=False)}{suffix}",
        __module__=target.__module__,
        **definitions,
    )


def key_only_instance(*, target: type["Model"], values: dict[str, Any]) -> "Model":
    """
    Create an instance of the target with only the given key fields set,
    skipping validation of the other fields. Required relations of the target
    get placeholder instances keyed with ``-1``.

    :param target: model to instantiate
    :type target: type[Model]
    :param values: key field values by field name
    :type values: dict[str, Any]
    :return: key-only instance of the target
    :rtype: Model
    """
    init_dict = {
        **values,
        **{
            name: key_only_instance(
                target=field.to, values=field.to.ormar_config.pk.to_dict(-1)
            )
            for name, field in target.ormar_config.model_fields.items()
            if field.is_relation and not field.nullable and not field.virtual
        },
    }
    return cast(
        "Model", target._internal_construct(_pk_only=True, _excluded=None, **init_dict)
    )
