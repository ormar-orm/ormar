import copy
import warnings
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Optional, Union

import pydantic
from pydantic import ConfigDict
from pydantic.fields import FieldInfo

from ormar.exceptions import ModelDefinitionError  # noqa: I100, I202
from ormar.fields import BaseField
from ormar.models.helpers.rebuild import mark_for_rebuild
from ormar.warnings import OrmarDeprecatedSince031

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model
    from ormar.fields import ManyToManyField


def create_pydantic_field(
    field_name: str, model: type["Model"], model_field: "ManyToManyField"
) -> None:
    """
    Registers pydantic field on through model that leads to passed model
    and is registered as field_name passed.

    Through model is fetched from through attributed on passed model_field.

    :param field_name: field name to register
    :type field_name: str
    :param model: type of field to register
    :type model: Model class
    :param model_field: relation field from which through model is extracted
    :type model_field: ManyToManyField class
    """
    model_field.through.model_fields[field_name] = FieldInfo.from_annotated_attribute(
        annotation=Optional[model],  # type: ignore
        default=None,
    )
    mark_for_rebuild(model=model_field.through)


def populate_pydantic_default_values(attrs: dict) -> tuple[dict, dict]:
    """
    Extracts ormar fields from annotations (deprecated) and from namespace
    dictionary of the class. Fields declared on model are all subclasses of the
    BaseField class.

    Trigger conversion of ormar field into pydantic FieldInfo, which has all needed
    parameters saved.

    Overwrites the annotations of ormar fields to corresponding types declared on
    ormar fields (constructed dynamically for relations).
    Those annotations are later used by pydantic to construct it's own fields.

    :param attrs: current class namespace
    :type attrs: dict
    :return: namespace of the class updated, dict of extracted model_fields
    :rtype: tuple[dict, dict]
    """
    model_fields = {}
    potential_fields = {}

    potential_fields.update(get_potential_fields(attrs))
    for field_name, field in potential_fields.items():
        field.name = field_name
        model_fields[field_name] = field
        attrs["__annotations__"][field_name] = get_field_annotation(field=field)
    return attrs, model_fields


def get_field_annotation(field: BaseField) -> Any:
    """
    Returns the pydantic annotation of an ormar field: the overwritten pydantic
    type if one was given, otherwise the field type, Optional when nullable.

    :param field: ormar field
    :type field: BaseField
    :return: annotation used by pydantic
    :rtype: Any
    """
    if (
        field.__pydantic_type__ is not None
        and field.__type__ != field.__pydantic_type__
    ):
        return field.__pydantic_type__
    return field.__type__ if not field.nullable else Optional[field.__type__]


def apply_not_null_defaults(attrs: dict, model_fields: dict, model_name: str) -> dict:
    """
    Makes fields with a simple literal default NOT NULL and non-Optional when
    ``ormar_config.not_null_defaults`` is True, unless nullable was set
    explicitly on the field. When the flag is left as None the fields stay
    nullable and one deprecation warning lists them. Abstract and proxy models
    are skipped, the rule runs for each concrete model with its own config.

    Changed fields are copies, so fields shared with parent classes stay
    untouched, and they are put in the class namespace so that pydantic builds
    them with the new annotation.

    :param attrs: namespace of the class being constructed
    :type attrs: dict
    :param model_fields: ormar fields of the model, including inherited ones
    :type model_fields: dict[str, BaseField]
    :param model_name: name of the model class
    :type model_name: str
    :return: model fields with the changed fields replaced
    :rtype: dict[str, BaseField]
    """
    ormar_config = attrs["ormar_config"]
    if (
        ormar_config.abstract
        or ormar_config.proxy
        or ormar_config.not_null_defaults is False
    ):
        return model_fields
    field_names = [
        field_name
        for field_name, field in model_fields.items()
        if not field.nullable_explicit
        and field.get_literal_server_default() is not None
    ]
    if not field_names:
        return model_fields
    if ormar_config.not_null_defaults is None:
        warnings.warn(
            OrmarDeprecatedSince031(
                f"Model {model_name}: fields with a literal default "
                f"({', '.join(field_names)}) will become NOT NULL and non-Optional "
                f"by default. Set OrmarConfig(not_null_defaults=True) to opt in "
                f"now, or nullable=True on a field to keep it nullable"
            ),
            stacklevel=3,
        )
        return model_fields
    updated_fields = dict(model_fields)
    for field_name in field_names:
        field = copy.copy(model_fields[field_name])
        field.nullable = False
        if not field.sql_nullable_explicit:
            field.sql_nullable = False
        updated_fields[field_name] = field
        attrs[field_name] = field
        attrs["__annotations__"][field_name] = get_field_annotation(field=field)
    return updated_fields


def merge_or_generate_pydantic_config(attrs: dict, name: str) -> None:
    """
    Checks if the user provided pydantic Config,
    and if he did merges it with the default one.

    Updates the attrs in place with a new config.

    :rtype: None
    """
    default_config = get_pydantic_base_orm_config()
    if "model_config" in attrs:
        provided_config = attrs["model_config"]
        if not isinstance(provided_config, dict):
            raise ModelDefinitionError(
                f"Config provided for class {name} has to be a dictionary."
            )

        config = {**default_config, **provided_config}
        attrs["model_config"] = config
    else:
        attrs["model_config"] = default_config


def get_pydantic_base_orm_config() -> pydantic.ConfigDict:
    """
    Returns empty pydantic Config with orm_mode set to True.

    :return: empty default config with orm_mode set.
    :rtype: pydantic Config
    """

    return ConfigDict(validate_assignment=True, ser_json_bytes="base64")


def get_potential_fields(attrs: Union[dict, MappingProxyType]) -> dict:
    """
    Gets all the fields in current class namespace that are Fields.

    :param attrs: current class namespace
    :type attrs: dict
    :return: extracted fields that are ormar Fields
    :rtype: dict
    """
    return {
        k: v
        for k, v in attrs.items()
        if (
            (isinstance(v, type) and issubclass(v, BaseField))
            or isinstance(v, BaseField)
        )
    }


def extract_pk_value(value: Any, fields: tuple[str, ...]) -> Any:
    """
    Reduce a related model, its dict representation or a bare key to the
    value representing it in flattened output: the value of its single key
    field, or a dict of its key fields when there are several (a key tuple is
    turned into that dict).

    Used as a before-validator on fields generated by ``get_pydantic`` with
    ``flatten_fields`` / ``flatten_all``, so both request payloads (bare key)
    and dumped ormar models (nested dict or model instance) validate against
    the flattened type.

    :param value: related model instance, dict or key value
    :type value: Any
    :param fields: fields representing the related model (``flat_fields()``)
    :type fields: tuple[str, ...]
    :return: flattened value, or the original value if it cannot be reduced
    :rtype: Any
    """
    if len(fields) > 1:
        if isinstance(value, pydantic.BaseModel):
            return {field: getattr(value, field, None) for field in fields}
        if isinstance(value, dict):
            return {field: value[field] for field in fields if field in value}
        if not isinstance(value, (tuple, list)) or len(value) != len(fields):
            raise ValueError(
                f"Expected a key of {len(fields)} values {list(fields)}, got {value!r}."
            )
        return dict(zip(fields, value))
    (pkname,) = fields
    if isinstance(value, dict):
        return value.get(pkname, value)
    if isinstance(value, pydantic.BaseModel):
        return getattr(value, pkname, value)
    return value
