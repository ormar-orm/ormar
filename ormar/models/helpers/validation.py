import decimal
import numbers
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

import pydantic

from ormar.models.helpers.models import config_field_not_set
from ormar.queryset.utils import translate_list_to_dict

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model
    from ormar.fields import BaseField


def generate_model_example(
    model: type["Model"], relation_map: Optional[dict] = None
) -> dict:
    """
    Generates example to be included in schema in fastapi.

    :param model: ormar.Model
    :type model: type["Model"]
    :param relation_map: dict with relations to follow
    :type relation_map: Optional[dict]
    :return: dict with example values
    :rtype: dict[str, int]
    """
    example: dict[str, Any] = dict()
    relation_map = (
        relation_map
        if relation_map is not None
        else translate_list_to_dict(model._iterate_related_models())
    )
    for name, field in model.ormar_config.model_fields.items():
        populates_sample_fields_values(
            example=example, name=name, field=field, relation_map=relation_map
        )
    to_exclude = {name for name in model.ormar_config.model_fields}
    pydantic_repr = generate_pydantic_example(pydantic_model=model, exclude=to_exclude)
    example.update(pydantic_repr)

    return example


def populates_sample_fields_values(
    example: dict[str, Any],
    name: str,
    field: "BaseField",
    relation_map: Optional[dict] = None,
) -> None:
    """
    Iterates the field and sets fields to sample values

    :param field: ormar field
    :type field: BaseField
    :param name: name of the field
    :type name: str
    :param example: example dict
    :type example: dict[str, Any]
    :param relation_map: dict with relations to follow
    :type relation_map: Optional[dict]
    """
    if not field.is_relation:
        is_bytes_str = field.__type__ is bytes and field.represent_as_base64_str
        example[name] = field.__sample__ if not is_bytes_str else "string"
    elif isinstance(relation_map, dict) and name in relation_map:
        example[name] = get_nested_model_example(
            name=name, field=field, relation_map=relation_map
        )


def get_nested_model_example(
    name: str, field: "BaseField", relation_map: dict
) -> Union[list, dict]:
    """
    Gets representation of nested model.

    :param name: name of the field to follow
    :type name: str
    :param field: ormar field
    :type field: BaseField
    :param relation_map: dict with relation map
    :type relation_map: dict
    :return: nested model or list of nested model repr
    :rtype: Union[list, dict]
    """
    value = generate_model_example(field.to, relation_map=relation_map.get(name, {}))
    new_value: Union[list, dict] = [value] if field.is_multi or field.virtual else value
    return new_value


def generate_pydantic_example(
    pydantic_model: type[pydantic.BaseModel], exclude: Optional[set] = None
) -> dict:
    """
    Generates dict with example.

    :param pydantic_model: model to parse
    :type pydantic_model: type[pydantic.BaseModel]
    :param exclude: list of fields to exclude
    :type exclude: Optional[set]
    :return: dict with fields and sample values
    :rtype: dict
    """
    example: dict[str, Any] = dict()
    exclude = exclude or set()
    name_to_check = [
        name for name in pydantic_model.model_fields if name not in exclude
    ]
    for name in name_to_check:
        field = pydantic_model.model_fields[name]
        type_ = field.annotation
        example[name] = get_pydantic_example_repr(type_)
    return example


def get_pydantic_example_repr(type_: Any) -> Any:
    """
    Gets sample representation of pydantic field for example dict.

    :param type_: type of pydantic field
    :type type_: Any
    :return: representation to include in example
    :rtype: Any
    """
    if hasattr(type_, "__origin__"):
        return generate_example_for_nested_types(type_)
    if issubclass(type_, (numbers.Number, decimal.Decimal)):
        return 0
    if issubclass(type_, pydantic.BaseModel):
        return generate_pydantic_example(pydantic_model=type_)
    return "string"


def generate_example_for_nested_types(type_: Any) -> Any:
    """
    Process nested types like Union[X, Y] or list[X]
    """
    if type_.__origin__ == Union:
        return generate_example_for_union(type_=type_)
    if type_.__origin__ is list:
        return [get_pydantic_example_repr(type_.__args__[0])]


def generate_example_for_union(type_: Any) -> Any:
    """
    Generates a pydantic example for Union[X, Y, ...].
    Note that Optional can also be set as Union[X, None]
    """
    values = tuple(
        get_pydantic_example_repr(x) for x in type_.__args__ if x is not type(None)
    )
    return values[0] if len(values) == 1 else values


def overwrite_example_and_description(
    schema: dict[str, Any], model: type["Model"]
) -> None:
    """
    Overwrites the example with properly nested children models.
    Overwrites the description if it's taken from ormar.Model.

    :param schema: schema of current model
    :type schema: dict[str, Any]
    :param model: model class
    :type model: type["Model"]
    """
    schema["example"] = generate_model_example(model=model)


def overwrite_binary_format(schema: dict[str, Any], model: type["Model"]) -> None:
    """
    Overwrites format of the field if it's a LargeBinary field with
    a flag to represent the field as base64 encoded string.

    :param schema: schema of current model
    :type schema: dict[str, Any]
    :param model: model class
    :type model: type["Model"]
    """
    for field_id, prop in schema.get("properties", {}).items():
        if (
            field_id in model._bytes_fields
            and model.ormar_config.model_fields[field_id].represent_as_base64_str
        ):
            prop["format"] = "base64"


def construct_schema_function() -> Callable:
    """
    Modifies model example and description if needed.

    Note that schema extra has to be a function, otherwise it's called to soon
    before all the relations are expanded.

    :return: callable that will be run by pydantic to modify the schema
    :rtype: Callable
    """

    def schema_extra(schema: dict[str, Any], model: type["Model"]) -> None:
        overwrite_example_and_description(schema=schema, model=model)
        overwrite_binary_format(schema=schema, model=model)

    return staticmethod(schema_extra)  # type: ignore


def modify_schema_example(model: type["Model"]) -> None:  # noqa CCR001
    """
    Modifies the schema example in openapi schema.

    :param model: newly constructed Model
    :type model: Model class
    """
    if not config_field_not_set(model=model, field_name="model_fields"):
        model.model_config["json_schema_extra"] = construct_schema_function()
