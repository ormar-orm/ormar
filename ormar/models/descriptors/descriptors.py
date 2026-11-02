import base64
from typing import TYPE_CHECKING, Any, Optional, cast

from ormar.fields.parsers import decode_bytes, encode_json

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model


class PydanticDescriptor:
    """
    Pydantic descriptor simply delegates everything to pydantic model
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __get__(self, instance: "Model", owner: type["Model"]) -> Any:
        value = instance.__dict__.get(self.name, None)
        return value

    def __set__(self, instance: "Model", value: Any) -> None:
        instance._internal_set(self.name, value)
        instance.set_save_status(False)


class JsonDescriptor:
    """
    Json descriptor dumps/loads strings to actual data on write/read
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __get__(self, instance: "Model", owner: type["Model"]) -> Any:
        value = instance.__dict__.get(self.name, None)
        return value

    def __set__(self, instance: "Model", value: Any) -> None:
        value = encode_json(value)
        instance._internal_set(self.name, value)
        instance.set_save_status(False)


class BytesDescriptor:
    """
    Bytes descriptor converts strings to bytes on write and converts bytes to str
    if represent_as_base64_str flag is set, so the value can be dumped to json
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __get__(self, instance: "Model", owner: type["Model"]) -> Any:
        value = instance.__dict__.get(self.name, None)
        field = instance.ormar_config.model_fields[self.name]
        if (
            value is not None
            and field.represent_as_base64_str
            and not isinstance(value, str)
        ):
            value = base64.b64encode(value).decode()
        return value

    def __set__(self, instance: "Model", value: Any) -> None:
        field = instance.ormar_config.model_fields[self.name]
        if isinstance(value, str):
            value = decode_bytes(
                value=value, represent_as_string=field.represent_as_base64_str
            )
        instance._internal_set(self.name, value)
        instance.set_save_status(False)


class PkDescriptor:
    """
    As of now it's basically a copy of PydanticDescriptor but that will
    change in the future with multi column primary keys
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __get__(self, instance: "Model", owner: type["Model"]) -> Any:
        value = instance.__dict__.get(self.name, None)
        return value

    def __set__(self, instance: "Model", value: Any) -> None:
        instance._internal_set(self.name, value)
        instance.set_save_status(False)


class RelationDescriptor:
    """
    Relation descriptor expands the relation to initialize the related model
    before setting it to __dict__. Note that expanding also registers the
    related model in RelationManager.
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __get__(self, instance: "Model", owner: type["Model"]) -> Any:
        if self.name in instance._orm:
            return instance._orm.get(self.name)  # type: ignore
        return None  # pragma no cover

    def __set__(self, instance: "Model", value: Any) -> None:
        field = instance.ormar_config.model_fields[self.name]
        field.expand_relationship(value=value, child=instance)

        if (
            value is None
            and field.nullable
            and not field.virtual
            and not field.is_multi
            and self.name in instance._orm
        ):
            previous = cast(Optional["Model"], instance._orm.get(self.name))
            if previous is not None:
                instance._orm.remove(self.name, previous)
                # keep the other side of the relation in sync, so the already
                # loaded parent does not keep pointing at a child whose FK is None
                relation_name = field.get_related_name()
                relation = previous._orm._get(relation_name)
                if relation is not None:
                    relation.remove(instance)

        if not isinstance(instance.__dict__.get(self.name), list):
            instance.set_save_status(False)
