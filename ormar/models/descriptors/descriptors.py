import base64
from typing import TYPE_CHECKING, Any, Optional, cast

from ormar.fields.parsers import decode_bytes, encode_json

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model
    from ormar.models.primary_key import PrimaryKey


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
    Descriptor of ``Model.pk``, reading and writing the key value through the
    model's ``PrimaryKey``.
    """

    def __init__(self, *, key: "PrimaryKey") -> None:
        """
        :param key: primary key of the model class
        :type key: PrimaryKey
        """
        self.key = key
        self.read = key.value

    def __get__(self, instance: "Model", owner: type["Model"]) -> Any:
        """
        :param instance: model instance
        :type instance: Model
        :param owner: model class
        :type owner: type[Model]
        :return: key value of the instance; on the class the descriptor itself
            for a composite key (a single key resolves to a field accessor
            for python-style filters through the model metaclass)
        :rtype: Any
        """
        if instance is None:
            if self.key.is_composite:
                return self
            raise AttributeError("pk")
        return self.read(instance)

    def __set__(self, instance: "Model", value: Any) -> None:
        """
        Sets every key field of the instance and marks it as not saved.

        :param instance: model instance
        :type instance: Model
        :param value: new key value
        :type value: Any
        """
        instance.remember_persisted_key()
        for field_name, part in self.key.to_dict(value=value).items():
            if field_name in self.key.relation_parts:
                setattr(instance, field_name, part)
            else:
                instance._internal_set(name=field_name, value=part)
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

        if not isinstance(instance.__dict__.get(self.name), list):
            instance.set_save_status(False)
