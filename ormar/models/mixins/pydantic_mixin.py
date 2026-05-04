import copy
import string
from random import choices
from typing import TYPE_CHECKING, Any, Optional, Union, cast

import pydantic
from pydantic import BaseModel
from pydantic._internal._decorators import DecoratorInfos

from ormar.fields import BaseField, ForeignKeyField, ManyToManyField
from ormar.models.excludable import (
    filter_not_excluded_fields,
    normalize_to_dict,
    skip_ellipsis,
)
from ormar.models.mixins.relation_mixin import RelationMixin  # noqa: I100, I202
from ormar.queryset.utils import translate_list_to_dict


class PydanticMixin(RelationMixin):
    __cache__: dict[str, type[pydantic.BaseModel]] = {}

    if TYPE_CHECKING:  # pragma: no cover
        __pydantic_decorators__: DecoratorInfos

    @classmethod
    def get_pydantic(
        cls,
        *,
        include: Union[set, dict, None] = None,
        exclude: Union[set, dict, None] = None,
    ) -> type[pydantic.BaseModel]:
        """
        Returns a pydantic model out of ormar model.

        Converts also nested ormar models into pydantic models.

        Can be used to fully exclude certain fields in fastapi response and requests.

        :param include: fields of own and nested models to include
        :type include: Union[set, dict, None]
        :param exclude: fields of own and nested models to exclude
        :type exclude: Union[set, dict, None]
        """
        relation_map = translate_list_to_dict(cls._iterate_related_models())

        return cls._convert_ormar_to_pydantic(
            include=include, exclude=exclude, relation_map=relation_map
        )

    @classmethod
    def _convert_ormar_to_pydantic(
        cls,
        relation_map: dict[str, Any],
        include: Union[set, dict, None] = None,
        exclude: Union[set, dict, None] = None,
    ) -> type[pydantic.BaseModel]:
        include = normalize_to_dict(include)
        exclude = normalize_to_dict(exclude)
        fields_dict: dict[str, Any] = dict()
        defaults: dict[str, Any] = dict()
        fields_to_process = filter_not_excluded_fields(
            fields={*cls.ormar_config.model_fields.keys()},
            include=include,
            exclude=exclude,
        )
        fields_to_process.sort(
            key=lambda x: list(cls.ormar_config.model_fields.keys()).index(x)
        )

        cache_key = f"{cls.__name__}_{str(include)}_{str(exclude)}"
        if cache_key in cls.__cache__:
            return cls.__cache__[cache_key]

        for name in fields_to_process:
            field = cls._determine_pydantic_field_type(
                name=name,
                defaults=defaults,
                include=include,
                exclude=exclude,
                relation_map=relation_map,
            )
            if field is not None:
                fields_dict[name] = field
        model = type(
            f"{cls.__name__}_{''.join(choices(string.ascii_uppercase, k=3))}",
            (pydantic.BaseModel,),
            {"__annotations__": fields_dict, **defaults},
        )
        model = cast(type[pydantic.BaseModel], model)
        cls._copy_field_validators(model=model)
        cls.__cache__[cache_key] = model
        return model

    @classmethod
    def _determine_pydantic_field_type(
        cls,
        name: str,
        defaults: dict,
        include: Union[set, dict, None],
        exclude: Union[set, dict, None],
        relation_map: dict[str, Any],
    ) -> Any:
        field = cls.ormar_config.model_fields[name]
        target: Any = None
        if field.is_relation and name in relation_map:
            target, default = cls._determined_included_relation_field_type(
                name=name,
                field=field,
                include=include,
                exclude=exclude,
                defaults=defaults,
                relation_map=relation_map,
            )
        elif not field.is_relation:
            defaults[name] = cls.model_fields[name].default  # type: ignore
            target = field.__type__
        if target is not None and field.nullable:
            target = Optional[target]
        return target

    @classmethod
    def _determined_included_relation_field_type(
        cls,
        name: str,
        field: Union[BaseField, ForeignKeyField, ManyToManyField],
        include: Union[set, dict, None],
        exclude: Union[set, dict, None],
        defaults: dict,
        relation_map: dict[str, Any],
    ) -> tuple[type[BaseModel], dict]:
        target = field.to._convert_ormar_to_pydantic(
            include=skip_ellipsis(include, name),
            exclude=skip_ellipsis(exclude, name),
            relation_map=cast(
                dict[str, Any], skip_ellipsis(relation_map, name, default=dict())
            ),
        )
        if field.is_multi or field.virtual:
            target = list[target]  # type: ignore
        if field.nullable:
            defaults[name] = None
        return target, defaults

    @classmethod
    def _copy_field_validators(cls, model: type[pydantic.BaseModel]) -> None:
        """
        Copy field validators from ormar model to generated pydantic model.
        """
        filed_names = list(model.model_fields.keys())
        cls.copy_selected_validators_type(
            model=model, fields=filed_names, validator_type="field_validators"
        )
        cls.copy_selected_validators_type(
            model=model, fields=filed_names, validator_type="validators"
        )

        class_validators = cls.__pydantic_decorators__.root_validators
        model.__pydantic_decorators__.root_validators.update(
            copy.deepcopy(class_validators)
        )
        model_validators = cls.__pydantic_decorators__.model_validators
        model.__pydantic_decorators__.model_validators.update(
            copy.deepcopy(model_validators)
        )
        model.model_rebuild(force=True)

    @classmethod
    def copy_selected_validators_type(
        cls, model: type[pydantic.BaseModel], fields: list[str], validator_type: str
    ) -> None:
        """
        Copy field validators from ormar model to generated pydantic model.
        """
        validators = getattr(cls.__pydantic_decorators__, validator_type)
        for name, decorator in validators.items():
            if any(field_name in decorator.info.fields for field_name in fields):
                copied_decorator = copy.deepcopy(decorator)
                copied_decorator.info.fields = [
                    field_name
                    for field_name in decorator.info.fields
                    if field_name in fields
                ]
                getattr(model.__pydantic_decorators__, validator_type)[name] = (
                    copied_decorator
                )
