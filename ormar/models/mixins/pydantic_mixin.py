import copy
import string
from functools import partial
from random import choices
from typing import TYPE_CHECKING, Annotated, Any, Optional, Union, cast

import pydantic
from pydantic import BeforeValidator
from pydantic._internal._decorators import DecoratorInfos

from ormar.fields import BaseField, ForeignKeyField, ManyToManyField
from ormar.models.excludable import (
    FlattenMap,
    filter_not_excluded_fields,
    normalize_to_dict,
    resolve_flatten_map,
    skip_ellipsis,
)
from ormar.models.helpers.pydantic import extract_pk_value
from ormar.models.mixins.relation_mixin import RelationMixin  # noqa: I100, I202
from ormar.queryset.utils import translate_list_to_dict

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model
    from ormar.queryset.field_accessor import FieldAccessor


class PydanticMixin(RelationMixin):
    __cache__: dict[tuple[type, str, str, str], type[pydantic.BaseModel]] = {}

    if TYPE_CHECKING:  # pragma: no cover
        __pydantic_decorators__: DecoratorInfos

    @classmethod
    def get_pydantic(
        cls,
        *,
        include: Union[set, dict, None] = None,
        exclude: Union[set, dict, None] = None,
        flatten_fields: Union[
            set, list, str, tuple, dict, "FieldAccessor", None
        ] = None,
        flatten_all: bool = False,
    ) -> type[pydantic.BaseModel]:
        """
        Returns a pydantic model out of ormar model.

        Converts also nested ormar models into pydantic models.

        Can be used to fully exclude certain fields in fastapi response and requests.

        Relations listed in ``flatten_fields`` (or all relations with
        ``flatten_all``) are represented by the related model's primary key
        (or a list of primary keys for many-to-many and reverse relations)
        instead of a nested pydantic model, matching the output of
        ``model_dump`` called with the same arguments.

        :param include: fields of own and nested models to include
        :type include: Union[set, dict, None]
        :param exclude: fields of own and nested models to exclude
        :type exclude: Union[set, dict, None]
        :param flatten_fields: relations to represent as their primary key
        :type flatten_fields: Union[set, list, str, tuple, dict, FieldAccessor, None]
        :param flatten_all: if True every relation is represented as its pk
        :type flatten_all: bool
        :raises QueryDefinitionError: on invalid flatten paths or when a
            flattened relation has sub-field include/exclude
        """
        relation_map = translate_list_to_dict(cls._iterate_related_models())
        flatten_map = resolve_flatten_map(
            model_cls=cast(type["Model"], cls),
            flatten_fields=flatten_fields,
            flatten_all=flatten_all,
        )
        if flatten_map is not None:
            flatten_map.check_vs_selector(normalize_to_dict(include), "include")
            flatten_map.check_vs_selector(normalize_to_dict(exclude), "exclude")

        return cls._convert_ormar_to_pydantic(
            include=include,
            exclude=exclude,
            flatten_map=flatten_map,
            relation_map=relation_map,
        )

    @classmethod
    def _convert_ormar_to_pydantic(
        cls,
        relation_map: dict[str, Any],
        include: Union[set, dict, None] = None,
        exclude: Union[set, dict, None] = None,
        flatten_map: Optional[FlattenMap] = None,
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

        cache_key = (cls, str(include), str(exclude), str(flatten_map))
        if cache_key in cls.__cache__:
            return cls.__cache__[cache_key]

        for name in fields_to_process:
            field = cls._determine_pydantic_field_type(
                name=name,
                defaults=defaults,
                include=include,
                exclude=exclude,
                flatten_map=flatten_map,
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
        cls._copy_computed_fields(model=model, include=include, exclude=exclude)
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
        flatten_map: Optional[FlattenMap],
        relation_map: dict[str, Any],
    ) -> Any:
        field = cls.ormar_config.model_fields[name]
        target: Any = None
        if field.is_relation and name in relation_map:
            if flatten_map and flatten_map.is_field_flattened(name):
                target = cls._determine_flattened_relation_field_type(
                    name=name, field=field, defaults=defaults
                )
            else:
                target = cls._determined_included_relation_field_type(
                    name=name,
                    field=field,
                    include=include,
                    exclude=exclude,
                    flatten_map=flatten_map.descend(name) if flatten_map else None,
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
        flatten_map: Optional[FlattenMap],
        defaults: dict,
        relation_map: dict[str, Any],
    ) -> Any:
        target = field.to._convert_ormar_to_pydantic(
            include=skip_ellipsis(include, name),
            exclude=skip_ellipsis(exclude, name),
            flatten_map=flatten_map,
            relation_map=cast(
                dict[str, Any], skip_ellipsis(relation_map, name, default=dict())
            ),
        )
        return cls._wrap_relation_field_type(
            name=name, field=field, target=target, defaults=defaults
        )

    @classmethod
    def _determine_flattened_relation_field_type(
        cls,
        name: str,
        field: Union[BaseField, ForeignKeyField, ManyToManyField],
        defaults: dict,
    ) -> Any:
        """
        Build the pydantic type of a flattened relation: the related
        model's pk type with a before-validator that reduces nested dicts and
        model instances to their pk value.

        :param name: name of the relation field
        :type name: str
        :param field: ormar relation field
        :type field: Union[BaseField, ForeignKeyField, ManyToManyField]
        :param defaults: default values of the generated pydantic model
        :type defaults: dict
        :return: pydantic annotation of the field
        :rtype: Any
        """
        target: Any = Annotated[
            field.to.pk_type(),  # type: ignore
            BeforeValidator(
                partial(extract_pk_value, pkname=field.to.ormar_config.pkname)
            ),
        ]
        return cls._wrap_relation_field_type(
            name=name, field=field, target=target, defaults=defaults
        )

    @staticmethod
    def _wrap_relation_field_type(
        name: str,
        field: Union[BaseField, ForeignKeyField, ManyToManyField],
        target: Any,
        defaults: dict,
    ) -> Any:
        """
        Wrap relation target type in a list for multi and reverse relations and
        register a ``None`` default for nullable relations.

        :param name: name of the relation field
        :type name: str
        :param field: ormar relation field
        :type field: Union[BaseField, ForeignKeyField, ManyToManyField]
        :param target: pydantic type of a single related item
        :type target: Any
        :param defaults: default values of the generated pydantic model
        :type defaults: dict
        :return: pydantic annotation of the field
        :rtype: Any
        """
        if field.is_multi or field.virtual:
            target = list[target]  # type: ignore
        if field.nullable:
            defaults[name] = None
        return target

    @classmethod
    def _copy_computed_fields(
        cls,
        model: type[pydantic.BaseModel],
        include: Optional[dict],
        exclude: Optional[dict],
    ) -> None:
        """
        Copy ``@pydantic.computed_field`` decorated methods from the ormar model
        onto the generated pydantic model so they are exposed in the schema and
        serialization output. Computed fields are filtered with the same
        include/exclude rules as regular fields.

        :param model: pydantic model generated from the ormar model
        :type model: type[pydantic.BaseModel]
        :param include: fields of own and nested models to include
        :type include: Optional[dict]
        :param exclude: fields of own and nested models to exclude
        :type exclude: Optional[dict]
        """
        computed_fields = cls.__pydantic_decorators__.computed_fields
        if not computed_fields:
            return
        names_to_copy = filter_not_excluded_fields(
            fields=set(computed_fields.keys()),
            include=include,
            exclude=exclude,
        )
        for name in names_to_copy:
            decorator = computed_fields[name]
            setattr(model, name, decorator.info.wrapped_property)
            model.__pydantic_decorators__.computed_fields[name] = copy.copy(decorator)

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
