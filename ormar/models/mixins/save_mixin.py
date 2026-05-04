import base64
import uuid
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Collection, Optional, Union, cast

from pydantic.plugin._schema_validator import (
    PluggableSchemaValidator,
    create_schema_validator,
)
from pydantic_core import CoreSchema, SchemaValidator

import ormar  # noqa: I100, I202
from ormar.exceptions import ModelPersistenceError
from ormar.fields.parsers import encode_json
from ormar.models.excludable import skip_ellipsis
from ormar.models.mixins import AliasMixin
from ormar.models.mixins.relation_mixin import RelationMixin

if TYPE_CHECKING:  # pragma: no cover
    from ormar import ForeignKeyField, Model


class SavePrepareMixin(RelationMixin, AliasMixin):
    """
    Used to prepare models to be saved in database
    """

    if TYPE_CHECKING:  # pragma: nocover
        _json_fields: set[str]
        _bytes_fields: set[str]
        _onupdate_fields: set[str]
        __pydantic_core_schema__: CoreSchema
        __ormar_fields_validators__: Optional[
            dict[str, Union[SchemaValidator, PluggableSchemaValidator]]
        ]

    @classmethod
    def prepare_model_to_save(cls, new_kwargs: dict) -> dict:
        """
        Combines all preparation methods before saving.
        Removes primary key for if it's nullable or autoincrement pk field,
        and it's set to None.
        Substitute related models with their primary key values as fk column.
        Populates the default values for field with default set and no value.
        Translate columns into aliases (db names).

        :param new_kwargs: dictionary of model that is about to be saved
        :type new_kwargs: dict[str, str]
        :return: dictionary of model that is about to be saved
        :rtype: dict[str, str]
        """
        new_kwargs = cls._remove_pk_from_kwargs(new_kwargs)
        new_kwargs = cls._remove_not_ormar_fields(new_kwargs)
        new_kwargs = cls.substitute_models_with_pks(new_kwargs)
        new_kwargs = cls.populate_default_values(new_kwargs)
        new_kwargs = cls.reconvert_str_to_bytes(new_kwargs)
        new_kwargs = cls.translate_columns_to_aliases(new_kwargs)
        return new_kwargs

    @classmethod
    def prepare_model_to_update(cls, new_kwargs: dict) -> dict:
        """
        Combines all preparation methods before updating.
        :param new_kwargs: dictionary of model that is about to be saved
        :type new_kwargs: dict[str, str]
        :return: dictionary of model that is about to be updated
        :rtype: dict[str, str]
        """
        new_kwargs = cls.parse_non_db_fields(new_kwargs)
        new_kwargs = cls.substitute_models_with_pks(new_kwargs)
        new_kwargs = cls.reconvert_str_to_bytes(new_kwargs)
        new_kwargs = cls.dump_all_json_fields_to_str(new_kwargs)
        new_kwargs = cls.translate_columns_to_aliases(new_kwargs)
        new_kwargs = cls.translate_enum_columns(new_kwargs)
        return new_kwargs

    @classmethod
    def translate_enum_columns(cls, new_kwargs: dict) -> dict:
        for key, value in new_kwargs.items():
            if isinstance(value, Enum):
                new_kwargs[key] = value.name
        return new_kwargs

    @classmethod
    def _remove_not_ormar_fields(cls, new_kwargs: dict) -> dict:
        """
        Removes primary key for if it's nullable or autoincrement pk field,
        and it's set to None.

        :param new_kwargs: dictionary of model that is about to be saved
        :type new_kwargs: dict[str, str]
        :return: dictionary of model that is about to be saved
        :rtype: dict[str, str]
        """
        ormar_fields = {k for k, v in cls.ormar_config.model_fields.items()}
        new_kwargs = {k: v for k, v in new_kwargs.items() if k in ormar_fields}
        return new_kwargs

    @classmethod
    def _remove_pk_from_kwargs(cls, new_kwargs: dict) -> dict:
        """
        Removes primary key for if it's nullable or autoincrement pk field,
        and it's set to None.

        :param new_kwargs: dictionary of model that is about to be saved
        :type new_kwargs: dict[str, str]
        :return: dictionary of model that is about to be saved
        :rtype: dict[str, str]
        """
        pkname = cls.ormar_config.pkname
        pk = cls.ormar_config.model_fields[pkname]
        if new_kwargs.get(pkname, ormar.Undefined) is None and (
            pk.nullable or pk.autoincrement
        ):
            del new_kwargs[pkname]
        return new_kwargs

    @classmethod
    def parse_non_db_fields(cls, model_dict: dict) -> dict:
        """
        Receives dictionary of model that is about to be saved and changes uuid fields
        to strings in bulk_update.

        :param model_dict: dictionary of model that is about to be saved
        :type model_dict: dict
        :return: dictionary of model that is about to be saved
        :rtype: dict
        """
        for name, field in cls.ormar_config.model_fields.items():
            if field.__type__ == uuid.UUID and name in model_dict:
                parsers = {"string": lambda x: str(x), "hex": lambda x: "%.32x" % x.int}
                uuid_format = field.column_type.uuid_format
                parser: Callable[..., Any] = parsers.get(uuid_format, lambda x: x)
                model_dict[name] = parser(model_dict[name])
        return model_dict

    @classmethod
    def substitute_models_with_pks(cls, model_dict: dict) -> dict:  # noqa  CCR001
        """
        Receives dictionary of model that is about to be saved and changes all related
        models that are stored as foreign keys to their fk value.

        :param model_dict: dictionary of model that is about to be saved
        :type model_dict: dict
        :return: dictionary of model that is about to be saved
        :rtype: dict
        """
        for field in cls.extract_related_names():
            field_value = model_dict.get(field, None)
            if field_value is not None:
                target_field = cls.ormar_config.model_fields[field]
                target_pkname = target_field.to.ormar_config.pkname
                if isinstance(field_value, ormar.Model):  # pragma: no cover
                    pk_value = getattr(field_value, target_pkname)
                    if not pk_value:
                        raise ModelPersistenceError(
                            f"You cannot save {field_value.get_name()} "
                            f"model without pk set!"
                        )
                    model_dict[field] = pk_value
                elif isinstance(field_value, (list, dict)) and field_value:
                    if isinstance(field_value, list):
                        model_dict[field] = [
                            target.get(target_pkname) for target in field_value
                        ]
                    else:
                        model_dict[field] = field_value.get(target_pkname)
                else:
                    model_dict.pop(field, None)
        return model_dict

    @classmethod
    def reconvert_str_to_bytes(cls, model_dict: dict) -> dict:
        """
        Receives dictionary of model that is about to be saved and changes
        all bytes fields that are represented as strings back into bytes.

        :param model_dict: dictionary of model that is about to be saved
        :type model_dict: dict
        :return: dictionary of model that is about to be saved
        :rtype: dict
        """
        bytes_base64_fields = {
            name
            for name, field in cls.ormar_config.model_fields.items()
            if field.represent_as_base64_str
        }
        for key, value in model_dict.items():
            if key in cls._bytes_fields and isinstance(value, str):
                model_dict[key] = (
                    value.encode("utf-8")
                    if key not in bytes_base64_fields
                    else base64.b64decode(value)
                )
        return model_dict

    @classmethod
    def dump_all_json_fields_to_str(cls, model_dict: dict) -> dict:
        """
        Receives dictionary of model that is about to be saved and changes
        all json fields into strings

        :param model_dict: dictionary of model that is about to be saved
        :type model_dict: dict
        :return: dictionary of model that is about to be saved
        :rtype: dict
        """
        for key, value in model_dict.items():
            if key in cls._json_fields:
                model_dict[key] = encode_json(value)
        return model_dict

    @classmethod
    def serialize_nested_models_json_fields(cls, kwargs: dict) -> dict:
        """
        Serializes JSON fields in nested model instances to strings.
        This ensures that when models with JSON fields are nested in other models,
        the JSON values are properly serialized as strings for Pydantic validation.

        Needed as sqlalchemy auto parses the JSON fields.

        :param kwargs: dictionary of keyword arguments
        :type kwargs: Dict
        :return: modified kwargs with serialized JSON fields in nested models
        :rtype: Dict
        """
        for key, value in kwargs.items():
            if isinstance(value, ormar.Model) and hasattr(value, "_json_fields"):
                value.dump_all_json_fields_to_str(value.__dict__)
        return kwargs

    @classmethod
    def populate_default_values(cls, new_kwargs: dict) -> dict:
        """
        Receives dictionary of model that is about to be saved and populates the default
        value on the fields that have the default value set, but no actual value was
        passed by the user.

        :param new_kwargs: dictionary of model that is about to be saved
        :type new_kwargs: dict
        :return: dictionary of model that is about to be saved
        :rtype: dict
        """
        for field_name, field in cls.ormar_config.model_fields.items():
            if field_name not in new_kwargs and field.has_default(use_server=False):
                new_kwargs[field_name] = field.get_default()
            # clear fields with server_default set as None
            if (
                field.server_default is not None
                and new_kwargs.get(field_name, None) is None
            ):
                new_kwargs.pop(field_name, None)
        return new_kwargs

    @classmethod
    def populate_onupdate_value(
        cls, new_kwargs: dict, explicit_fields: Optional[set[str]] = None
    ) -> dict:
        """
        Populates on_update values for fields that have on_update configured
        and were not provided explicitly.

        If ``explicit_fields`` is omitted the keys of ``new_kwargs`` are
        treated as explicitly provided. Callers pass the set explicitly when
        ``new_kwargs`` is a full ``model_dump`` (e.g. ``bulk_update``) so that
        fields the user actually mutated can be distinguished from values that
        are just present by virtue of the dump.

        :param new_kwargs: dictionary of values about to be used for the update
        :type new_kwargs: dict
        :param explicit_fields: fields that should be considered user-provided
        :type explicit_fields: Optional[set[str]]
        :return: new_kwargs with on_update values populated
        :rtype: dict
        """
        if explicit_fields is None:
            explicit_fields = set(new_kwargs.keys())
        for field_name in cls._onupdate_fields - explicit_fields:
            field = cls.ormar_config.model_fields[field_name]
            new_kwargs[field_name] = field.get_on_update()
        return new_kwargs

    @classmethod
    def validate_enums(cls, new_kwargs: dict) -> dict:
        """
        Receives dictionary of model that is about to be saved and validates the
        fields with choices set to see if the value is allowed.

        :param new_kwargs: dictionary of model that is about to be saved
        :type new_kwargs: dict
        :return: dictionary of model that is about to be saved
        :rtype: dict
        """
        validators = cls._build_individual_schema_validator()
        for key, value in new_kwargs.items():
            if key in validators:
                validators[key].validate_python(value)
        return new_kwargs

    @classmethod
    def _build_individual_schema_validator(cls) -> Any:
        if cls.__ormar_fields_validators__ is not None:
            return cls.__ormar_fields_validators__
        field_validators = {}
        for key, field in cls._extract_pydantic_fields().items():
            if cls.__pydantic_core_schema__["type"] == "definitions":
                schema = {
                    "type": "definitions",
                    "schema": field["schema"],
                    "definitions": cls.__pydantic_core_schema__["definitions"],
                }
            else:
                schema = field["schema"]
            field_validators[key] = create_schema_validator(
                schema, cls, cls.__module__, cls.__qualname__, "BaseModel"
            )
        cls.__ormar_fields_validators__ = field_validators
        return cls.__ormar_fields_validators__

    @classmethod
    def _extract_pydantic_fields(cls) -> Any:
        if cls.__pydantic_core_schema__["type"] == "model":
            return cls.__pydantic_core_schema__["schema"]["fields"]
        elif cls.__pydantic_core_schema__["type"] == "definitions":
            main_schema = cls.__pydantic_core_schema__["schema"]
            if "schema_ref" in main_schema:  # pragma: no cover
                reference_id = main_schema["schema_ref"]
                return next(
                    ref
                    for ref in cls.__pydantic_core_schema__["definitions"]
                    if ref["ref"] == reference_id
                )["schema"]["fields"]
            return main_schema["schema"]["fields"]

    @staticmethod
    async def _upsert_model(
        instance: "Model",
        save_all: bool,
        previous_model: Optional["Model"],
        relation_field: Optional["ForeignKeyField"],
        update_count: int,
    ) -> int:
        """
        Method updates given instance if:

        * instance is not saved or
        * instance have no pk or
        * save_all=True flag is set

        and instance is not __pk_only__.

        If relation leading to instance is a ManyToMany also the through model is saved

        :param instance: current model to upsert
        :type instance: Model
        :param save_all: flag if all models should be saved or only not saved ones
        :type save_all: bool
        :param relation_field: field with relation
        :type relation_field: Optional[ForeignKeyField]
        :param previous_model: previous model from which method came
        :type previous_model: Model
        :param update_count: no of updated models
        :type update_count: int
        :return: no of updated models
        :rtype: int
        """
        if (
            save_all or not instance.pk or not instance.saved
        ) and not instance.__pk_only__:
            await instance.upsert(__force_save__=True)
            if relation_field and relation_field.is_multi:
                await instance._upsert_through_model(
                    instance=instance,
                    relation_field=relation_field,
                    previous_model=cast("Model", previous_model),
                )
            update_count += 1
        return update_count

    @staticmethod
    async def _upsert_through_model(
        instance: "Model", previous_model: "Model", relation_field: "ForeignKeyField"
    ) -> None:
        """
        Upsert through model for m2m relation.

        :param instance: current model to upsert
        :type instance: Model
        :param relation_field: field with relation
        :type relation_field: Optional[ForeignKeyField]
        :param previous_model: previous model from which method came
        :type previous_model: Model
        """
        through_name = previous_model.ormar_config.model_fields[
            relation_field.name
        ].through.get_name()
        through = getattr(instance, through_name)
        if through:
            through_dict = through.model_dump(exclude=through.extract_related_names())
        else:
            through_dict = {}
        await getattr(
            previous_model, relation_field.name
        ).queryset_proxy.upsert_through_instance(instance, **through_dict)

    async def _update_relation_list(
        self,
        fields_list: Collection["ForeignKeyField"],
        follow: bool,
        save_all: bool,
        relation_map: dict,
        update_count: int,
    ) -> int:
        """
        Internal method used in save_related to follow deeper from
        related models and update numbers of updated related instances.

        :type save_all: flag if all models should be saved
        :type save_all: bool
        :param fields_list: list of ormar fields to follow and save
        :type fields_list: Collection["ForeignKeyField"]
        :param relation_map: map of relations to follow
        :type relation_map: dict
        :param follow: flag to trigger deep save -
        by default only directly related models are saved
        with follow=True also related models of related models are saved
        :type follow: bool
        :param update_count: internal parameter for recursive calls -
        number of updated instances
        :type update_count: int
        :return: tuple of update count and visited
        :rtype: int
        """
        for field in fields_list:
            values = self._get_field_values(name=field.name)
            for value in values:
                if follow:
                    update_count = await value.save_related(
                        follow=follow,
                        save_all=save_all,
                        relation_map=skip_ellipsis(
                            relation_map, field.name, default={}
                        ),
                        update_count=update_count,
                        previous_model=self,
                        relation_field=field,
                    )
                else:
                    update_count = await value._upsert_model(
                        instance=value,
                        save_all=save_all,
                        previous_model=self,
                        relation_field=field,
                        update_count=update_count,
                    )
        return update_count

    def _get_field_values(self, name: str) -> list:
        """
        Extract field values and ensures it is a list.

        :param name: name of the field
        :type name: str
        :return: list of values
        :rtype: list
        """
        values = getattr(self, name) or []
        if not isinstance(values, list):
            values = [values]
        return values
