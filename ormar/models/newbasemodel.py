import base64
import builtins
import sys
import warnings
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Literal,
    Mapping,
    MutableSequence,
    Optional,
    TypeVar,
    Union,
    cast,
)

import pydantic
import sqlalchemy
import typing_extensions

import ormar  # noqa I100
from ormar.exceptions import ModelError, ModelPersistenceError, QueryDefinitionError
from ormar.fields.foreign_key import ForeignKeyField
from ormar.fields.parsers import decode_bytes, encode_json
from ormar.models.excludable import (
    ExcludableItems,
    FlattenMap,
    convert_all,
    filter_not_excluded_fields,
    normalize_to_dict,
    skip_ellipsis,
)
from ormar.models.helpers import register_relation_in_alias_manager
from ormar.models.helpers.relations import expand_reverse_relationship
from ormar.models.helpers.sqlalchemy import (
    populate_config_sqlalchemy_table_if_required,
    update_column_definition,
)
from ormar.models.metaclass import ModelMetaclass
from ormar.models.modelproxy import ModelTableProxy
from ormar.models.utils import Extra
from ormar.queryset.field_accessor import FieldAccessor
from ormar.queryset.utils import extract_access_chains, translate_list_to_dict
from ormar.relations.alias_manager import AliasManager
from ormar.relations.relation import Relation
from ormar.relations.relation_manager import RelationsManager
from ormar.warnings import OrmarDeprecatedSince020

if TYPE_CHECKING:  # pragma no cover
    from ormar.models import Model, OrmarConfig
    from ormar.signals import SignalEmitter

    T = TypeVar("T", bound="NewBaseModel")

    IntStr = Union[int, str]
    DictStrAny = dict[str, Any]
    SetStr = set[str]
    AbstractSetIntStr = AbstractSet[IntStr]
    MappingIntStrAny = Mapping[IntStr, Any]


@dataclass(frozen=True)
class NestedDescent:
    """
    Per-field locals threaded into the recursive ``model_dump`` call when
    descending into a related field. Returned from ``_resolve_field_descent``
    only when the field is not handled inline (flattened or skipped).
    """

    flatten_map: Optional["FlattenMap"]
    relation_map: builtins.dict
    include: Union[set, builtins.dict, None]
    exclude: Union[set, builtins.dict, None]


class NewBaseModel(pydantic.BaseModel, ModelTableProxy, metaclass=ModelMetaclass):
    """
    Main base class of ormar Model.
    Inherits from pydantic BaseModel and has all mixins combined in ModelTableProxy.
    Constructed with ModelMetaclass which in turn also inherits pydantic metaclass.

    Abstracts away all internals and helper functions, so final Model class has only
    the logic concerned with database connection and data persistence.
    """

    __slots__ = (
        "_orm_id",
        "_orm_saved",
        "_orm",
        "_pk_column",
        "__pk_only__",
        "__ormar_excludable__",
        "__cached_hash__",
        "__pydantic_extra__",
        "__pydantic_fields_set__",
        "__setattr_fields__",
    )

    if TYPE_CHECKING:  # pragma no cover
        pk: Any
        __relation_map__: Optional[list[str]]
        __cached_hash__: Optional[int]
        __setattr_fields__: set[str]
        _orm_relationship_manager: AliasManager
        _orm: RelationsManager
        _orm_id: int
        _orm_saved: bool
        _related_names: Optional[set]
        _through_names: Optional[set]
        _related_names_hash: str
        _quick_access_fields: set
        _json_fields: set
        _bytes_fields: set
        _onupdate_fields: set
        ormar_config: OrmarConfig

    # noinspection PyMissingConstructor
    def __init__(self, *args: Any, **kwargs: Any) -> None:  # type: ignore
        """
        Initializer that creates a new ormar Model that is also pydantic Model at the
        same time.

        Passed keyword arguments can be only field names and their corresponding values
        as those will be passed to pydantic validation that will complain if extra
        params are passed.

        If relations are defined each relation is expanded and children models are also
        initialized and validated. Relation from both sides is registered so you can
        access related models from both sides.

        Json fields are automatically loaded/dumped if needed.

        Models marked as abstract=True in internal OrmarConfig cannot be initialized.

        :raises ModelError: if abstract model is initialized, model has ForwardRefs
         that has not been updated or unknown field is passed
        :param args: ignored args
        :type args: Any
        :param kwargs: keyword arguments - all fields values and some special params
        :type kwargs: Any
        """
        self._verify_model_can_be_initialized()
        self._initialize_internal_attributes()

        object.__setattr__(self, "__pk_only__", False)
        object.__setattr__(self, "__ormar_excludable__", None)

        new_kwargs, through_tmp_dict = self._process_kwargs(kwargs)

        new_kwargs = self.serialize_nested_models_json_fields(new_kwargs)
        self.__pydantic_validator__.validate_python(
            new_kwargs,
            self_instance=self,  # type: ignore
        )
        self._register_related_models(new_kwargs, through_tmp_dict)

    @classmethod
    def _internal_construct(
        cls,
        _pk_only: bool = False,
        _excluded: Optional[set[str]] = None,
        **kwargs: Any,
    ) -> "NewBaseModel":
        """
        Internal-only factory for constructing model instances with pk_only or
        excluded support. Not reachable from user-supplied kwargs or JSON
        deserialization.

        :param _pk_only: if True, skip validation and set only pk field
        :type _pk_only: bool
        :param _excluded: set of field names to explicitly set to None
        :type _excluded: Optional[set[str]]
        :param kwargs: field values for the model
        :type kwargs: Any
        :return: constructed model instance
        :rtype: NewBaseModel
        """
        instance = cls.__new__(cls)
        instance._verify_model_can_be_initialized()
        instance._initialize_internal_attributes()
        object.__setattr__(instance, "__pk_only__", _pk_only)
        object.__setattr__(instance, "__ormar_excludable__", None)

        new_kwargs, through_tmp_dict = instance._process_kwargs(kwargs)

        if _excluded:
            for field_to_nullify in _excluded:
                new_kwargs[field_to_nullify] = None

        if not _pk_only:
            new_kwargs = instance.serialize_nested_models_json_fields(new_kwargs)
            instance.__pydantic_validator__.validate_python(
                new_kwargs,
                self_instance=instance,  # type: ignore
            )
        else:
            fields_set = {instance.ormar_config.pkname}
            object.__setattr__(instance, "__dict__", new_kwargs)
            object.__setattr__(instance, "__pydantic_fields_set__", fields_set)

        instance._register_related_models(new_kwargs, through_tmp_dict)
        return instance

    def _register_related_models(
        self, new_kwargs: dict[str, Any], through_tmp_dict: dict[str, Any]
    ) -> None:
        """
        Adds back through fields and registers related models after initialization.

        :param new_kwargs: processed keyword arguments with field values
        :type new_kwargs: dict[str, Any]
        :param through_tmp_dict: through model fields extracted during processing
        :type through_tmp_dict: dict[str, Any]
        """
        new_kwargs.update(through_tmp_dict)
        model_fields = object.__getattribute__(self, "ormar_config").model_fields
        for related in self.extract_related_names().union(self.extract_through_names()):
            model_fields[related].expand_relationship(
                new_kwargs.get(related), self, to_register=True
            )

    @classmethod
    def _construct_with_excluded(
        cls, excluded: set[str], **kwargs: Any
    ) -> typing_extensions.Self:
        """
        Constructs model instance and nullifies excluded fields post-construction.
        Used when loading partial results from the database.

        :param excluded: set of field names to nullify after construction
        :type excluded: set[str]
        :param kwargs: field values for the model
        :type kwargs: Any
        :return: constructed model instance
        :rtype: Self
        """
        instance = cls(**kwargs)
        for field_to_nullify in excluded:
            instance.__dict__[field_to_nullify] = None
        return instance

    def __setattr__(self, name: str, value: Any) -> None:  # noqa CCR001
        """
        Overwrites setattr in pydantic parent as otherwise descriptors are not called.

        :param name: name of the attribute to set
        :type name: str
        :param value: value of the attribute to set
        :type value: Any
        :return: None
        :rtype: None
        """
        prev_hash = hash(self)

        if hasattr(self, name):
            object.__setattr__(self, name, value)
        else:
            # let pydantic handle errors for unknown fields
            super().__setattr__(name, value)

        if self._onupdate_fields and name in self.ormar_config.model_fields:
            self.__setattr_fields__.add(name)

        # In this case, the hash could have changed, so update it
        if name == self.ormar_config.pkname or self.pk is None:
            object.__setattr__(self, "__cached_hash__", None)
            new_hash = hash(self)

            if prev_hash != new_hash:
                self._update_relation_cache(prev_hash, new_hash)

    def __getattr__(self, item: str) -> Any:
        """
        Used for private attributes of pydantic v2.

        :param item: name of attribute
        :type item: str
        :return: Any
        :rtype: Any
        """
        # TODO: Check __pydantic_extra__
        if item == "__pydantic_extra__":
            return None
        return super().__getattr__(item)  # type: ignore

    def __getstate__(self) -> dict[Any, Any]:
        state = super().__getstate__()
        self_dict = self.model_dump()
        state["__dict__"].update(**self_dict)
        return state

    def __setstate__(self, state: dict[Any, Any]) -> None:
        relations = {
            k: v
            for k, v in state["__dict__"].items()
            if k in self.extract_related_names()
        }
        basic_state = {
            k: v
            for k, v in state["__dict__"].items()
            if k not in self.extract_related_names()
        }
        state["__dict__"] = basic_state
        super().__setstate__(state)
        self._initialize_internal_attributes()
        for name, value in relations.items():
            setattr(self, name, value)

    def _update_relation_cache(self, prev_hash: int, new_hash: int) -> None:
        """
        Update all relation proxy caches with different hash if we have changed

        :param prev_hash: The previous hash to update
        :type prev_hash: int
        :param new_hash: The hash to update to
        :type new_hash: int
        """

        def _update_cache(relations: list[Relation], recurse: bool = True) -> None:
            for relation in relations:
                relation_proxy = relation.get()

                if hasattr(relation_proxy, "update_cache"):
                    relation_proxy.update_cache(prev_hash, new_hash)  # type: ignore
                elif recurse and hasattr(relation_proxy, "_orm"):
                    _update_cache(
                        relation_proxy._orm._relations.values(),  # type: ignore
                        recurse=False,
                    )

        _update_cache(list(self._orm._relations.values()))

    def _internal_set(self, name: str, value: Any) -> None:
        """
        Delegates call to pydantic.

        :param name: name of param
        :type name: str
        :param value: value to set
        :type value: Any
        """
        super().__setattr__(name, value)

    def _verify_model_can_be_initialized(self) -> None:
        """
        Raises exception if model is abstract or has ForwardRefs in relation fields.

        :return: None
        :rtype: None
        """
        if self.ormar_config.abstract:
            raise ModelError(f"You cannot initialize abstract model {self.get_name()}")
        if self.ormar_config.requires_ref_update:
            raise ModelError(
                f"Model {self.get_name()} has not updated "
                f"ForwardRefs. \nBefore using the model you "
                f"need to call update_forward_refs()."
            )

    def _process_kwargs(self, kwargs: dict) -> tuple[dict, dict]:  # noqa: CCR001
        """
        Initializes nested models.

        Removes property_fields

        Checks if field is in the model fields or pydantic fields.

        Extracts through models from kwargs into temporary dict.

        :param kwargs: passed to init keyword arguments
        :type kwargs: dict
        :return: modified kwargs
        :rtype: tuple[dict, dict]
        """
        property_fields = self.ormar_config.property_fields
        model_fields = self.ormar_config.model_fields
        pydantic_fields = set(self.__class__.model_fields.keys())

        # remove property fields
        for prop_filed in property_fields:
            kwargs.pop(prop_filed, None)

        if "pk" in kwargs:
            kwargs[self.ormar_config.pkname] = kwargs.pop("pk")

        # extract through fields
        through_tmp_dict = dict()
        for field_name in self.extract_through_names():
            through_tmp_dict[field_name] = kwargs.pop(field_name, None)

        kwargs = self._remove_extra_parameters_if_they_should_be_ignored(
            kwargs=kwargs, model_fields=model_fields, pydantic_fields=pydantic_fields
        )
        try:
            new_kwargs: dict[str, Any] = {
                k: self._convert_to_bytes(
                    k,
                    self._convert_json(
                        k,
                        (
                            model_fields[k].expand_relationship(
                                v, self, to_register=False
                            )
                            if k in model_fields
                            else (v if k in pydantic_fields else model_fields[k])
                        ),
                    ),
                )
                for k, v in kwargs.items()
            }
        except KeyError as e:
            raise ModelError(
                f"Unknown field '{e.args[0]}' for model {self.get_name(lower=False)}"
            )

        return new_kwargs, through_tmp_dict

    def _remove_extra_parameters_if_they_should_be_ignored(
        self, kwargs: dict, model_fields: dict, pydantic_fields: set
    ) -> dict:
        """
        Removes the extra fields from kwargs if they should be ignored.

        :param kwargs: passed arguments
        :type kwargs: dict
        :param model_fields: dictionary of model fields
        :type model_fields: dict
        :param pydantic_fields: set of pydantic fields names
        :type pydantic_fields: set
        :return: dict without extra fields
        :rtype: dict
        """
        if self.ormar_config.extra == Extra.ignore:
            kwargs = {
                k: v
                for k, v in kwargs.items()
                if k in model_fields or k in pydantic_fields
            }
        return kwargs

    def _initialize_internal_attributes(self) -> None:
        """
        Initializes internal attributes during __init__()
        :rtype: None
        """
        # object.__setattr__(self, "_orm_id", uuid.uuid4().hex)
        object.__setattr__(self, "_orm_saved", False)
        object.__setattr__(self, "_pk_column", None)
        object.__setattr__(self, "__setattr_fields__", set())
        object.__setattr__(
            self,
            "_orm",
            RelationsManager(
                related_fields=self.extract_related_fields(), owner=cast("Model", self)
            ),
        )

    def __eq__(self, other: object) -> bool:
        """
        Compares other model to this model. when == is called.
        :param other: other model to compare
        :type other: object
        :return: result of comparison
        :rtype: bool
        """
        if isinstance(other, NewBaseModel):
            return self.__same__(other)
        return super().__eq__(other)  # pragma no cover

    def __hash__(self) -> int:
        if getattr(self, "__cached_hash__", None) is not None:
            return self.__cached_hash__ or 0

        if self.pk is not None:
            ret = hash(str(self.pk) + self.__class__.__name__)
        else:
            vals = {
                k: v
                for k, v in self.__dict__.items()
                if k not in self.extract_related_names()
            }
            ret = hash(str(vals) + self.__class__.__name__)

        object.__setattr__(self, "__cached_hash__", ret)
        return ret

    def __same__(self, other: "NewBaseModel") -> bool:
        """
        Used by __eq__, compares other model to this model.
        Compares:
        * _orm_ids,
        * primary key values if it's set
        * dictionary of own fields (excluding relations)
        :param other: model to compare to
        :type other: NewBaseModel
        :return: result of comparison
        :rtype: bool
        """
        if (self.pk is None and other.pk is not None) or (
            self.pk is not None and other.pk is None
        ):
            return False
        else:
            return hash(self) == other.__hash__()

    @classmethod
    def get_name(cls, lower: bool = True) -> str:
        """
        Returns name of the Model class, by default lowercase.

        :param lower: flag if name should be set to lowercase
        :type lower: bool
        :return: name of the model
        :rtype: str
        """
        name = cls.__name__
        if lower:
            name = name.lower()
        return name

    @property
    def pk_column(self) -> sqlalchemy.Column:
        """
        Retrieves primary key sqlalchemy column from models OrmarConfig.table.
        Each model has to have primary key.
        Only one primary key column is allowed.

        :return: primary key sqlalchemy column
        :rtype: sqlalchemy.Column
        """
        if object.__getattribute__(self, "_pk_column") is not None:
            return object.__getattribute__(self, "_pk_column")
        pk_columns = self.ormar_config.table.primary_key.columns.values()
        pk_col = pk_columns[0]
        object.__setattr__(self, "_pk_column", pk_col)
        return pk_col

    @property
    def saved(self) -> bool:
        """Saved status of the model. Changed by setattr and loading from db"""
        return self._orm_saved

    @property
    def signals(self) -> "SignalEmitter":
        """Exposes signals from model OrmarConfig"""
        return self.ormar_config.signals

    @classmethod
    def pk_type(cls) -> Any:
        """Shortcut to models primary key field type"""
        return cls.ormar_config.model_fields[cls.ormar_config.pkname].__type__

    @classmethod
    def db_backend_name(cls) -> str:
        """Shortcut to database dialect,
        cause some dialect require different treatment"""
        return cls.ormar_config.database.dialect.name

    def remove(self, parent: "Model", name: str) -> None:
        """Removes child from relation with given name in RelationshipManager"""
        self._orm.remove_parent(self, parent, name)

    def set_save_status(self, status: bool) -> None:
        """Sets value of the save status"""
        object.__setattr__(self, "_orm_saved", status)

    @classmethod
    def update_forward_refs(cls, **localns: Any) -> None:
        """
        Processes fields that are ForwardRef and need to be evaluated into actual
        models.

        Expands relationships, register relation in alias manager and substitutes
        sqlalchemy columns with new ones with proper column type (null before).

        Populates OrmarConfig table of the Model which is left empty before.

        Sets self_reference flag on models that links to themselves.

        Calls the pydantic method to evaluate pydantic fields.

        :param localns: local namespace
        :type localns: Any
        :return: None
        :rtype: None
        """
        globalns = sys.modules[cls.__module__].__dict__.copy()
        globalns.setdefault(cls.__name__, cls)
        fields_to_check = cls.ormar_config.model_fields.copy()
        for field in fields_to_check.values():
            if field.has_unresolved_forward_refs():
                field = cast(ForeignKeyField, field)
                field.evaluate_forward_ref(globalns=globalns, localns=localns)
                field.set_self_reference_flag()
                if field.is_multi and not field.through:
                    field = cast(ormar.ManyToManyField, field)
                    field.create_default_through_model()
                expand_reverse_relationship(model_field=field)
                register_relation_in_alias_manager(field=field)
                update_column_definition(model=cls, field=field)
        populate_config_sqlalchemy_table_if_required(config=cls.ormar_config)
        # super().update_forward_refs(**localns)
        cls.model_rebuild(
            force=True,
            _types_namespace={
                field.to.__name__: field.to
                for field in fields_to_check.values()
                if field.is_relation
            },
        )
        cls.ormar_config.requires_ref_update = False

    @staticmethod
    def _extract_nested_models_from_list(  # noqa: CFQ002
        relation_map: builtins.dict,
        models: MutableSequence,
        include: Union[set, builtins.dict, None],
        exclude: Union[set, builtins.dict, None],
        exclude_primary_keys: bool,
        exclude_through_models: bool,
        flatten_map: Optional[FlattenMap] = None,
    ) -> list:
        """
        Converts list of models into list of dictionaries.

        :param models: list of models
        :type models: list
        :param include: fields to include
        :type include: Union[set, dict, None]
        :param exclude: fields to exclude
        :type exclude: Union[set, dict, None]
        :param flatten_map: FlattenMap of relations to render as pk values
        :type flatten_map: Optional[FlattenMap]
        :return: list of models converted to dictionaries
        :rtype: list[dict]
        """
        result = []
        for model in models:
            try:
                model_dict = model.model_dump(
                    relation_map=relation_map,
                    include=include,
                    exclude=exclude,
                    exclude_primary_keys=exclude_primary_keys,
                    exclude_through_models=exclude_through_models,
                    flatten_fields=flatten_map,
                )
                if not exclude_through_models:
                    model.populate_through_models(
                        model=model,
                        model_dict=model_dict,
                        include=include,
                        exclude=exclude,
                        relation_map=relation_map,
                    )
                result.append(model_dict)
            except ReferenceError:  # pragma no cover
                continue
        return result

    @staticmethod
    def populate_through_models(
        model: "Model",
        model_dict: builtins.dict,
        include: Union[set, builtins.dict],
        exclude: Union[set, builtins.dict],
        relation_map: builtins.dict,
    ) -> None:
        """
        Populates through models with values from dict representation.

        :param model: model to populate through models
        :type model: Model
        :param model_dict: dict representation of the model
        :type model_dict: dict
        :param include: fields to include
        :type include: dict
        :param exclude: fields to exclude
        :type exclude: dict
        :param relation_map: map of relations to follow to avoid circular refs
        :type relation_map: dict
        :return: None
        :rtype: None
        """

        models_to_populate = filter_not_excluded_fields(
            fields=model.extract_through_names(),
            include=normalize_to_dict(include),
            exclude=normalize_to_dict(exclude),
        )
        through_fields_to_populate = [
            model.ormar_config.model_fields[through_model]
            for through_model in models_to_populate
            if model.ormar_config.model_fields[through_model].related_name
            not in relation_map
        ]
        for through_field in through_fields_to_populate:
            through_instance = getattr(model, through_field.name)
            if through_instance:
                model_dict[through_field.name] = through_instance.model_dump()

    @staticmethod
    def _resolve_field_descent(  # noqa: CFQ002
        field: str,
        nested_model: Any,
        flatten_map: Optional[FlattenMap],
        exclude_list: bool,
        relation_map: builtins.dict,
        include: Optional[builtins.dict],
        exclude: Optional[builtins.dict],
        dict_instance: builtins.dict,
    ) -> Optional[NestedDescent]:
        """
        Decide how to dump a related field. When the field is flattened, write
        its primary-key representation directly into ``dict_instance`` and
        return ``None`` so the caller skips further descent. When the field is
        an excluded list, also return ``None``. Otherwise return a
        ``NestedDescent`` carrying the per-field locals the caller threads
        into the recursive dump.

        :param field: relation field name being processed
        :type field: str
        :param nested_model: value of the relation on the current instance
        :type nested_model: Any
        :param flatten_map: current flatten directive scope, or ``None``
        :type flatten_map: Optional[FlattenMap]
        :param exclude_list: whether to skip list-valued relations entirely
        :type exclude_list: bool
        :param relation_map: relation traversal map for the current scope
        :type relation_map: dict
        :param include: include selector for the current scope
        :type include: Optional[dict]
        :param exclude: exclude selector for the current scope
        :type exclude: Optional[dict]
        :param dict_instance: dict being built for the current model
        :type dict_instance: dict
        :return: per-field locals, or ``None`` if the field was handled inline
            (flattened or skipped)
        :rtype: Optional[NestedDescent]
        """
        is_sequence = isinstance(nested_model, MutableSequence)
        if is_sequence and exclude_list:
            return None
        if flatten_map is not None and flatten_map.is_field_flattened(field):
            if is_sequence:
                dict_instance[field] = [m.pk for m in nested_model]
            else:
                dict_instance[field] = (
                    nested_model.pk if nested_model is not None else None
                )
            return None
        return NestedDescent(
            flatten_map=flatten_map.descend(field) if flatten_map is not None else None,
            relation_map=cast(
                builtins.dict, skip_ellipsis(relation_map, field, default=dict())
            ),
            include=convert_all(skip_ellipsis(include, field)),
            exclude=convert_all(skip_ellipsis(exclude, field)),
        )

    def _extract_nested_models(  # noqa: CCR001, CFQ002
        self,
        relation_map: builtins.dict,
        dict_instance: builtins.dict,
        include: Optional[builtins.dict],
        exclude: Optional[builtins.dict],
        exclude_primary_keys: bool,
        exclude_through_models: bool,
        exclude_list: bool,
        flatten_map: Optional[FlattenMap] = None,
    ) -> builtins.dict:
        """
        Traverse nested models and converts them into dictionaries.
        Calls itself recursively if needed.

        :param relation_map: map of the relations to follow to avoid circular deps
        :type relation_map: dict
        :param dict_instance: current instance dict
        :type dict_instance: dict
        :param include: fields to include
        :type include: Optional[dict]
        :param exclude: fields to exclude
        :type exclude: Optional[dict]
        :param exclude_list: whether to exclude list-valued relations
        :type exclude_list: bool
        :param flatten_map: FlattenMap directing relations to render as pk values
        :type flatten_map: Optional[FlattenMap]
        :return: current model dict with child models converted to dictionaries
        :rtype: dict
        """
        fields = filter_not_excluded_fields(
            fields=self.extract_related_names(), include=include, exclude=exclude
        )

        for field in fields:
            if not relation_map or field not in relation_map:
                continue
            try:
                nested_model = getattr(self, field)
                descent = self._resolve_field_descent(
                    field=field,
                    nested_model=nested_model,
                    flatten_map=flatten_map,
                    exclude_list=exclude_list,
                    relation_map=relation_map,
                    include=include,
                    exclude=exclude,
                    dict_instance=dict_instance,
                )
                if descent is None:
                    continue
                if isinstance(nested_model, MutableSequence):
                    dict_instance[field] = self._extract_nested_models_from_list(
                        relation_map=descent.relation_map,
                        models=nested_model,
                        include=descent.include,
                        exclude=descent.exclude,
                        exclude_primary_keys=exclude_primary_keys,
                        exclude_through_models=exclude_through_models,
                        flatten_map=descent.flatten_map,
                    )
                elif nested_model is not None:
                    model_dict = nested_model.model_dump(
                        relation_map=descent.relation_map,
                        include=descent.include,
                        exclude=descent.exclude,
                        exclude_primary_keys=exclude_primary_keys,
                        exclude_through_models=exclude_through_models,
                        flatten_fields=descent.flatten_map,
                    )
                    if not exclude_through_models:
                        nested_model.populate_through_models(
                            model=nested_model,
                            model_dict=model_dict,
                            include=descent.include,
                            exclude=descent.exclude,
                            relation_map=descent.relation_map,
                        )
                    dict_instance[field] = model_dict
                else:
                    dict_instance[field] = None
            except ReferenceError:  # pragma: no cover
                dict_instance[field] = None
        return dict_instance

    @typing_extensions.deprecated(
        "The `dict` method is deprecated; use `model_dump` instead.",
        category=OrmarDeprecatedSince020,
    )
    def dict(  # type: ignore # noqa A003
        self,
        *,
        include: Union[set, builtins.dict, None] = None,
        exclude: Union[set, builtins.dict, None] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        exclude_primary_keys: bool = False,
        exclude_through_models: bool = False,
        exclude_list: bool = False,
        relation_map: Optional[builtins.dict] = None,
    ) -> "DictStrAny":  # noqa: A003 # pragma: no cover
        warnings.warn(
            "The `dict` method is deprecated; use `model_dump` instead.",
            DeprecationWarning,
        )
        return self.model_dump(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            exclude_primary_keys=exclude_primary_keys,
            exclude_through_models=exclude_through_models,
            exclude_list=exclude_list,
            relation_map=relation_map,
        )

    def model_dump(  # type: ignore # noqa A003, CFQ002
        self,
        *,
        mode: Union[Literal["json", "python"], str] = "python",
        include: Union[set, builtins.dict, None] = None,
        exclude: Union[set, builtins.dict, None] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        exclude_primary_keys: bool = False,
        exclude_through_models: bool = False,
        exclude_list: bool = False,
        relation_map: Optional[builtins.dict] = None,
        flatten_fields: Union[
            set, list, str, tuple, builtins.dict, "FieldAccessor", FlattenMap, None
        ] = None,
        flatten_all: bool = False,
        round_trip: bool = False,
        warnings: bool = True,
    ) -> "DictStrAny":  # noqa: A003
        """

        Generate a dictionary representation of the model,
        optionally specifying which fields to include or exclude.

        Nested models are also parsed to dictionaries.

        Additionally, fields decorated with @property_field are also added.

        :param exclude_through_models: flag to exclude through models from dict
        :type exclude_through_models: bool
        :param exclude_primary_keys: flag to exclude primary keys from dict
        :type exclude_primary_keys: bool
        :param include: fields to include
        :type include: Union[set, dict, None]
        :param exclude: fields to exclude
        :type exclude: Union[set, dict, None]
        :param by_alias: flag to get values by alias - passed to pydantic
        :type by_alias: bool
        :param exclude_unset: flag to exclude not set values - passed to pydantic
        :type exclude_unset: bool
        :param exclude_defaults: flag to exclude default values - passed to pydantic
        :type exclude_defaults: bool
        :param exclude_none: flag to exclude None values - passed to pydantic
        :type exclude_none: bool
        :param exclude_list: flag to exclude lists of nested values models from dict
        :type exclude_list: bool
        :param relation_map: map of the relations to follow to avoid circular deps
        :type relation_map: dict
        :param flatten_fields: relations to render as their primary-key value.
            Accepts dunder-strings, list/set/tuple, nested dict (with Ellipsis),
            or ``FieldAccessor`` / list of accessors. Resolves to a nested-dict
            spec used during traversal.
        :type flatten_fields: Union[set, list, str, tuple, dict, FieldAccessor, None]
        :param flatten_all: if True every nested relation is collapsed to its pk
        :type flatten_all: bool
        :param mode: The mode in which `to_python` should run.
            If mode is 'json', the dictionary will only contain JSON serializable types.
            If mode is 'python', the dictionary may contain any Python objects.
        :type mode: str
        :param round_trip: flag to enable serialization round-trip support
        :type round_trip: bool
        :param warnings: flag to log warnings for invalid fields
        :type warnings: bool
        :return:
        :rtype:
        """
        flatten_map = self._resolve_flatten_map(
            flatten_fields=flatten_fields, flatten_all=flatten_all
        )
        if exclude_primary_keys and flatten_map:
            raise QueryDefinitionError(
                "flatten_fields / flatten_all cannot be combined with "
                "exclude_primary_keys=True: flattening renders primary keys, "
                "excluding removes them."
            )
        include_dict = normalize_to_dict(include)
        exclude_dict = normalize_to_dict(exclude)
        if flatten_map is not None:
            flatten_map.check_vs_selector(include_dict, "include")
            flatten_map.check_vs_selector(exclude_dict, "exclude")

        pydantic_exclude = self._update_excluded_with_related(exclude)
        pydantic_exclude = self._update_excluded_with_pks_and_through(
            exclude=pydantic_exclude,
            exclude_primary_keys=exclude_primary_keys,
            exclude_through_models=exclude_through_models,
        )
        dict_instance = super().model_dump(
            mode=mode,
            include=include,
            exclude=pydantic_exclude,
            by_alias=by_alias,
            exclude_defaults=exclude_defaults,
            exclude_unset=exclude_unset,
            exclude_none=exclude_none,
            round_trip=round_trip,
            warnings=False,
        )

        dict_instance = {
            k: self._convert_bytes_to_str(column_name=k, value=v)
            for k, v in dict_instance.items()
        }

        relation_map = (
            relation_map
            if relation_map is not None
            else translate_list_to_dict(self._iterate_related_models())
        )
        pk_only = getattr(self, "__pk_only__", False)
        if relation_map and not pk_only:
            dict_instance = self._extract_nested_models(
                relation_map=relation_map,
                dict_instance=dict_instance,
                include=include_dict,
                exclude=exclude_dict,
                exclude_primary_keys=exclude_primary_keys,
                exclude_through_models=exclude_through_models,
                exclude_list=exclude_list,
                flatten_map=flatten_map,
            )

        return dict_instance

    def _resolve_flatten_map(
        self,
        flatten_fields: Union[
            set, list, str, tuple, builtins.dict, "FieldAccessor", FlattenMap, None
        ],
        flatten_all: bool,
    ) -> Optional[FlattenMap]:
        """
        Produce a :class:`FlattenMap` from the various input forms accepted by
        ``model_dump``. Resolves dunder strings / lists / sets / nested dicts /
        ``FieldAccessor`` chains; an already-resolved ``FlattenMap`` (passed
        during recursion) is returned as-is.

        :param flatten_fields: user spec, nested dict, or FlattenMap from recursion
        :type flatten_fields: Union[set, list, str, tuple, dict, FieldAccessor,
            FlattenMap, None]
        :param flatten_all: if True every relation flattens regardless of spec
        :type flatten_all: bool
        :return: resolved FlattenMap, or None when nothing should flatten
        :rtype: Optional[FlattenMap]
        """
        if flatten_all:
            return FlattenMap(flatten_all=True)

        if flatten_fields is None:
            excludable = getattr(self, "__ormar_excludable__", None)
            return excludable.flatten_map() if excludable is not None else None

        if isinstance(flatten_fields, FlattenMap):
            return flatten_fields

        if isinstance(flatten_fields, builtins.dict):
            return FlattenMap(data=flatten_fields)

        excludable = ExcludableItems()
        excludable.build(
            items=extract_access_chains(flatten_fields),
            model_cls=cast(type["Model"], type(self)),
            slot="flatten",
        )
        return excludable.flatten_map()

    @typing_extensions.deprecated(
        "The `json` method is deprecated; use `model_dump_json` instead.",
        category=OrmarDeprecatedSince020,
    )
    def json(  # type: ignore # noqa A003
        self,
        *,
        include: Union[set, builtins.dict, None] = None,
        exclude: Union[set, builtins.dict, None] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        exclude_primary_keys: bool = False,
        exclude_through_models: bool = False,
        **dumps_kwargs: Any,
    ) -> str:  # pragma: no cover
        warnings.warn(
            "The `json` method is deprecated; use `model_dump_json` instead.",
            DeprecationWarning,
        )
        return self.model_dump_json(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            exclude_primary_keys=exclude_primary_keys,
            exclude_through_models=exclude_through_models,
            **dumps_kwargs,
        )

    def model_dump_json(  # type: ignore # noqa A003
        self,
        *,
        include: Union[set, builtins.dict, None] = None,
        exclude: Union[set, builtins.dict, None] = None,
        by_alias: bool = False,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        exclude_primary_keys: bool = False,
        exclude_through_models: bool = False,
        flatten_fields: Union[
            set, list, str, tuple, builtins.dict, "FieldAccessor", FlattenMap, None
        ] = None,
        flatten_all: bool = False,
        **dumps_kwargs: Any,
    ) -> str:
        """
        Generate a JSON representation of the model, `include` and `exclude`
        arguments as per `dict()`.

        `encoder` is an optional function to supply as `default` to json.dumps(),
        other arguments as per `json.dumps()`.
        """
        data = self.model_dump(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            exclude_primary_keys=exclude_primary_keys,
            exclude_through_models=exclude_through_models,
            flatten_fields=flatten_fields,
            flatten_all=flatten_all,
        )
        return self.__pydantic_serializer__.to_json(data, warnings=False).decode()

    @classmethod
    @typing_extensions.deprecated(
        "The `construct` method is deprecated; use `model_construct` instead.",
        category=OrmarDeprecatedSince020,
    )
    def construct(
        cls: type["T"], _fields_set: Union[set[str], None] = None, **values: Any
    ) -> "T":  # pragma: no cover
        warnings.warn(
            "The `construct` method is deprecated; use `model_construct` instead.",
            DeprecationWarning,
        )
        return cls.model_construct(_fields_set=_fields_set, **values)

    @classmethod
    def model_construct(
        cls: type["T"], _fields_set: Optional["SetStr"] = None, **values: Any
    ) -> "T":
        own_values = {
            k: v for k, v in values.items() if k not in cls.extract_related_names()
        }
        model = cls.__new__(cls)
        fields_values: dict[str, Any] = {}
        for name, field in cls.model_fields.items():
            if name in own_values:
                fields_values[name] = own_values[name]
            elif not field.is_required():
                fields_values[name] = field.get_default()
        fields_values.update(own_values)

        if _fields_set is None:
            _fields_set = set(values.keys())

        extra_allowed = cls.model_config.get("extra") == "allow"
        if not extra_allowed:
            fields_values.update(values)
        object.__setattr__(model, "__dict__", fields_values)
        model._initialize_internal_attributes()
        cls._construct_relations(model=model, values=values)
        object.__setattr__(model, "__pydantic_fields_set__", _fields_set)
        return cls._pydantic_model_construct_finalizer(
            model=model, extra_allowed=extra_allowed, values=values
        )

    @classmethod
    def _pydantic_model_construct_finalizer(
        cls: type["T"], model: "T", extra_allowed: bool, **values: Any
    ) -> "T":
        """
        Recreate pydantic model_construct logic here as we do not call super method.
        """
        _extra: Union[builtins.dict[str, Any], None] = None
        if extra_allowed:  # pragma: no cover
            _extra = {}
            for k, v in values.items():
                _extra[k] = v

        if not cls.__pydantic_root_model__:
            object.__setattr__(model, "__pydantic_extra__", _extra)

        if cls.__pydantic_post_init__:  # pragma: no cover
            model.model_post_init(None)
        elif not cls.__pydantic_root_model__:
            # Note: if there are any private attributes,
            # cls.__pydantic_post_init__ would exist
            # Since it doesn't, that means that `__pydantic_private__`
            # should be set to None
            object.__setattr__(model, "__pydantic_private__", None)

        return model

    @classmethod
    def _construct_relations(cls: type["T"], model: "T", values: builtins.dict) -> None:
        present_relations = [
            relation for relation in cls.extract_related_names() if relation in values
        ]
        for relation in present_relations:
            value_to_set = values[relation]
            if not isinstance(value_to_set, list):
                value_to_set = [value_to_set]
            relation_field = cls.ormar_config.model_fields[relation]
            relation_value = [
                relation_field.expand_relationship(x, model, to_register=False)
                for x in value_to_set
                if x is not None
            ]

            for child in relation_value:
                model._orm.add(
                    parent=cast("Model", child),
                    child=cast("Model", model),
                    field=cast("ForeignKeyField", relation_field),
                )

    def update_from_dict(self, value_dict: builtins.dict) -> "NewBaseModel":
        """
        Updates self with values of fields passed in the dictionary.

        :param value_dict: dictionary of fields names and values
        :type value_dict: dict
        :return: self
        :rtype: NewBaseModel
        """
        for key, value in value_dict.items():
            setattr(self, key, value)
        return self

    def _convert_to_bytes(
        self, column_name: str, value: Any
    ) -> Union[str, builtins.dict]:
        """
        Converts value to bytes from string

        :param column_name: name of the field
        :type column_name: str
        :param value: value fo the field
        :type value: Any
        :return: converted value if needed, else original value
        :rtype: Any
        """
        if column_name not in self._bytes_fields:
            return value
        field = self.ormar_config.model_fields[column_name]
        if value is not None:
            value = decode_bytes(
                value=value, represent_as_string=field.represent_as_base64_str
            )
        return value

    def _convert_bytes_to_str(
        self, column_name: str, value: Any
    ) -> Union[str, builtins.dict]:
        """
        Converts value to str from bytes for represent_as_base64_str columns.

        :param column_name: name of the field
        :type column_name: str
        :param value: value fo the field
        :type value: Any
        :return: converted value if needed, else original value
        :rtype: Any
        """
        if column_name not in self._bytes_fields:
            return value
        field = self.ormar_config.model_fields[column_name]
        if (
            value is not None
            and not isinstance(value, str)
            and field.represent_as_base64_str
        ):
            return base64.b64encode(value).decode()
        return value

    def _convert_json(
        self, column_name: str, value: Any
    ) -> Union[str, builtins.dict, None]:
        """
        Converts value to/from json if needed (for Json columns).

        :param column_name: name of the field
        :type column_name: str
        :param value: value fo the field
        :type value: Any
        :return: converted value if needed, else original value
        :rtype: Any
        """
        if column_name not in self._json_fields:
            return value
        return encode_json(value)

    def _extract_own_model_fields(self) -> builtins.dict:
        """
        Returns a dictionary with field names and values for fields that are not
        relations fields (ForeignKey, ManyToMany etc.)

        :return: dictionary of fields names and values.
        :rtype: dict
        """
        related_names = self.extract_related_names()
        self_fields = {k: v for k, v in self.__dict__.items() if k not in related_names}
        return self_fields

    def _extract_model_db_fields(self) -> builtins.dict:
        """
        Returns a dictionary with field names and values for fields that are stored in
        current model's table.

        That includes own non-relational fields ang foreign key fields.

        :return: dictionary of fields names and values.
        :rtype: dict
        """
        self_fields = self._extract_own_model_fields()
        self_fields = {
            k: v
            for k, v in self_fields.items()
            if self.get_column_alias(k) in self.ormar_config.table.columns
        }
        for field in self._extract_db_related_names():
            relation_field = self.ormar_config.model_fields[field]
            target_pk_name = relation_field.to.ormar_config.pkname
            target_field = getattr(self, field)
            self_fields[field] = getattr(target_field, target_pk_name, None)
            if not relation_field.nullable and not self_fields[field]:
                raise ModelPersistenceError(
                    f"You cannot save {relation_field.to.get_name()} "
                    f"model without pk set!"
                )
        return self_fields
