import string
import sys
import uuid
from dataclasses import dataclass
from random import choices
from typing import TYPE_CHECKING, Any, ForwardRef, Optional, Union, cast, overload

import sqlalchemy
from pydantic import BaseModel, create_model

import ormar  # noqa I101
from ormar.exceptions import ModelDefinitionError, RelationshipInstanceError
from ormar.fields.base import BaseField
from ormar.fields.referential_actions import ReferentialAction

if TYPE_CHECKING:  # pragma no cover
    from ormar.fields import ManyToManyField
    from ormar.models import Model, NewBaseModel, T


def create_dummy_instance(fk: type["T"], pk: Any = None) -> "T":
    """
    Ormar never returns you a raw data.
    So if you have a related field that has a value populated
    it will construct you a Model instance out of it.

    Creates a "fake" instance of passed Model from pk value.
    The instantiated Model has only pk value filled.
    To achieve this __pk_only__ flag has to be passed as it skips the validation.

    If the nested related Models are required they are set with -1 as pk value.

    :param fk: class of the related Model to which instance should be constructed
    :type fk: Model class
    :param pk: value of the primary_key column
    :type pk: Any
    :return: Model instance populated with only pk
    :rtype: Model
    """
    init_dict = {
        **{fk.ormar_config.pkname: pk or -1},
        **{
            k: create_dummy_instance(v.to)
            for k, v in fk.ormar_config.model_fields.items()
            if v.is_relation and not v.nullable and not v.virtual
        },
    }
    return cast("T", fk._internal_construct(_pk_only=True, _excluded=None, **init_dict))


def create_dummy_model(
    base_model: type["T"],
    pk_field: Union[BaseField, "ForeignKeyField", "ManyToManyField"],
) -> type["BaseModel"]:
    """
    Used to construct a dummy pydantic model for type hints and pydantic validation.
    Populates only pk field and set it to desired type.

    :param base_model: class of target dummy model
    :type base_model: Model class
    :param pk_field: ormar Field to be set on pydantic Model
    :type pk_field: Union[BaseField, "ForeignKeyField", "ManyToManyField"]
    :return: constructed dummy model
    :rtype: pydantic.BaseModel
    """
    alias = (
        "".join(choices(string.ascii_uppercase, k=6))  # + uuid.uuid4().hex[:4]
    ).lower()
    fields = {f"{pk_field.name}": (pk_field.__type__, None)}

    dummy_model = create_model(  # type: ignore
        f"PkOnly{base_model.get_name(lower=False)}{alias}",
        __module__=base_model.__module__,
        **fields,  # type: ignore
    )
    return dummy_model


def populate_fk_params_based_on_to_model(
    to: type["T"],
    nullable: bool,
    onupdate: Optional[str] = None,
    ondelete: Optional[str] = None,
) -> tuple[Any, list, Any, Any]:
    """
    Based on target to model to which relation leads to populates the type of the
    pydantic field to use, ForeignKey constraint and type of the target column field.

    :param to: target related ormar Model
    :type to: Model class
    :param nullable: marks field as optional/ required
    :type nullable: bool
    :param onupdate: parameter passed to sqlalchemy.ForeignKey.
    How to treat child rows on update of parent (the one where FK is defined) model.
    :type onupdate: str
    :param ondelete: parameter passed to sqlalchemy.ForeignKey.
    How to treat child rows on delete of parent (the one where FK is defined) model.
    :type ondelete: str
    :return: tuple with target pydantic type, list of fk constraints and target col type
    :rtype: tuple[Any, list, Any]
    """
    fk_string = (
        to.ormar_config.tablename + "." + to.get_column_alias(to.ormar_config.pkname)
    )
    to_field = to.ormar_config.model_fields[to.ormar_config.pkname]
    pk_only_model = create_dummy_model(to, to_field)
    __type__ = (
        Union[to_field.__type__, to, pk_only_model]
        if not nullable
        else Optional[Union[to_field.__type__, to, pk_only_model]]
    )
    constraints = [
        ForeignKeyConstraint(
            reference=fk_string, ondelete=ondelete, onupdate=onupdate, name=None
        )
    ]
    column_type = to_field.column_type
    return __type__, constraints, column_type, pk_only_model


def validate_not_allowed_fields(kwargs: dict) -> None:
    """
    Verifies if not allowed parameters are set on relation models.
    Usually they are omitted later anyway but this way it's explicitly
    notify the user that it's not allowed/ supported.

    :raises ModelDefinitionError: if any forbidden field is set
    :param kwargs: dict of kwargs to verify passed to relation field
    :type kwargs: dict
    """
    default = kwargs.pop("default", None)
    encrypt_secret = kwargs.pop("encrypt_secret", None)
    encrypt_backend = kwargs.pop("encrypt_backend", None)
    encrypt_custom_backend = kwargs.pop("encrypt_custom_backend", None)
    overwrite_pydantic_type = kwargs.pop("overwrite_pydantic_type", None)

    not_supported = [
        default,
        encrypt_secret,
        encrypt_backend,
        encrypt_custom_backend,
        overwrite_pydantic_type,
    ]
    if any(x is not None for x in not_supported):
        raise ModelDefinitionError(
            f"Argument {next((x for x in not_supported if x is not None))} "
            f"is not supported "
            "on relation fields!"
        )


def validate_referential_action(
    action: Optional[Union[ReferentialAction, str]],
) -> Optional[str]:
    """
    Validation `onupdate` and `ondelete` action cast to a string value

    :raises ModelDefinitionError: if action is a not valid name string value
    :param action: referential action attribute or name string
    :type action: Optional[Union[ReferentialAction, str]]
    :rtype: Optional[str]
    """

    if action is not None and not isinstance(action, ReferentialAction):
        try:
            action = ReferentialAction(action.upper())
        except (ValueError, AttributeError):
            raise ModelDefinitionError(f"{action} ReferentialAction not supported.")

    return action.value if action is not None else None


@dataclass
class ForeignKeyConstraint:
    """
    Internal container to store ForeignKey definitions used later
    to produce sqlalchemy.ForeignKeys
    """

    reference: Union[str, sqlalchemy.Column]
    name: Optional[str]
    ondelete: Optional[str]
    onupdate: Optional[str]


@overload
def ForeignKey(to: type["T"], **kwargs: Any) -> "T":  # pragma: no cover
    ...


@overload
def ForeignKey(to: ForwardRef, **kwargs: Any) -> "Model":  # pragma: no cover
    ...


def ForeignKey(  # type: ignore # noqa CFQ002
    to: Union[type["T"], "ForwardRef"],
    *,
    name: Optional[str] = None,
    unique: bool = False,
    nullable: bool = True,
    related_name: Optional[str] = None,
    virtual: bool = False,
    onupdate: Union[ReferentialAction, str, None] = None,
    ondelete: Union[ReferentialAction, str, None] = None,
    **kwargs: Any,
) -> "T":
    """
    Despite a name it's a function that returns constructed ForeignKeyField.
    This function is actually used in model declaration (as ormar.ForeignKey(ToModel)).

    Accepts number of relation setting parameters as well as all BaseField ones.

    :param to: target related ormar Model
    :type to: Model class
    :param name: name of the database field - later called alias
    :type name: str
    :param unique: parameter passed to sqlalchemy.ForeignKey, unique flag
    :type unique: bool
    :param nullable: marks field as optional/ required
    :type nullable: bool
    :param related_name: name of reversed FK relation populated for you on to model
    :type related_name: str
    :param virtual: marks if relation is virtual.
    It is for reversed FK and auto generated FK on through model in Many2Many relations.
    :type virtual: bool
    :param onupdate: parameter passed to sqlalchemy.ForeignKey.
    How to treat child rows on update of parent (the one where FK is defined) model.
    :type onupdate: Union[ReferentialAction, str]
    :param ondelete: parameter passed to sqlalchemy.ForeignKey.
    How to treat child rows on delete of parent (the one where FK is defined) model.
    :type ondelete: Union[ReferentialAction, str]
    :param kwargs: all other args to be populated by BaseField
    :type kwargs: Any
    :return: ormar ForeignKeyField with relation to selected model
    :rtype: ForeignKeyField
    """

    onupdate = validate_referential_action(action=onupdate)
    ondelete = validate_referential_action(action=ondelete)

    owner = kwargs.pop("owner", None)
    self_reference = kwargs.pop("self_reference", False)

    orders_by = kwargs.pop("orders_by", None)
    related_orders_by = kwargs.pop("related_orders_by", None)

    skip_reverse = kwargs.pop("skip_reverse", False)
    skip_field = kwargs.pop("skip_field", False)

    sql_nullable = kwargs.pop("sql_nullable", None)
    sql_nullable = nullable if sql_nullable is None else sql_nullable

    index = kwargs.pop("index", False)

    validate_not_allowed_fields(kwargs)
    pk_only_model = None
    if to.__class__ == ForwardRef:
        __type__ = to if not nullable else Optional[to]
        constraints: list = []
        column_type = None
    else:
        (
            __type__,
            constraints,
            column_type,
            pk_only_model,
        ) = populate_fk_params_based_on_to_model(
            to=to,  # type: ignore
            nullable=nullable,
            ondelete=ondelete,
            onupdate=onupdate,
        )

    namespace = dict(
        __type__=__type__,
        to=to,
        to_pk_only=pk_only_model,
        through=None,
        alias=name,
        name=kwargs.pop("real_name", None),
        nullable=nullable,
        sql_nullable=sql_nullable,
        constraints=constraints,
        unique=unique,
        column_type=column_type,
        related_name=related_name,
        virtual=virtual,
        primary_key=False,
        index=index,
        default=None,
        server_default=None,
        onupdate=onupdate,
        ondelete=ondelete,
        owner=owner,
        self_reference=self_reference,
        is_relation=True,
        orders_by=orders_by,
        related_orders_by=related_orders_by,
        skip_reverse=skip_reverse,
        skip_field=skip_field,
        **kwargs,
    )

    Field = type("ForeignKey", (ForeignKeyField, BaseField), {})
    return Field(**namespace)


class ForeignKeyField(BaseField):  # type: ignore[misc]
    """
    Actual class returned from ForeignKey function call and stored in model_fields.
    """

    def __init__(self, **kwargs: Any) -> None:
        if TYPE_CHECKING:  # pragma: no cover
            self.__type__: type
            self.to: type["Model"]
        self.ondelete: str = kwargs.pop("ondelete", None)
        self.onupdate: str = kwargs.pop("onupdate", None)
        super().__init__(**kwargs)

    def get_source_related_name(self) -> str:
        """
        Returns name to use for source relation name.
        For FK it's the same, differs for m2m fields.
        It's either set as `related_name` or by default it's owner model. get_name + 's'
        :return: name of the related_name or default related name.
        :rtype: str
        """
        return self.get_related_name()

    def get_related_name(self) -> str:
        """
        Returns name to use for reverse relation.
        It's either set as `related_name` or by default it's owner model. get_name + 's'
        :return: name of the related_name or default related name.
        :rtype: str
        """
        return self.related_name or self.owner.get_name() + "s"

    def default_target_field_name(self) -> str:
        """
        Returns default target model name on through model.
        :return: name of the field
        :rtype: str
        """
        prefix = "from_" if self.self_reference else ""
        return self.through_reverse_relation_name or f"{prefix}{self.to.get_name()}"

    def default_source_field_name(self) -> str:
        """
        Returns default target model name on through model.
        :return: name of the field
        :rtype: str
        """
        prefix = "to_" if self.self_reference else ""
        return self.through_relation_name or f"{prefix}{self.owner.get_name()}"

    def get_filter_clause_target(self) -> type["Model"]:
        return self.to

    def get_model_relation_fields(self, use_alias: bool = False) -> str:
        """
        Extract names of the database columns or model fields that are connected
        with given relation based on use_alias switch and which side of the relation
        the current field is - reverse or normal.

        :param use_alias: use db names aliases or model fields
        :type use_alias: bool
        :return: name or names of the related columns/ fields
        :rtype: Union[str, list[str]]
        """
        if use_alias:
            return self._get_model_relation_fields_alias()
        return self._get_model_relation_fields_name()

    def _get_model_relation_fields_name(self) -> str:
        if self.virtual:
            return self.owner.ormar_config.pkname
        return self.name

    def _get_model_relation_fields_alias(self) -> str:
        if self.virtual:
            return self.owner.ormar_config.model_fields[
                self.owner.ormar_config.pkname
            ].get_alias()
        return self.get_alias()

    def get_related_field_alias(self) -> str:
        """
        Extract names of the related database columns or that are connected
        with given relation based to use as a target in filter clause.

        :return: name or names of the related columns/ fields
        :rtype: Union[str, dict[str, str]]
        """
        if self.virtual:
            field_name = self.get_related_name()
            field = self.to.ormar_config.model_fields[field_name]
            return field.get_alias()
        target_field = self.to.get_column_alias(self.to.ormar_config.pkname)
        return target_field

    def get_related_field_name(self) -> Union[str, list[str]]:
        """
        Returns name of the relation field that should be used in prefetch query.
        This field is later used to register relation in prefetch query,
        populate relations dict, and populate nested model in prefetch query.

        :return: name(s) of the field
        :rtype: Union[str, list[str]]
        """
        if self.virtual:
            return self.get_related_name()
        return self.to.ormar_config.pkname

    def _evaluate_forward_ref(
        self, globalns: Any, localns: Any, is_through: bool = False
    ) -> None:
        target = "through" if is_through else "to"
        target_obj = getattr(self, target)
        if sys.version_info.minor <= 8:  # pragma: no cover
            evaluated = target_obj._evaluate(globalns, localns)
        else:  # pragma: no cover
            evaluated = target_obj._evaluate(globalns, localns, recursive_guard=set())
        setattr(self, target, evaluated)

    def evaluate_forward_ref(self, globalns: Any, localns: Any) -> None:
        """
        Evaluates the ForwardRef to actual Field based on global and local namespaces

        :param globalns: global namespace
        :type globalns: Any
        :param localns: local namespace
        :type localns: Any
        :return: None
        :rtype: None
        """
        if self.to.__class__ == ForwardRef:
            self._evaluate_forward_ref(globalns, localns)
            (
                self.__type__,
                self.constraints,
                self.column_type,
                self.to_pk_only,
            ) = populate_fk_params_based_on_to_model(
                to=self.to,
                nullable=self.nullable,
                ondelete=self.ondelete,
                onupdate=self.onupdate,
            )

    def _extract_model_from_sequence(
        self, value: list, child: "Model", to_register: bool
    ) -> list["Model"]:
        """
        Takes a list of Models and registers them on parent.
        Registration is mutual, so children have also reference to parent.

        Used in reverse FK relations.

        :param value: list of Model
        :type value: list
        :param child: child/ related Model
        :type child: Model
        :param to_register: flag if the relation should be set in RelationshipManager
        :type to_register: bool
        :return: list (if needed) registered Models
        :rtype: list["Model"]
        """
        return [
            self.expand_relationship(  # type: ignore
                value=val, child=child, to_register=to_register
            )
            for val in value
        ]

    def _register_existing_model(
        self, value: "Model", child: "Model", to_register: bool
    ) -> "Model":
        """
        Takes already created instance and registers it for parent.
        Registration is mutual, so children have also reference to parent.

        Used in reverse FK relations and normal FK for single models.

        :param value: already instantiated Model
        :type value: Model
        :param child: child/ related Model
        :type child: Model
        :param to_register: flag if the relation should be set in RelationshipManager
        :type to_register: bool
        :return: (if needed) registered Model
        :rtype: Model
        """
        if to_register:
            self.register_relation(model=value, child=child)
        return value

    def _construct_model_from_dict(
        self, value: dict, child: "Model", to_register: bool
    ) -> "Model":
        """
        Takes a dictionary, creates a instance and registers it for parent.
        If dictionary contains only one field and it's a pk it is a __pk_only__ model.
        Registration is mutual, so children have also reference to parent.

        Used in normal FK for dictionaries.

        :param value: dictionary of a Model
        :type value: dict
        :param child: child/ related Model
        :type child: Model
        :param to_register: flag if the relation should be set in RelationshipManager
        :type to_register: bool
        :return: (if needed) registered Model
        :rtype: Model
        """
        pk_only_model = None
        keys = set(value.keys())
        own_keys = keys - self.to.extract_related_names()
        if (
            len(own_keys) == 1
            and list(own_keys)[0] == self.to.ormar_config.pkname
            and value.get(self.to.ormar_config.pkname) is not None
            and not self.is_through
        ):
            pk_only_model = self.to_pk_only(**value)
            model = cast(
                "Model",
                self.to._internal_construct(_pk_only=True, _excluded=None, **value),
            )
        else:
            model = self.to(**value)
        if to_register:
            self.register_relation(model=model, child=child)
        return pk_only_model if pk_only_model is not None else model

    def _construct_model_from_pk(
        self, value: Any, child: "Model", to_register: bool
    ) -> "Model":
        """
        Takes a pk value, creates a dummy instance and registers it for parent.
        Registration is mutual, so children have also reference to parent.

        Used in normal FK for dictionaries.

        :param value: value of a related pk / fk column
        :type value: Any
        :param child: child/ related Model
        :type child: Model
        :param to_register: flag if the relation should be set in RelationshipManager
        :type to_register: bool
        :return: (if needed) registered Model
        :rtype: Model
        """
        if self.to.pk_type() == uuid.UUID and isinstance(value, str):  # pragma: nocover
            value = uuid.UUID(value)
        if not isinstance(value, self.to.pk_type()):
            if isinstance(value, self.to_pk_only):
                value = getattr(value, self.to.ormar_config.pkname)
            else:
                raise RelationshipInstanceError(
                    f"Relationship error - ForeignKey {self.to.__name__} "
                    f"is of type {self.to.pk_type()} "
                    f"while {type(value)} passed as a parameter."
                )
        model = create_dummy_instance(fk=self.to, pk=value)
        if to_register:
            self.register_relation(model=model, child=child)
        return model

    def register_relation(self, model: "Model", child: "Model") -> None:
        """
        Registers relation between parent and child in relation manager.
        Relation manager is kep on each model (different instance).

        Used in Metaclass and sometimes some relations are missing
        (i.e. cloned Models in fastapi might miss one).

        :param model: parent model (with relation definition)
        :type model: Model class
        :param child: child model
        :type child: Model class
        """
        model._orm.add(parent=model, child=child, field=self)

    def has_unresolved_forward_refs(self) -> bool:
        """
        Verifies if the filed has any ForwardRefs that require updating before the
        model can be used.

        :return: result of the check
        :rtype: bool
        """
        return self.to.__class__ == ForwardRef

    def expand_relationship(
        self,
        value: Any,
        child: Union["Model", "NewBaseModel"],
        to_register: bool = True,
    ) -> Optional[Union["Model", list["Model"]]]:
        """
        For relations the child model is first constructed (if needed),
        registered in relation and returned.
        For relation fields the value can be a pk value (Any type of field),
        dict (from Model) or actual instance/list of a "Model".

        Selects the appropriate constructor based on a passed value.

        :param value: a Model field value, returned untouched for non relation fields.
        :type value: Any
        :param child: a child Model to register
        :type child: Union["Model", "NewBaseModel"]
        :param to_register: flag if the relation should be set in RelationshipManager
        :type to_register: bool
        :return: returns a Model or a list of Models
        :rtype: Optional[Union["Model", list["Model"]]]
        """
        if value is None:
            return None if not self.virtual else []
        constructors = {
            f"{self.to.__name__}": self._register_existing_model,
            "dict": self._construct_model_from_dict,
            "list": self._extract_model_from_sequence,
        }

        model = constructors.get(  # type: ignore
            value.__class__.__name__, self._construct_model_from_pk
        )(value, child, to_register)
        return model

    def get_relation_name(self) -> str:  # pragma: no cover
        """
        Returns name of the relation, which can be a own name or through model
        names for m2m models

        :return: result of the check
        :rtype: bool
        """
        return self.name

    def get_source_model(self) -> type["Model"]:  # pragma: no cover
        """
        Returns model from which the relation comes -> either owner or through model

        :return: source model
        :rtype: type["Model"]
        """
        return self.owner
