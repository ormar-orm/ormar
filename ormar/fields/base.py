from typing import TYPE_CHECKING, Any, Literal, Optional, Union, overload

import sqlalchemy
from pydantic.fields import FieldInfo, _Unset

import ormar  # noqa I101
from ormar import ModelDefinitionError
from ormar.fields.sqlalchemy_encrypted import (
    EncryptBackend,
    EncryptBackends,
    EncryptedString,
)

if TYPE_CHECKING:  # pragma no cover
    from ormar.models import Model, NewBaseModel


class BaseField(FieldInfo):  # type: ignore[misc]
    """
    BaseField serves as a parent class for all basic Fields in ormar.
    It keeps all common parameters available for all fields as well as
    set of useful functions.

    All values are kept as class variables, ormar Fields are never instantiated.
    Subclasses pydantic.FieldInfo to keep the fields related
    to pydantic field types like ConstrainedStr
    """

    def __init__(self, **kwargs: Any) -> None:
        self.__type__: type = kwargs.pop("__type__", None)
        self.__pydantic_type__: type = kwargs.pop("__pydantic_type__", None)
        self.__sample__: type = kwargs.pop("__sample__", None)
        self.related_name = kwargs.pop("related_name", None)

        self.column_type: sqlalchemy.Column = kwargs.pop("column_type", None)
        self.constraints: list = kwargs.pop("constraints", list())
        self.name: str = kwargs.pop("name", None)
        self.db_alias: str = kwargs.pop("alias", None)

        self.primary_key: bool = kwargs.pop("primary_key", False)
        self.autoincrement: bool = kwargs.pop("autoincrement", False)
        self.nullable: bool = kwargs.pop("nullable", False)
        self.sql_nullable: bool = kwargs.pop("sql_nullable", False)
        self.index: bool = kwargs.pop("index", False)
        self.unique: bool = kwargs.pop("unique", False)

        self.virtual: bool = kwargs.pop(
            "virtual", None
        )  # ManyToManyFields and reverse ForeignKeyFields
        self.is_multi: bool = kwargs.pop("is_multi", None)  # ManyToManyField
        self.is_relation: bool = kwargs.pop(
            "is_relation", None
        )  # ForeignKeyField + subclasses
        self.is_through: bool = kwargs.pop("is_through", False)  # ThroughFields

        self.through_relation_name = kwargs.pop("through_relation_name", None)
        self.through_reverse_relation_name = kwargs.pop(
            "through_reverse_relation_name", None
        )

        self.skip_reverse: bool = kwargs.pop("skip_reverse", False)
        self.skip_field: bool = kwargs.pop("skip_field", False)

        self.owner: type["Model"] = kwargs.pop("owner", None)
        self.to: type["Model"] = kwargs.pop("to", None)
        self.to_pk_only: type["Model"] = kwargs.pop("to_pk_only", None)
        self.through: type["Model"] = kwargs.pop("through", None)
        self.self_reference: bool = kwargs.pop("self_reference", False)
        self.self_reference_primary: Optional[str] = kwargs.pop(
            "self_reference_primary", None
        )
        self.orders_by: Optional[list[str]] = kwargs.pop("orders_by", None)
        self.related_orders_by: Optional[list[str]] = kwargs.pop(
            "related_orders_by", None
        )

        self.encrypt_secret: str = kwargs.pop("encrypt_secret", None)
        self.encrypt_backend: EncryptBackends = kwargs.pop(
            "encrypt_backend", EncryptBackends.NONE
        )
        self.encrypt_custom_backend: Optional[type[EncryptBackend]] = kwargs.pop(
            "encrypt_custom_backend", None
        )

        self.ormar_default: Any = kwargs.pop("default", None)
        self.server_default: Any = kwargs.pop("server_default", None)

        self.comment: str = kwargs.pop("comment", None)

        self.represent_as_base64_str: bool = kwargs.pop(
            "represent_as_base64_str", False
        )

        for name, value in kwargs.items():
            setattr(self, name, value)

        kwargs.update(self.get_pydantic_default())
        super().__init__(**kwargs)

    def is_valid_uni_relation(self) -> bool:
        """
        Checks if field is a relation definition but only for ForeignKey relation,
        so excludes ManyToMany fields, as well as virtual ForeignKey
        (second side of FK relation).

        Is used to define if a field is a db ForeignKey column that
        should be saved/populated when dealing with internal/own
        Model columns only.

        :return: result of the check
        :rtype: bool
        """
        return not self.is_multi and not self.virtual

    def get_alias(self) -> str:
        """
        Used to translate Model column names to database column names during db queries.

        :return: returns custom database column name if defined by user,
        otherwise field name in ormar/pydantic
        :rtype: str
        """
        return self.db_alias if self.db_alias else self.name

    def get_pydantic_default(self) -> dict:
        """
        Generates base pydantic.FieldInfo with only default and optionally
        required to fix pydantic Json field being set to required=False.
        Used in an ormar Model Metaclass.

        :return: instance of base pydantic.FieldInfo
        :rtype: pydantic.FieldInfo
        """
        base = self.default_value()
        if base is None:
            base = dict(default=None) if self.nullable else dict(default=_Unset)
        return base

    def default_value(self, use_server: bool = False) -> Optional[dict]:
        """
        Returns a FieldInfo instance with populated default
        (static) or default_factory (function).
        If the field is a autoincrement primary key the default is None.
        Otherwise field have to has either default, or default_factory populated.

        If all default conditions fail None is returned.

        Used in converting to pydantic FieldInfo.

        :param use_server: flag marking if server_default should be
        treated as default value, default False
        :type use_server: bool
        :return: returns a call to pydantic.Field
        which is returning a FieldInfo instance
        :rtype: Optional[pydantic.FieldInfo]
        """
        if self.is_auto_primary_key():
            return dict(default=None)
        if self.has_default(use_server=use_server):
            default = (
                self.ormar_default
                if self.ormar_default is not None
                else self.server_default
            )
            if callable(default):
                return dict(default_factory=default)
            return dict(default=default)
        return None

    @overload
    def get_default(
        self,
        *,
        call_default_factory: Literal[True],
        validated_data: Union[dict[str, Any], None] = None,
    ) -> Any: ...

    @overload
    def get_default(self, *, call_default_factory: Literal[False] = ...) -> Any: ...

    def get_default(
        self,
        *,
        call_default_factory: bool = True,
        validated_data: Union[dict[str, Any], None] = None,
        use_server: bool = False,
    ) -> Any:  # noqa CCR001
        """
        Return default value for a field.
        If the field is Callable the function is called and actual result is returned.
        Used to populate default_values for pydantic Model in ormar Model Metaclass.

        :param use_server: flag marking if server_default should be
        treated as default value, default False
        :type use_server: bool
        :return: default value for the field if set, otherwise implicit None
        :rtype: Any
        """
        if self.has_default():
            default = (
                self.ormar_default
                if self.ormar_default is not None
                else self._get_default_server_value(use_server=use_server)
            )
            return self._get_default_callable_value(
                default=default,
                call_default_factory=call_default_factory,
            )

    def _get_default_server_value(self, use_server: bool) -> Any:  # pragma: no cover
        """
        Return default value for a server side if use_server is True
        """
        return self.server_default if use_server else None

    @staticmethod
    def _get_default_callable_value(default: Any, call_default_factory: bool) -> Any:
        """
        Return default factory value if call_default_factory is True
        and default is a callable.
        """
        return default() if (callable(default) and call_default_factory) else default

    def has_default(self, use_server: bool = True) -> bool:
        """
        Checks if the field has default value set.

        :param use_server: flag marking if server_default should be
        treated as default value, default False
        :type use_server: bool
        :return: result of the check if default value is set
        :rtype: bool
        """
        return self.ormar_default is not None or (
            self.server_default is not None and use_server
        )

    def is_auto_primary_key(self) -> bool:
        """
        Checks if field is first a primary key and if it,
        it's than check if it's set to autoincrement.
        Autoincrement primary_key is nullable/optional.

        :return: result of the check for primary key and autoincrement
        :rtype: bool
        """
        if self.primary_key:
            return self.autoincrement
        return False

    def construct_constraints(self) -> list:
        """
        Converts list of ormar constraints into sqlalchemy ForeignKeys.
        Has to be done dynamically as sqlalchemy binds ForeignKey to the table.
        And we need a new ForeignKey for subclasses of current model

        :return: list of sqlalchemy foreign keys - by default one.
        :rtype: list[sqlalchemy.schema.ForeignKey]
        """
        constraints = [
            sqlalchemy.ForeignKey(
                con.reference,
                ondelete=con.ondelete,
                onupdate=con.onupdate,
                name=f"fk_{self.owner.ormar_config.tablename}_{self.to.ormar_config.tablename}"
                f"_{self.to.get_column_alias(self.to.ormar_config.pkname)}_{self.name}",
            )
            for con in self.constraints
        ]
        return constraints

    def get_column(self, name: str) -> sqlalchemy.Column:
        """
        Returns definition of sqlalchemy.Column used in creation of sqlalchemy.Table.
        Populates name, column type constraints, as well as a number of parameters like
        primary_key, index, unique, nullable, default and server_default.

        :param name: name of the db column - used if alias is not set
        :type name: str
        :return: actual definition of the database column as sqlalchemy requires.
        :rtype: sqlalchemy.Column
        """
        if self.encrypt_backend == EncryptBackends.NONE:
            column: sqlalchemy.Column = sqlalchemy.Column(
                self.db_alias or name,
                self.column_type,
                *self.construct_constraints(),
                primary_key=self.primary_key,
                autoincrement=self.autoincrement,
                nullable=self.sql_nullable,
                index=self.index,
                unique=self.unique,
                default=self.ormar_default,
                server_default=self.server_default,
                comment=self.comment,
            )
        else:
            column = self._get_encrypted_column(name=name)
        return column

    def _get_encrypted_column(self, name: str) -> sqlalchemy.Column:
        """
        Returns EncryptedString column type instead of actual column.

        :param name: column name
        :type name: str
        :return: newly defined column
        :rtype:  sqlalchemy.Column
        """
        if self.primary_key or self.is_relation:
            raise ModelDefinitionError(
                "Primary key field and relations fields cannot be encrypted!"
            )
        column: sqlalchemy.Column = sqlalchemy.Column(
            self.db_alias or name,
            EncryptedString(
                _field_type=self,
                encrypt_secret=self.encrypt_secret,
                encrypt_backend=self.encrypt_backend,
                encrypt_custom_backend=self.encrypt_custom_backend,
            ),
            nullable=self.nullable,
            index=self.index,
            unique=self.unique,
            default=self.ormar_default,
            server_default=self.server_default,
        )
        return column

    def expand_relationship(
        self,
        value: Any,
        child: Union["Model", "NewBaseModel"],
        to_register: bool = True,
    ) -> Any:
        """
        Function overwritten for relations, in basic field the value is returned as is.
        For relations the child model is first constructed (if needed),
        registered in relation and returned.
        For relation fields the value can be a pk value (Any type of field),
        dict (from Model) or actual instance/list of a "Model".

        :param value: a Model field value, returned untouched for non relation fields.
        :type value: Any
        :param child: a child Model to register
        :type child: Union["Model", "NewBaseModel"]
        :param to_register: flag if the relation should be set in RelationshipManager
        :type to_register: bool
        :return: returns untouched value for normal fields, expands only for relations
        :rtype: Any
        """
        return value

    def set_self_reference_flag(self) -> None:
        """
        Sets `self_reference` to True if field to and owner are same model.
        :return: None
        :rtype: None
        """
        if self.owner is not None and (
            self.owner == self.to or self.owner.ormar_config == self.to.ormar_config
        ):
            self.self_reference = True
            self.self_reference_primary = self.name

    def has_unresolved_forward_refs(self) -> bool:
        """
        Verifies if the filed has any ForwardRefs that require updating before the
        model can be used.

        :return: result of the check
        :rtype: bool
        """
        return False

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

    def get_related_name(self) -> str:
        """
        Returns name to use for reverse relation.
        It's either set as `related_name` or by default it's owner model. get_name + 's'
        :return: name of the related_name or default related name.
        :rtype: str
        """
        return ""  # pragma: no cover
