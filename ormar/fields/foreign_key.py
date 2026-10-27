import sys
import uuid
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    ForwardRef,
    NamedTuple,
    Optional,
    Union,
    cast,
    overload,
)

import sqlalchemy
from pydantic import BaseModel

import ormar  # noqa I101
from ormar.exceptions import ModelDefinitionError, RelationshipInstanceError
from ormar.fields.base import BaseField
from ormar.fields.key_only import key_only_instance, key_only_model_type
from ormar.fields.link import (
    ColumnPair,
    RelationLink,
    key_parts,
    qualified_fk_reference,
    referenced_value,
)
from ormar.fields.referential_actions import ReferentialAction

if TYPE_CHECKING:  # pragma no cover
    from ormar.models import Model, NewBaseModel, T


class FkParams(NamedTuple):
    """
    Parameters of a relation field derived from the model it leads to.

    :ivar annotation: pydantic type of the field
    :ivar column_type: SQLAlchemy type of the referencing column
    :ivar key_only_model: pydantic model validating key-only values
    """

    annotation: Any
    column_type: Any
    key_only_model: type[BaseModel]


def to_field_names(
    to_field: Union[str, list[str], tuple[str, ...], None],
) -> Optional[tuple[str, ...]]:
    """
    :param to_field: ``to_field`` as given: one field name or several
    :type to_field: Union[str, list[str], tuple[str, ...], None]
    :return: the referenced field names as a tuple, ``None`` for the primary key
    :rtype: Optional[tuple[str, ...]]
    """
    if to_field is None:
        return None
    if isinstance(to_field, str):
        return (to_field,)
    return tuple(to_field)


def populate_fk_params_based_on_to_model(
    *,
    to: type["T"],
    nullable: bool,
    virtual: bool,
    to_field: Union[str, list[str], tuple[str, ...], None] = None,
) -> FkParams:
    """
    Based on target to model to which relation leads to populates the type of the
    pydantic field to use and the type of the referencing column.

    :param to: target related ormar Model
    :type to: Model class
    :param nullable: marks field as optional/ required
    :type nullable: bool
    :param virtual: whether the field is a reverse relation holding no column
    :type virtual: bool
    :param to_field: unique field(s) of the target referenced instead of its pk
    :type to_field: Union[str, list[str], tuple[str, ...], None]
    :return: annotation, column type and key-only model of the relation
    :rtype: FkParams
    """
    key = to.ormar_config.pk
    names = to_field_names(to_field=to_field)
    if names is None:
        target_fields = key.fields
        value_type = key.python_type()
    else:
        validate_to_field(to=to, fields=names)
        target_fields = names
        value_type = (
            to.ormar_config.model_fields[names[0]].__type__
            if len(names) == 1
            else tuple
        )
    target_field = to.ormar_config.model_fields[target_fields[0]]
    key_only_model = key_only_model_type(target=to, fields=target_fields)
    annotation = (
        Union[value_type, to, key_only_model]
        if not nullable
        else Optional[Union[value_type, to, key_only_model]]
    )
    return FkParams(
        annotation=annotation,
        column_type=target_field.column_type,
        key_only_model=key_only_model,
    )


def validate_link_columns(
    to: type["Model"],
    to_field: Union[str, list[str], tuple[str, ...], None],
    columns: Optional[dict[str, str]],
    name: Optional[str],
    primary_key: bool,
) -> None:
    """
    Validate the column options of a foreign key against the columns it
    references.

    :raises ModelDefinitionError: if ``columns=`` doesn't map exactly the
        referenced columns, ``name=`` is used for several columns, or a foreign
        key of several columns is part of a primary key (not supported yet)
    :param to: target model
    :type to: type[Model]
    :param to_field: referenced field(s), ``None`` for the primary key
    :type to_field: Union[str, list[str], tuple[str, ...], None]
    :param columns: local column names by referenced column
    :type columns: Optional[dict[str, str]]
    :param name: single local column name
    :type name: Optional[str]
    :param primary_key: whether the foreign key is part of the primary key
    :type primary_key: bool
    """
    names = to_field_names(to_field=to_field) or to.ormar_config.pk.fields
    referenced = {to.get_column_alias(field_name=field) for field in names}
    if columns is not None and set(columns) != referenced:
        raise ModelDefinitionError(
            f"columns= must map every referenced column of {to.__name__} "
            f"({sorted(referenced)}) to a column name, got {sorted(columns)}."
        )
    if len(names) > 1 and name is not None:
        raise ModelDefinitionError(
            f"A foreign key to several columns of {to.__name__} needs columns= "
            "instead of name=."
        )
    if len(names) > 1 and primary_key:
        raise ModelDefinitionError(
            f"A foreign key to several columns of {to.__name__} cannot be part of "
            "a primary key yet."
        )


def validate_view_target(to: type["Model"], virtual: bool, db_constraint: bool) -> None:
    """
    A database can't enforce a foreign key to a view, so a relation to a model
    mapped to a view must not create one.

    :raises ModelDefinitionError: if a foreign key to a view keeps its constraint
    :param to: target model
    :type to: type[Model]
    :param virtual: whether the field is a reverse relation holding no column
    :type virtual: bool
    :param db_constraint: whether the foreign key creates a database constraint
    :type db_constraint: bool
    """
    if to.ormar_config.view and not virtual and db_constraint:
        raise ModelDefinitionError(
            f"{to.__name__} is a view, a foreign key to it needs db_constraint=False."
        )


def validate_to_field(to: type["Model"], fields: tuple[str, ...]) -> None:
    """
    Check that ``to_field`` names fields identifying one row of the target: its
    primary key, a ``unique=True`` field, or exactly the columns of a unique
    constraint (``UniqueColumns``).

    :raises ModelDefinitionError: if a field does not exist or the set is not
        unique
    :param to: target model
    :type to: type[Model]
    :param fields: referenced field names
    :type fields: tuple[str, ...]
    """
    model_fields = to.ormar_config.model_fields
    for name in fields:
        field = model_fields.get(name)
        if field is None or field.is_relation:
            raise ModelDefinitionError(
                f"to_field '{name}' is not a column field of {to.__name__}."
            )
    aliases = {model_fields[name].get_alias() for name in fields}
    unique_sets = [
        {column.name for column in constraint.columns}
        for constraint in to.ormar_config.table.constraints
        if isinstance(constraint, sqlalchemy.UniqueConstraint)
    ]
    if not (
        set(to.ormar_config.pk.fields) == set(fields)
        or (len(fields) == 1 and model_fields[fields[0]].unique)
        or aliases in unique_sets
    ):
        raise ModelDefinitionError(
            f"to_field {list(fields)} of {to.__name__} must be unique "
            "(primary key, unique=True or a UniqueColumns constraint)."
        )


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
    nullable: Optional[bool] = None,
    related_name: Optional[str] = None,
    virtual: bool = False,
    onupdate: Union[ReferentialAction, str, None] = None,
    ondelete: Union[ReferentialAction, str, None] = None,
    foreign_key_name: Optional[str] = None,
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
    :param nullable: marks field as optional/ required; defaults to optional,
        and to required for a foreign key in the primary key
    :type nullable: Optional[bool]
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
    :param foreign_key_name: optional override for the foreign key constraint name
    generated in migrations. Useful when the auto-generated name exceeds a database
    specific identifier length limit (for example MySQL 64 chars).
    :type foreign_key_name: Optional[str]
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

    primary_key = kwargs.pop("primary_key", False)
    if primary_key and nullable:
        raise ModelDefinitionError(
            "A foreign key that is part of the primary key cannot be nullable."
        )
    nullable = (not primary_key) if nullable is None else nullable
    sql_nullable = kwargs.pop("sql_nullable", None)
    sql_nullable = nullable if sql_nullable is None else sql_nullable

    index = kwargs.pop("index", False)
    to_field = kwargs.pop("to_field", None)
    columns = kwargs.pop("columns", None)
    if columns is not None and name is not None:
        raise ModelDefinitionError(
            "Pass either name= (one column) or columns= (column names by "
            "referenced column), not both."
        )
    db_constraint = kwargs.pop("db_constraint", True)
    if not db_constraint and (
        onupdate is not None or ondelete is not None or foreign_key_name is not None
    ):
        raise ModelDefinitionError(
            "ondelete/onupdate/foreign_key_name need a database constraint, they "
            "cannot be used with db_constraint=False."
        )

    validate_not_allowed_fields(kwargs)
    key_only_model = None
    if to.__class__ == ForwardRef:
        __type__ = to if not nullable else Optional[to]
        column_type = None
    else:
        validate_view_target(
            to=to,  # type: ignore
            virtual=virtual,
            db_constraint=db_constraint,
        )
        params = populate_fk_params_based_on_to_model(
            to=to,  # type: ignore
            nullable=nullable,
            virtual=virtual,
            to_field=to_field,
        )
        validate_link_columns(
            to=to,  # type: ignore
            to_field=to_field,
            columns=columns,
            name=name,
            primary_key=primary_key,
        )
        __type__ = params.annotation
        column_type = params.column_type
        key_only_model = params.key_only_model

    namespace = dict(
        __type__=__type__,
        to=to,
        to_key_only=key_only_model,
        through=None,
        alias=name,
        name=kwargs.pop("real_name", None),
        nullable=nullable,
        sql_nullable=sql_nullable,
        unique=unique,
        column_type=column_type,
        related_name=related_name,
        virtual=virtual,
        primary_key=primary_key,
        index=index,
        default=None,
        server_default=None,
        onupdate=onupdate,
        ondelete=ondelete,
        foreign_key_name=foreign_key_name,
        to_field=to_field,
        columns=columns,
        db_constraint=db_constraint,
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


@overload
def OneToOne(to: type["T"], **kwargs: Any) -> "T":  # pragma: no cover
    ...


@overload
def OneToOne(to: ForwardRef, **kwargs: Any) -> "Model":  # pragma: no cover
    ...


def OneToOne(  # type: ignore
    to: Union[type["T"], "ForwardRef"], **kwargs: Any
) -> "T":
    """
    A foreign key whose related model has at most one model pointing back at it:
    its column is unique (unless it is the primary key, which already is), and
    the reverse side holds a single model or ``None`` instead of a list. The
    default reverse name is the owner model name, without the trailing ``s``.

    Accepts the same arguments as ``ForeignKey``.

    :param to: target related ormar Model
    :type to: Model class
    :param kwargs: ``ForeignKey`` parameters
    :type kwargs: Any
    :return: ormar ForeignKeyField with a one to one relation
    :rtype: ForeignKeyField
    """
    if kwargs.get("unique") is False:
        raise ModelDefinitionError(
            "A OneToOne relation is always unique, use a ForeignKey for a "
            "non-unique relation."
        )
    if not kwargs.get("primary_key"):
        kwargs["unique"] = True
    return cast("T", ForeignKey(to=to, one_to_one=True, **kwargs))


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
        self.foreign_key_name: Optional[str] = kwargs.pop("foreign_key_name", None)
        self.to_field: Union[str, list[str], None] = kwargs.pop("to_field", None)
        self.columns: Optional[dict[str, str]] = kwargs.pop("columns", None)
        self.db_constraint: bool = kwargs.pop("db_constraint", True)
        super().__init__(**kwargs)

    def construct_constraints(self) -> list:
        """
        Build the SQLAlchemy foreign keys of the field's column from its link,
        one per column pair, named ``foreign_key_name`` or
        ``fk_<owner table>_<target table>_<target column>_<field name>``.
        Built on every call, as SQLAlchemy binds a ForeignKey to one table and
        subclasses need their own.

        :return: SQLAlchemy foreign keys, empty until the target is resolved
        :rtype: list
        """
        if (
            self.virtual
            or not self.db_constraint
            or self.has_unresolved_forward_refs()
            or self.owner.ormar_config.view
        ):
            return []
        owner_table = self.owner.ormar_config.tablename
        target_table = self.to.ormar_config.tablename
        return [
            sqlalchemy.ForeignKey(
                column=qualified_fk_reference(
                    target=self.to, column_alias=pair.target_column
                ),
                ondelete=self.ondelete,
                onupdate=self.onupdate,
                name=self.foreign_key_name
                or f"fk_{owner_table}_{target_table}_{pair.target_column}_{self.name}",
            )
            for pair in self.link.pairs
        ]

    def get_related_name(self) -> str:
        """
        Returns name to use for reverse relation.
        It's either set as `related_name` or by default it's owner model. get_name + 's'
        :return: name of the related_name or default related name.
        :rtype: str
        """
        suffix = "" if self.one_to_one else "s"
        return self.related_name or self.owner.get_name() + suffix

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

    @property
    def link(self) -> RelationLink:
        """
        Columns connecting the owner's table to the table of the related model,
        built once per owner and target: a field copied to another model
        (inheritance) or re-targeted (forward refs) builds its own link.

        :return: link from the owner to the related model
        :rtype: RelationLink
        """
        cache_key = (self.owner, self.to, self.through)
        cached = self.__dict__.get("link_cache")
        if cached is not None and cached[0] == cache_key:
            return cached[1]
        link = self.build_link()
        self.__dict__["link_cache"] = (cache_key, link)
        return link

    def build_link(self) -> RelationLink:
        """
        A foreign key links its own column to the target's primary key, a
        reverse relation is the mirror of the foreign key it was created from.

        :return: link from the owner to the related model
        :rtype: RelationLink
        """
        if self.virtual:
            forward = self.to.ormar_config.model_fields[self.get_related_name()]
            return cast("ForeignKeyField", forward).link.mirrored()
        return RelationLink(
            source=self.owner,
            target=self.to,
            pairs=tuple(
                ColumnPair(
                    source_field=self.name,
                    source_column=self.local_column(
                        target_column=self.to.get_column_alias(field_name=field)
                    ),
                    target_field=field,
                    target_column=self.to.get_column_alias(field_name=field),
                )
                for field in self.target_fields()
            ),
        )

    def target_fields(self) -> tuple[str, ...]:
        """
        :return: fields of the related model the foreign key references: its
            ``to_field`` or its primary key fields
        :rtype: tuple[str, ...]
        """
        names = to_field_names(to_field=self.to_field)
        if names is not None:
            return names
        return self.to.ormar_config.pk.fields

    def local_column(self, target_column: str) -> str:
        """
        :param target_column: referenced column of the related model's table
        :type target_column: str
        :return: name of the owner's column storing it: from ``columns=``, the
            field's own column for a single-column key, or
            ``<field>_<referenced column>`` for keys of several columns
        :rtype: str
        """
        if self.columns is not None:
            return self.columns[target_column]
        if len(self.target_fields()) == 1:
            return self.get_alias()
        return f"{self.name}_{target_column}"

    def get_columns(self) -> list[sqlalchemy.Column]:
        """
        :return: the owner's columns of the foreign key, one per referenced
            column (a placeholder single column until a forward reference is
            resolved)
        :rtype: list[sqlalchemy.Column]
        """
        if self.has_unresolved_forward_refs():
            return [self.get_column(self.get_alias())]
        pairs = self.link.pairs
        if len(pairs) == 1:
            return [self.get_column(pairs[0].source_column)]
        target_columns = self.to.ormar_config.table.c
        return [
            sqlalchemy.Column(
                pair.source_column,
                target_columns[pair.target_column].type,
                nullable=self.sql_nullable,
                index=self.index,
                unique=False,
            )
            for pair in pairs
        ]

    def column_aliases(self) -> tuple[str, ...]:
        """
        :return: the owner's column names of the foreign key
        :rtype: tuple[str, ...]
        """
        if self.virtual or self.has_unresolved_forward_refs():
            return (self.get_alias(),)
        return self.link.source_columns()

    def construct_table_constraints(self) -> list:
        """
        :return: the table-level ``FOREIGN KEY (a, b) REFERENCES t (x, y)`` of
            a foreign key of several columns, empty otherwise
        :rtype: list
        """
        if (
            self.virtual
            or self.is_multi
            or not self.db_constraint
            or self.has_unresolved_forward_refs()
            or self.owner.ormar_config.view
        ):
            return []
        pairs = self.link.pairs
        if len(pairs) == 1:
            return []
        owner_table = self.owner.ormar_config.tablename
        target_table = self.to.ormar_config.tablename
        return [
            sqlalchemy.ForeignKeyConstraint(
                [pair.source_column for pair in pairs],
                [
                    qualified_fk_reference(
                        target=self.to, column_alias=pair.target_column
                    )
                    for pair in pairs
                ],
                ondelete=self.ondelete,
                onupdate=self.onupdate,
                name=self.foreign_key_name
                or f"fk_{owner_table}_{target_table}_{self.name}",
            )
        ]

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
            validate_view_target(
                to=self.to, virtual=self.virtual, db_constraint=self.db_constraint
            )
            params = populate_fk_params_based_on_to_model(
                to=self.to,
                nullable=self.nullable,
                virtual=self.virtual,
                to_field=self.to_field,
            )
            if not self.virtual:
                validate_link_columns(
                    to=self.to,
                    to_field=self.to_field,
                    columns=self.columns,
                    name=self.db_alias,
                    primary_key=self.primary_key,
                )
            self.__type__ = params.annotation
            self.column_type = params.column_type
            self.to_key_only = params.key_only_model

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
        key_only_model = None
        target_fields = self.target_fields()
        own_keys = set(value.keys()) - self.to.extract_related_names()
        if (
            own_keys == set(target_fields)
            and all(value.get(field) is not None for field in target_fields)
            and not self.is_through
        ):
            key_only_model = self.to_key_only(**value)
            model = cast(
                "Model",
                self.to._internal_construct(_pk_only=True, _excluded=None, **value),
            )
        else:
            model = self.to(**value)
        if to_register:
            self.register_relation(model=model, child=child)
        return key_only_model if key_only_model is not None else model

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
        model = self.construct_key_only_model(value=value)
        if to_register:
            self.register_relation(model=model, child=child)
        return model

    def construct_key_only_model(self, value: Any) -> "Model":
        """
        Takes a key value and creates an instance of the related model with
        only the key populated, without registering it on any side.

        :param value: value of a related pk / fk column
        :type value: Any
        :raises RelationshipInstanceError: if value does not match the pk type
        :return: related model with only pk set
        :rtype: Model
        """
        if isinstance(value, self.to_key_only):
            values = {field: getattr(value, field) for field in self.target_fields()}
            return key_only_instance(target=self.to, values=values)
        value_type = self.target_value_type()
        if value_type == uuid.UUID and isinstance(value, str):  # pragma: nocover
            value = uuid.UUID(value)
        if not isinstance(value, value_type):
            raise RelationshipInstanceError(
                f"Relationship error - ForeignKey {self.to.__name__} "
                f"is of type {value_type} "
                f"while {type(value)} passed as a parameter."
            )
        return key_only_instance(target=self.to, values=self.target_values(value=value))

    def target_value_of(self, related: Any) -> Any:
        """
        :param related: related model, its key-only model, a dict of its
            fields or a raw value
        :type related: Any
        :return: the value this foreign key stores for it, ``None`` when the
            related model lacks it
        :rtype: Any
        """
        return self.link.values_for(related=related)[self.name]

    def flat_fields(self) -> tuple[str, ...]:
        """
        :return: fields of a related model that represent it in flattened
            output: the referenced fields of a foreign key, the primary key
            of a reverse or many-to-many relation
        :rtype: tuple[str, ...]
        """
        if self.virtual or self.is_multi:
            return self.to.ormar_config.pk.fields
        return self.target_fields()

    def flat_value(self, related: "Model") -> Any:
        """
        Represent a related model in flattened output (``flatten_fields``)::

            Track.album -> 4
            Address.country (to_field="iso_code") -> "PL"
            OrderLine.order (two columns) -> {"tenant_id": 1, "number": 7}

        :param related: related model
        :type related: Model
        :return: the referenced value, a dict of them for several fields
        :rtype: Any
        """
        fields = self.flat_fields()
        if len(fields) == 1:
            return referenced_value(related=related, field=fields[0])
        return {
            field: referenced_value(related=related, field=field) for field in fields
        }

    def flat_value_type(self) -> Any:
        """
        :return: python type of a flattened related model (see ``flat_value``)
        :rtype: Any
        """
        fields = self.flat_fields()
        if len(fields) > 1:
            return dict[str, Any]
        if self.virtual or self.is_multi:
            return self.to.ormar_config.model_fields[fields[0]].__type__
        return self.target_value_type()

    def target_value_type(self) -> Any:
        """
        :return: python type of a raw value of the relation: the ``to_field``
            type or the related model's key type
        :rtype: Any
        """
        names = to_field_names(to_field=self.to_field)
        if names is None:
            return self.to.ormar_config.pk.python_type()
        if len(names) == 1:
            return self.to.ormar_config.model_fields[names[0]].__type__
        return tuple

    def target_values(self, value: Any) -> dict[str, Any]:
        """
        :param value: raw value of the relation
        :type value: Any
        :return: the value split into the referenced fields of the related model
        :rtype: dict[str, Any]
        """
        names = to_field_names(to_field=self.to_field)
        if names is None:
            return self.to.ormar_config.pk.to_dict(value=value)
        return dict(zip(names, key_parts(value=value, size=len(names))))

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

    @cached_property
    def _constructor_dispatch(self) -> dict[str, Any]:
        """
        Per-field map from input class name to the constructor handling that
        shape. Built once at first access — by the time
        ``expand_relationship`` runs, ``_verify_model_can_be_initialized``
        has already gated on ``requires_ref_update``, so the bound methods
        captured here are stable.

        :return: dispatch table for the slow path of ``expand_relationship``
        :rtype: dict[str, Any]
        """
        return {
            "dict": self._construct_model_from_dict,
            "list": self._extract_model_from_sequence,
        }

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
            return [] if self.kind is not None and self.kind.many else None
        # Fast path: ``value`` is already a Model of ``self.to``. Dominant
        # case in row materialization and in user kwargs that pass
        # constructed Models directly. Skips the dispatch table and the
        # ``_register_existing_model`` indirection entirely.
        if value.__class__ is self.to:
            if to_register:
                self.register_relation(model=value, child=cast("Model", child))
            return value
        return self._constructor_dispatch.get(
            value.__class__.__name__, self._construct_model_from_pk
        )(value, child, to_register)

    def get_relation_name(self) -> str:  # pragma: no cover
        """
        Returns name of the relation, which can be a own name or through model
        names for m2m models

        :return: result of the check
        :rtype: bool
        """
        return self.name
