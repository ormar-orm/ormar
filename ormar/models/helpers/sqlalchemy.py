import logging
from typing import TYPE_CHECKING, ForwardRef, Optional, Union

import sqlalchemy

import ormar  # noqa: I100, I202
from ormar.models.descriptors import RelationDescriptor
from ormar.models.helpers.pydantic import create_pydantic_field
from ormar.models.helpers.related_names_validation import (
    validate_related_names_in_relations,
)

if TYPE_CHECKING:  # pragma no cover
    from ormar import BaseField, ForeignKeyField, ManyToManyField, Model
    from ormar.models import NewBaseModel
    from ormar.models.ormar_config import OrmarConfig


def qualified_fk_reference(target: type["Model"], column_alias: str) -> str:
    """
    Builds the dotted reference string passed to ``sqlalchemy.ForeignKey``.

    SQLAlchemy expects ``"schema.table.column"`` for cross-schema foreign keys
    and ``"table.column"`` when the target is in the default schema.

    :param target: target ormar Model
    :type target: type["Model"]
    :param column_alias: alias of the referenced column on the target table
    :type column_alias: str
    :return: dotted reference string
    :rtype: str
    """
    config = target.ormar_config
    prefix = f"{config.schema}." if config.schema else ""
    return f"{prefix}{config.tablename}.{column_alias}"


def validate_cross_schema_constraints(
    metadata: sqlalchemy.MetaData, dialect_name: str
) -> None:
    """
    Rejects cross-schema foreign keys on dialects that cannot enforce them.

    SQLite forbids foreign keys whose parent and child tables live in different
    attached databases. Detecting this at metaclass time is unreliable because
    models are usually declared before the engine/dialect is known, so the check
    runs against the assembled metadata before ``create_all``.

    :raises ModelDefinitionError: if a cross-schema FK is found on SQLite
    :param metadata: metadata holding all the registered ormar tables
    :type metadata: sqlalchemy.MetaData
    :param dialect_name: name of the dialect about to issue DDL
    :type dialect_name: str
    """
    if dialect_name != "sqlite":
        return
    for table in metadata.sorted_tables:
        for fk in table.foreign_keys:
            parent_schema = fk.column.table.schema
            child_schema = table.schema
            if parent_schema != child_schema:
                raise ormar.ModelDefinitionError(
                    f"SQLite does not support foreign keys across schemas: "
                    f"{table.fullname}.{fk.parent.name} -> {fk.column.table.fullname}."
                    " Use PostgreSQL or MySQL, or keep both tables in the same schema."
                )


def adjust_through_many_to_many_model(model_field: "ManyToManyField") -> None:
    """
    Registers m2m relation on through model.
    Sets ormar.ForeignKey from through model to both child and parent models.
    Sets sqlalchemy.ForeignKey to both child and parent models.
    Sets pydantic fields with child and parent model types.

    :param model_field: relation field defined in parent model
    :type model_field: ManyToManyField
    """
    parent_name = model_field.default_target_field_name()
    child_name = model_field.default_source_field_name()
    model_fields = model_field.through.ormar_config.model_fields
    model_fields[parent_name] = ormar.ForeignKey(  # type: ignore
        model_field.to,
        real_name=parent_name,
        ondelete="CASCADE",
        owner=model_field.through,
    )

    model_fields[child_name] = ormar.ForeignKey(  # type: ignore
        model_field.owner,
        real_name=child_name,
        ondelete="CASCADE",
        owner=model_field.through,
    )

    create_and_append_m2m_fk(
        model=model_field.to,
        model_field=model_field,
        field_name=parent_name,
        foreign_key_name=model_field.through_reverse_foreign_key_name,
        nullable=model_field.through_reverse_relation_nullable,
    )
    create_and_append_m2m_fk(
        model=model_field.owner,
        model_field=model_field,
        field_name=child_name,
        foreign_key_name=model_field.through_foreign_key_name,
        nullable=model_field.through_relation_nullable,
    )

    create_pydantic_field(parent_name, model_field.to, model_field)
    create_pydantic_field(child_name, model_field.owner, model_field)

    setattr(model_field.through, parent_name, RelationDescriptor(name=parent_name))
    setattr(model_field.through, child_name, RelationDescriptor(name=child_name))


def create_and_append_m2m_fk(
    model: type["Model"],
    model_field: "ManyToManyField",
    field_name: str,
    foreign_key_name: Optional[str] = None,
    nullable: bool = True,
) -> None:
    """
    Registers sqlalchemy Column with sqlalchemy.ForeignKey leading to the model.

    Newly created field is added to m2m relation
    through model OrmarConfig columns and table.

    :param field_name: name of the column to create
    :type field_name: str
    :param model: Model class to which FK should be created
    :type model: Model class
    :param model_field: field with ManyToMany relation
    :type model_field: ManyToManyField field
    :param foreign_key_name: optional override for the generated FK constraint name.
    :type foreign_key_name: Optional[str]
    :param nullable: whether the created column is nullable.
    :type nullable: bool
    """
    pk_alias = model.get_column_alias(model.ormar_config.pkname)
    pk_column = next(
        (col for col in model.ormar_config.columns if col.name == pk_alias), None
    )
    if pk_column is None:  # pragma: no cover
        raise ormar.ModelDefinitionError(
            "ManyToMany relation cannot lead to field without pk"
        )
    through_table = model_field.through.ormar_config.tablename
    target_table = model.ormar_config.tablename
    default_name = f"fk_{through_table}_{target_table}_{field_name}_{pk_alias}"
    column = sqlalchemy.Column(
        field_name,
        pk_column.type,
        sqlalchemy.schema.ForeignKey(
            qualified_fk_reference(model, pk_alias),
            ondelete="CASCADE",
            onupdate="CASCADE",
            name=foreign_key_name or default_name,
        ),
        nullable=nullable,
    )
    model_field.through.ormar_config.columns.append(column)
    model_field.through.ormar_config.table.append_column(column, replace_existing=True)


def check_pk_column_validity(
    field_name: str, field: "BaseField", pkname: Optional[str]
) -> Optional[str]:
    """
    Receives the field marked as primary key and verifies if the pkname
    was not already set (only one allowed per model).

    :raises ModelDefintionError: if pkname already set
    :param field_name: name of field
    :type field_name: str
    :param field: ormar.Field
    :type field: BaseField
    :param pkname: already set pkname
    :type pkname: Optional[str]
    :return: name of the field that should be set as pkname
    :rtype: str
    """
    if pkname is not None:
        raise ormar.ModelDefinitionError("Only one primary key column is allowed.")
    return field_name


def sqlalchemy_columns_from_model_fields(
    model_fields: dict, new_model: type["Model"]
) -> tuple[Optional[str], list[sqlalchemy.Column]]:
    """
    Iterates over declared on Model model fields and extracts fields that
    should be treated as database fields.

    If the model is empty it sets mandatory id field as primary key
    (used in through models in m2m relations).

    Triggers a validation of relation_names in relation fields. If multiple fields
    are leading to the same related model only one can have empty related_name param.
    Also related_names have to be unique.

    Trigger validation of primary_key - only one and required pk can be set

    Sets `owner` on each model_field as reference to newly created Model.

    :raises ModelDefinitionError: if validation of related_names fail,
    or pkname validation fails.
    :param model_fields: dictionary of declared ormar model fields
    :type model_fields: dict[str, ormar.Field]
    :param new_model:
    :type new_model: Model class
    :return: pkname, list of sqlalchemy columns
    :rtype: tuple[Optional[str], list[sqlalchemy.Column]]
    """
    if len(model_fields.keys()) == 0:
        model_fields["id"] = ormar.Integer(name="id", primary_key=True)
        logging.warning(
            f"Table {new_model.ormar_config.tablename} had no fields so auto "
            "Integer primary key named `id` created."
        )
    validate_related_names_in_relations(model_fields, new_model)
    return _process_fields(model_fields=model_fields, new_model=new_model)


def _process_fields(
    model_fields: dict, new_model: type["Model"]
) -> tuple[Optional[str], list[sqlalchemy.Column]]:
    """
    Helper method.

    Populates pkname and columns.

    Sets `owner` on each model_field as reference to newly created Model.

    :raises ModelDefinitionError: if validation of related_names fail,
    or pkname validation fails.
    :param model_fields: dictionary of declared ormar model fields
    :type model_fields: dict[str, ormar.Field]
    :param new_model:
    :type new_model: Model class
    :return: pkname, list of sqlalchemy columns
    :rtype: tuple[Optional[str], list[sqlalchemy.Column]]
    """
    columns = []
    pkname = None
    for field_name, field in model_fields.items():
        field.owner = new_model
        if _is_through_model_not_set(field):
            field.create_default_through_model()
        if field.primary_key:
            pkname = check_pk_column_validity(field_name, field, pkname)
        if _is_db_field(field):
            columns.append(field.get_column(field.get_alias()))
    return pkname, columns


def _is_through_model_not_set(field: "BaseField") -> bool:
    """
    Alias to if check that verifies if through model was created.
    :param field: field to check
    :type field: "BaseField"
    :return: result of the check
    :rtype: bool
    """
    return field.is_multi and not field.through and not field.to.__class__ == ForwardRef


def _is_db_field(field: "BaseField") -> bool:
    """
    Alias to if check that verifies if field should be included in database.
    :param field: field to check
    :type field: "BaseField"
    :return: result of the check
    :rtype: bool
    """
    return not field.virtual and not field.is_multi


def populate_config_tablename_columns_and_pk(
    name: str, new_model: type["Model"]
) -> type["Model"]:
    """
    Sets Model tablename if it's not already set in OrmarConfig.
    Default tablename if not present is class name lower + s (i.e. Bed becomes -> beds)

    Checks if Model's OrmarConfig have pkname and columns set.
    If not calls the sqlalchemy_columns_from_model_fields to populate
    columns from ormar.fields definitions.

    :raises ModelDefinitionError: if pkname is not present raises ModelDefinitionError.
    Each model has to have pk.

    :param name: name of the current Model
    :type name: str
    :param new_model: currently constructed Model
    :type new_model: ormar.models.metaclass.ModelMetaclass
    :return: Model with populated pkname and columns in OrmarConfig
    :rtype: ormar.models.metaclass.ModelMetaclass
    """
    tablename = name.lower() + "s"
    new_model.ormar_config.tablename = (
        new_model.ormar_config.tablename
        if new_model.ormar_config.tablename
        else tablename
    )
    pkname: Optional[str]

    if new_model.ormar_config.columns:
        columns = new_model.ormar_config.columns
        pkname = new_model.ormar_config.pkname
    else:
        pkname, columns = sqlalchemy_columns_from_model_fields(
            new_model.ormar_config.model_fields, new_model
        )

    if pkname is None:
        raise ormar.ModelDefinitionError("Table has to have a primary key.")

    new_model.ormar_config.columns = columns
    new_model.ormar_config.pkname = pkname
    if not new_model.ormar_config.orders_by:
        # by default, we sort by pk name if other option not provided
        new_model.ormar_config.orders_by.append(pkname)
    return new_model


def check_for_null_type_columns_from_forward_refs(config: "OrmarConfig") -> bool:
    """
    Check is any column is of NUllType() meaning it's empty column from ForwardRef

    :param config: OrmarConfig of the Model without sqlalchemy table constructed
    :type config: Model class OrmarConfig
    :return: result of the check
    :rtype: bool
    """
    return not any(
        isinstance(col.type, sqlalchemy.sql.sqltypes.NullType) for col in config.columns
    )


def populate_config_sqlalchemy_table_if_required(config: "OrmarConfig") -> None:
    """
    Constructs sqlalchemy table out of columns and parameters set on OrmarConfig.
    It populates name, metadata, columns and constraints.

    :param config: OrmarConfig of the Model without sqlalchemy table constructed
    :type config: Model class OrmarConfig
    """
    if config.table is None and check_for_null_type_columns_from_forward_refs(
        config=config
    ):
        set_constraint_names(config=config)
        table = sqlalchemy.Table(
            config.tablename,
            config.metadata,
            *config.columns,
            *config.constraints,
            schema=config.schema,
        )
        config.table = table


def set_constraint_names(config: "OrmarConfig") -> None:
    """
    Populates the names on IndexColumns and UniqueColumns and CheckColumns constraints.

    :param config: OrmarConfig of the Model without sqlalchemy table constructed
    :type config: Model class OrmarConfig
    """
    for constraint in config.constraints:
        if isinstance(constraint, sqlalchemy.UniqueConstraint) and not constraint.name:
            constraint.name = (
                f"uc_{config.tablename}_"
                f"{'_'.join([str(col) for col in constraint._pending_colargs])}"
            )
        elif (
            isinstance(constraint, sqlalchemy.Index)
            and constraint.name == "TEMPORARY_NAME"
        ):
            constraint.name = (
                f"ix_{config.tablename}_"
                f"{'_'.join([col for col in constraint._pending_colargs])}"
            )
        elif isinstance(constraint, sqlalchemy.CheckConstraint) and not constraint.name:
            sql_condition: str = str(constraint.sqltext).replace(" ", "_")
            constraint.name = f"check_{config.tablename}_{sql_condition}"


def update_column_definition(
    model: Union[type["Model"], type["NewBaseModel"]], field: "ForeignKeyField"
) -> None:
    """
    Updates a column with a new type column based on updated parameters in FK fields.

    :param model: model on which columns needs to be updated
    :type model: type["Model"]
    :param field: field with column definition that requires update
    :type field: ForeignKeyField
    :return: None
    :rtype: None
    """
    columns = model.ormar_config.columns
    for ind, column in enumerate(columns):
        if column.name == field.get_alias():
            new_column = field.get_column(field.get_alias())
            columns[ind] = new_column
            break
