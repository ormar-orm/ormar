import logging
import re
from typing import TYPE_CHECKING, Any, ForwardRef, Optional, Union, cast

import sqlalchemy

import ormar  # noqa: I100, I202
from ormar.databases.dialects import rules_for
from ormar.fields.link import qualified_fk_reference
from ormar.models.descriptors import PkDescriptor, RelationDescriptor
from ormar.models.helpers.pydantic import create_pydantic_field
from ormar.models.helpers.related_names_validation import (
    validate_related_names_in_relations,
)
from ormar.models.primary_key import build_primary_key

if TYPE_CHECKING:  # pragma no cover
    from ormar import BaseField, ForeignKeyField, ManyToManyField, Model
    from ormar.models import NewBaseModel
    from ormar.models.ormar_config import OrmarConfig
    from ormar.models.primary_key import PrimaryKey


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
    if rules_for(backend_name=dialect_name).cross_schema_foreign_keys:
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
    Sets ormar.ForeignKey from through model to both child and parent models,
    unless the through model already declares them (see resolve_through_fields).
    Sets sqlalchemy.ForeignKey to both child and parent models.
    Sets pydantic fields with child and parent model types.

    :param model_field: relation field defined in parent model
    :type model_field: ManyToManyField
    """
    model_fields = model_field.through.ormar_config.model_fields
    sides = (
        (
            model_field.default_target_field_name(),
            model_field.to,
            model_field.through_reverse_foreign_key_name,
            model_field.through_reverse_relation_nullable,
        ),
        (
            model_field.default_source_field_name(),
            model_field.owner,
            model_field.through_foreign_key_name,
            model_field.through_relation_nullable,
        ),
    )
    for field_name, target, foreign_key_name, nullable in sides:
        existing = model_fields.get(field_name)
        if (
            existing is not None
            and existing.is_relation
            and not existing.virtual
            and existing.to is target
        ):
            continue
        model_fields[field_name] = ormar.ForeignKey(  # type: ignore
            to=target,
            real_name=field_name,
            ondelete=None if target.ormar_config.view else "CASCADE",
            db_constraint=not target.ormar_config.view,
            owner=model_field.through,
        )
        create_and_append_m2m_fk(
            model=target,
            model_field=model_field,
            field_name=field_name,
            foreign_key_name=foreign_key_name,
            nullable=nullable,
        )
        create_pydantic_field(
            field_name=field_name, model=target, model_field=model_field
        )
        setattr(model_field.through, field_name, RelationDescriptor(name=field_name))


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
    through_field = cast(
        "ForeignKeyField", model_field.through.ormar_config.model_fields[field_name]
    )
    if len(through_field.link.pairs) > 1:
        append_multi_column_m2m_fk(
            through_field=through_field,
            foreign_key_name=foreign_key_name,
            nullable=nullable,
        )
        return
    (pk_alias,) = model.ormar_config.pk.column_aliases()
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
    foreign_keys = (
        []
        if model.ormar_config.view
        else [
            sqlalchemy.schema.ForeignKey(
                qualified_fk_reference(target=model, column_alias=pk_alias),
                ondelete="CASCADE",
                onupdate="CASCADE",
                name=foreign_key_name or default_name,
            )
        ]
    )
    column = sqlalchemy.Column(
        field_name,
        pk_column.type,
        *foreign_keys,
        nullable=nullable,
    )
    model_field.through.ormar_config.columns.append(column)
    model_field.through.ormar_config.table.append_column(column, replace_existing=True)


def append_multi_column_m2m_fk(
    through_field: "ForeignKeyField",
    foreign_key_name: Optional[str],
    nullable: bool,
) -> None:
    """
    Add the columns of a through model foreign key to a model with a composite
    primary key (``<field>_<key column>``), with one composite
    ``FOREIGN KEY`` constraint (``ON DELETE / ON UPDATE CASCADE``), built by
    the foreign key field itself.

    :param through_field: foreign key of the through model to that model
    :type through_field: ForeignKeyField
    :param foreign_key_name: name of the constraint, default generated
    :type foreign_key_name: Optional[str]
    :param nullable: whether the columns are nullable
    :type nullable: bool
    """
    through = through_field.owner
    table = through.ormar_config.table
    through_field.sql_nullable = nullable
    through_field.onupdate = "CASCADE"
    if foreign_key_name is not None:
        through_field.foreign_key_name = foreign_key_name
    for column in through_field.get_columns():
        through.ormar_config.columns.append(column)
        table.append_column(column, replace_existing=True)
    for constraint in through_field.construct_table_constraints():
        table.append_constraint(constraint)


def sqlalchemy_columns_from_model_fields(
    model_fields: dict, new_model: type["Model"]
) -> tuple[tuple[str, ...], list[sqlalchemy.Column]]:
    """
    Iterates over declared on Model model fields and extracts fields that
    should be treated as database fields.

    If the model is empty it sets mandatory id field as primary key
    (used in through models in m2m relations).

    Triggers a validation of relation_names in relation fields. If multiple fields
    are leading to the same related model only one can have empty related_name param.
    Also related_names have to be unique.


    Sets `owner` on each model_field as reference to newly created Model.

    :raises ModelDefinitionError: if validation of related_names fail
    :param model_fields: dictionary of declared ormar model fields
    :type model_fields: dict[str, ormar.Field]
    :param new_model:
    :type new_model: Model class
    :return: names of the primary key fields, list of sqlalchemy columns
    :rtype: tuple[tuple[str, ...], list[sqlalchemy.Column]]
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
) -> tuple[tuple[str, ...], list[sqlalchemy.Column]]:
    """
    Helper method.

    Collects the primary key field names and columns.

    Sets `owner` on each model_field as reference to newly created Model.

    :param model_fields: dictionary of declared ormar model fields
    :type model_fields: dict[str, ormar.Field]
    :param new_model:
    :type new_model: Model class
    :return: names of the primary key fields, list of sqlalchemy columns
    :rtype: tuple[tuple[str, ...], list[sqlalchemy.Column]]
    """
    columns = []
    pk_fields = []
    for field_name, field in model_fields.items():
        field.owner = new_model
        if _is_through_model_not_set(field):
            field.create_default_through_model()
        if field.primary_key:
            pk_fields.append(field_name)
        if _is_db_field(field):
            columns.extend(field.get_columns())
    return tuple(pk_fields), merge_shared_columns(model=new_model, columns=columns)


def names_columns_explicitly(field: "BaseField") -> bool:
    """
    :param field: field storing a column
    :type field: BaseField
    :return: whether the field is a foreign key naming its columns itself
        (``columns=`` / ``name=``), so a column it shares with another field
        is meant, not a clash of generated names
    :rtype: bool
    """
    return field.is_relation and (
        cast("ForeignKeyField", field).columns is not None or bool(field.db_alias)
    )


def column_owners(
    model: Union[type["Model"], type["NewBaseModel"]],
) -> dict[str, list["BaseField"]]:
    """
    :param model: model whose fields are inspected
    :type model: Union[type[Model], type[NewBaseModel]]
    :return: fields storing each database column, in declaration order
    :rtype: dict[str, list[BaseField]]
    """
    owners: dict[str, list["BaseField"]] = {}
    for field in model.ormar_config.model_fields.values():
        if _is_db_field(field):
            for column in field.column_aliases():
                owners.setdefault(column, []).append(field)
    return owners


def validate_shared_column(
    model: Union[type["Model"], type["NewBaseModel"]],
    name: str,
    fields: list["BaseField"],
    columns: list[sqlalchemy.Column],
) -> None:
    """
    Check that a column stored by several fields may be shared: at most one
    of them is a plain field, every foreign key among them names the column
    itself, and they agree on its type.

    :raises ModelDefinitionError: if the fields can't share the column
    :param model: model the column belongs to
    :type model: Union[type[Model], type[NewBaseModel]]
    :param name: column name
    :type name: str
    :param fields: fields storing the column
    :type fields: list[BaseField]
    :param columns: the column as each field defines it
    :type columns: list[sqlalchemy.Column]
    """
    plain = [field for field in fields if not field.is_relation]
    relations = [field for field in fields if field.is_relation]
    if len(plain) > 1 or not all(
        names_columns_explicitly(field=field) for field in relations
    ):
        raise ormar.ModelDefinitionError(
            f"Column {name} of {model.__name__} is used by two fields, "
            "rename one of them (name= / columns=)."
        )
    if len({repr(column.type) for column in columns}) > 1:
        raise ormar.ModelDefinitionError(
            f"Fields {[field.name for field in fields]} of {model.__name__} share "
            f"column {name} but give it different types "
            f"({', '.join(repr(column.type) for column in columns)})."
        )


def merge_shared_columns(
    model: Union[type["Model"], type["NewBaseModel"]],
    columns: list[sqlalchemy.Column],
) -> list[sqlalchemy.Column]:
    """
    Keep one column per name when several fields store the same column (e.g.
    a ``tenant_id`` shared by two foreign keys of a link table), built by
    ``merged_column``.

    :raises ModelDefinitionError: if the fields can't share a column
    :param model: model the columns belong to
    :type model: Union[type[Model], type[NewBaseModel]]
    :param columns: columns of the model's fields, in declaration order
    :type columns: list[sqlalchemy.Column]
    :return: columns with each name once
    :rtype: list[sqlalchemy.Column]
    """
    by_name: dict[str, list[sqlalchemy.Column]] = {}
    for column in columns:
        by_name.setdefault(column.name, []).append(column)
    if all(len(same) == 1 for same in by_name.values()):
        return columns
    owners = column_owners(model=model)
    merged = []
    for name, same in by_name.items():
        if len(same) == 1:
            merged.append(same[0])
            continue
        validate_shared_column(
            model=model, name=name, fields=owners[name], columns=same
        )
        merged.append(merged_column(name=name, fields=owners[name], columns=same))
    return merged


def merged_column(
    name: str,
    fields: list["BaseField"],
    columns: list[sqlalchemy.Column],
) -> sqlalchemy.Column:
    """
    Build the one column several fields share (all of them give it the same
    type). It is part of the primary key when any of the fields is, nullable
    only when all of them are and never generated by the database. Fields
    storing the column on their own (a plain field, foreign keys of one
    column) add their foreign keys, index and unique flags; the default,
    server default and comment come from the plain field (foreign keys set
    none).

    :param name: column name
    :type name: str
    :param fields: fields storing the column, in declaration order
    :type fields: list[BaseField]
    :param columns: the column as each field defines it
    :type columns: list[sqlalchemy.Column]
    :return: the shared column
    :rtype: sqlalchemy.Column
    """
    own = [field for field in fields if len(field.column_aliases()) == 1]
    plain = next((field for field in own if not field.is_relation), None)
    foreign_keys = [
        foreign_key for field in own for foreign_key in field.construct_constraints()
    ]
    return sqlalchemy.Column(
        name,
        columns[0].type,
        *foreign_keys,
        primary_key=any(column.primary_key for column in columns),
        autoincrement=False,
        nullable=all(column.nullable for column in columns),
        index=any(field.index for field in own) or None,
        unique=any(field.unique for field in own) or None,
        default=plain.ormar_default if plain is not None else None,
        server_default=plain.server_default if plain is not None else None,
        comment=plain.comment if plain is not None else None,
    )


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


def prepare_composite_key_fields(
    key: "PrimaryKey", columns: list[sqlalchemy.Column]
) -> None:
    """
    Validate the fields of a composite primary key and turn off the
    autoincrement integer keys get by default, as no part of a composite key
    is generated by the database.

    :raises ModelDefinitionError: if a part explicitly autoincrements
    :param key: composite primary key of the model
    :type key: PrimaryKey
    :param columns: the model's columns, adjusted in place
    :type columns: list[sqlalchemy.Column]
    """
    model_fields = key.owner.ormar_config.model_fields
    key_fields = key.fields
    generated = [
        name
        for name in key_fields
        if model_fields[name].autoincrement
        and model_fields[name].autoincrement_explicit
    ]
    if generated:
        raise ormar.ModelDefinitionError(
            f"Composite primary key fields cannot autoincrement: {generated}"
        )
    key_columns = set(key.column_aliases())
    for column in columns:
        if column.name in key_columns:
            column.autoincrement = False


def populate_config_tablename_columns_and_pk(
    name: str, new_model: type["Model"]
) -> type["Model"]:
    """
    Sets Model tablename if it's not already set in OrmarConfig.
    Default tablename if not present is class name lower + s (i.e. Bed becomes -> beds)

    Checks if Model's OrmarConfig have primary key and columns set.
    If not calls the sqlalchemy_columns_from_model_fields to populate
    columns from ormar.fields definitions.

    :raises ModelDefinitionError: if no field is marked as primary key.
    Each model has to have pk.

    :param name: name of the current Model
    :type name: str
    :param new_model: currently constructed Model
    :type new_model: ormar.models.metaclass.ModelMetaclass
    :return: Model with populated primary key and columns in OrmarConfig
    :rtype: ormar.models.metaclass.ModelMetaclass
    """
    tablename = name.lower() + "s"
    new_model.ormar_config.tablename = (
        new_model.ormar_config.tablename
        if new_model.ormar_config.tablename
        else tablename
    )
    if new_model.ormar_config.columns:
        # a config with columns is shared with the model that built them
        # (e.g. pydantic copies made for circular references), keep its key
        columns = new_model.ormar_config.columns
        key = new_model.ormar_config.pk
    else:
        pk_fields, columns = sqlalchemy_columns_from_model_fields(
            model_fields=new_model.ormar_config.model_fields, new_model=new_model
        )
        if not pk_fields:
            raise ormar.ModelDefinitionError("Table has to have a primary key.")
        key = build_primary_key(owner=new_model, fields=pk_fields)
        if key.is_composite:
            prepare_composite_key_fields(key=key, columns=columns)

    new_model.ormar_config.columns = columns
    new_model.ormar_config.pk = key
    if not new_model.ormar_config.orders_by:
        # by default, we sort by the primary key if other option not provided
        new_model.ormar_config.orders_by.extend(key.fields)
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
        # a view gets its own metadata so create_all and Alembic leave it alone
        table = sqlalchemy.Table(
            config.tablename,
            (
                sqlalchemy.MetaData(
                    schema=config.metadata.schema,
                    naming_convention=config.metadata.naming_convention,
                )
                if config.view
                else config.metadata
            ),
            *config.columns,
            *config.constraints,
            *(
                constraint
                for field in config.model_fields.values()
                for constraint in field.construct_table_constraints()
            ),
            schema=config.schema,
            comment=config.comment,
        )
        config.table = table


def index_name_fragment(expression: Any) -> str:
    """
    Returns an identifier-safe name fragment for an index column or SQL
    expression, so auto-generated index names work for functional indexes
    (e.g. LOWER(name)) as well as plain columns.

    :param expression: an index column name or SQL expression element
    :type expression: Any
    :return: sanitized fragment containing only alphanumerics and underscores
    :rtype: str
    """
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(expression)).strip("_")


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
            constraint.name = sqlalchemy.quoted_name(
                f"ix_{config.tablename}_"
                f"{'_'.join(index_name_fragment(e) for e in constraint.expressions)}",
                quote=None,
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
            columns[ind : ind + 1] = field.get_columns()
            columns[:] = merge_shared_columns(model=model, columns=columns)
            break


def rebuild_primary_key(model: Union[type["Model"], type["NewBaseModel"]]) -> None:
    """
    A key whose only field is a foreign key to a forward reference is built as
    a key of one column; once the reference resolves to a model with a key of
    several columns, the foreign key stores several columns and the model's
    key becomes composite.

    :param model: model whose forward references were just resolved
    :type model: Union[type[Model], type[NewBaseModel]]
    """
    key = model.ormar_config.pk
    if key.is_composite or len(key.column_aliases()) == 1:
        return
    composite = build_primary_key(owner=cast(type["Model"], model), fields=key.fields)
    model.ormar_config.pk = composite
    model.pk = PkDescriptor(key=composite)  # type: ignore[assignment]
