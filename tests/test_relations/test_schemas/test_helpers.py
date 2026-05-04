"""
Unit tests for the schema-aware helpers introduced for per-model schemas.

These run on every dialect because they exercise the helper functions directly,
not via DDL — so SQLite test runs still cover both branches of the helpers.
"""

import pytest
import sqlalchemy

import ormar
from ormar.exceptions import ModelDefinitionError
from ormar.models.helpers.sqlalchemy import (
    qualified_fk_reference,
    validate_cross_schema_constraints,
)
from tests.settings import create_config

base_ormar_config = create_config()


class HelperUserNoSchema(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="helper_users_plain")

    id: int = ormar.Integer(primary_key=True)


class HelperUserAudit(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="helper_users_audit", schema="audit"
    )

    id: int = ormar.Integer(primary_key=True)


def test_qualified_fk_reference_without_schema():
    assert qualified_fk_reference(HelperUserNoSchema, "id") == "helper_users_plain.id"


def test_qualified_fk_reference_with_schema():
    assert (
        qualified_fk_reference(HelperUserAudit, "id") == "audit.helper_users_audit.id"
    )


def test_validate_cross_schema_returns_early_on_postgres():
    metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        "parent",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        schema="a",
    )
    sqlalchemy.Table(
        "child",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(
            "parent_id", sqlalchemy.Integer, sqlalchemy.ForeignKey("a.parent.id")
        ),
        schema="b",
    )
    # Non-sqlite dialects skip the validation entirely.
    validate_cross_schema_constraints(metadata, "postgresql")
    validate_cross_schema_constraints(metadata, "mysql")


def test_validate_cross_schema_passes_when_schemas_match():
    metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        "parent",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    )
    sqlalchemy.Table(
        "child",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(
            "parent_id", sqlalchemy.Integer, sqlalchemy.ForeignKey("parent.id")
        ),
    )
    validate_cross_schema_constraints(metadata, "sqlite")


def test_validate_cross_schema_raises_on_sqlite_when_schemas_differ():
    metadata = sqlalchemy.MetaData()
    sqlalchemy.Table(
        "parent",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        schema="a",
    )
    sqlalchemy.Table(
        "child",
        metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column(
            "parent_id", sqlalchemy.Integer, sqlalchemy.ForeignKey("a.parent.id")
        ),
        schema="b",
    )
    with pytest.raises(ModelDefinitionError, match="across schemas"):
        validate_cross_schema_constraints(metadata, "sqlite")
