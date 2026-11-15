"""
IndexColumns subclasses sqlalchemy Index, not ColumnCollectionConstraint, so the
constraints annotation of OrmarConfig has to accept both. This module has no
``type: ignore`` and mypy checks it, so it fails the type check if it does not.
"""

import sqlalchemy

import ormar
from tests.settings import create_config

base_ormar_config: ormar.OrmarConfig = create_config()


class Shooter(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        metadata=base_ormar_config.metadata,
        database=base_ormar_config.database,
        engine=base_ormar_config.engine,
        tablename="typed_shooters",
        constraints=[
            ormar.IndexColumns("club", "name"),
            ormar.UniqueColumns("license"),
            ormar.CheckColumns("age >= 0"),
        ],
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    club: str = ormar.String(max_length=100)
    license: str = ormar.String(max_length=20)
    age: int = ormar.Integer()


class Range(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="typed_ranges",
        constraints=[
            ormar.IndexColumns("city", name="ix_range_city"),
            ormar.UniqueColumns("city", "name"),
            ormar.CheckColumns("lanes > 0"),
        ],
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    city: str = ormar.String(max_length=100)
    lanes: int = ormar.Integer()


def constraint_names(table: sqlalchemy.Table, kind: type) -> set:
    return {
        constraint.name
        for constraint in table.constraints
        if isinstance(constraint, kind)
    }


def test_constraints_in_ormar_config_are_on_the_table():
    table = Shooter.ormar_config.table
    index_names = {index.name for index in table.indexes}
    assert index_names == {"ix_typed_shooters_club_name"}
    assert constraint_names(table, sqlalchemy.UniqueConstraint) == {
        "uc_typed_shooters_license"
    }
    assert constraint_names(table, sqlalchemy.CheckConstraint) == {
        "check_typed_shooters_age_>=_0"
    }


def test_constraints_in_config_copy_are_on_the_table():
    table = Range.ormar_config.table
    index_names = {index.name for index in table.indexes}
    assert index_names == {"ix_range_city"}
    assert constraint_names(table, sqlalchemy.UniqueConstraint) == {
        "uc_typed_ranges_city_name"
    }
    assert constraint_names(table, sqlalchemy.CheckConstraint) == {
        "check_typed_ranges_lanes_>_0"
    }
