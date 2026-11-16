"""
Unit tests of the dialect rules. They need no database, so every backend's
rules run in every test run.
"""

import pytest
from sqlalchemy.exc import IntegrityError

from ormar import DatabaseConnection
from ormar.databases.dialects import (
    DialectRules,
    MysqlRules,
    PostgresRules,
    SqliteRules,
    rules_for,
)


@pytest.mark.parametrize(
    "backend_name, rules_class",
    [
        ("sqlite", SqliteRules),
        ("postgresql", PostgresRules),
        ("mysql", MysqlRules),
        ("mariadb", MysqlRules),
        ("oracle", DialectRules),
    ],
)
def test_rules_for_backend(backend_name, rules_class):
    rules = rules_for(backend_name=backend_name)
    assert type(rules) is rules_class
    assert rules.backend_name == backend_name


@pytest.mark.parametrize(
    "url, rules_class",
    [
        ("sqlite+aiosqlite:///test.db", SqliteRules),
        ("postgresql+asyncpg://user:pass@localhost/db", PostgresRules),
        ("mysql+asyncmy://user:pass@localhost/db", MysqlRules),
        ("mariadb+asyncmy://user:pass@localhost/db", MysqlRules),
    ],
)
def test_database_rules_known_before_connect(url, rules_class):
    database = DatabaseConnection(url=url)
    assert not database.is_connected
    assert type(database.rules) is rules_class


def test_rules_flags():
    sqlite_rules = SqliteRules(backend_name="sqlite")
    postgres_rules = PostgresRules(backend_name="postgresql")
    mysql_rules = MysqlRules(backend_name="mariadb")
    assert sqlite_rules.autocommit_engine and mysql_rules.autocommit_engine
    assert not postgres_rules.autocommit_engine
    assert not mysql_rules.conflict_returning
    assert sqlite_rules.conflict_returning and postgres_rules.conflict_returning
    assert not mysql_rules.update_returning
    assert postgres_rules.update_returning
    assert DialectRules(backend_name="oracle").update_returning
    assert not mysql_rules.execute_many_with_trailing_binds
    assert not sqlite_rules.cross_schema_foreign_keys
    assert postgres_rules.cross_schema_foreign_keys
    assert sqlite_rules.foreign_keys_pragma and sqlite_rules.explicit_begin
    assert not mysql_rules.foreign_keys_pragma and not postgres_rules.explicit_begin
    assert sqlite_rules.random_order == postgres_rules.random_order == "random()"
    assert mysql_rules.random_order == "rand()"


def integrity_error(*args):
    return IntegrityError(statement="INSERT", params={}, orig=Exception(*args))


def test_is_duplicate_key():
    mysql_rules = MysqlRules(backend_name="mysql")
    assert mysql_rules.is_duplicate_key(error=integrity_error(1062, "Duplicate"))
    assert not mysql_rules.is_duplicate_key(error=integrity_error(1452, "FK"))
    assert not mysql_rules.is_duplicate_key(error=integrity_error())
    for rules in (
        SqliteRules(backend_name="sqlite"),
        PostgresRules(backend_name="postgresql"),
    ):
        assert not rules.is_duplicate_key(error=integrity_error(1062, "Duplicate"))


@pytest.mark.parametrize(
    "rules, nulls_ordering, expected",
    [
        (SqliteRules(backend_name="sqlite"), "last", "t.c desc nulls last"),
        (PostgresRules(backend_name="postgresql"), "first", "t.c desc nulls first"),
        (MysqlRules(backend_name="mysql"), "first", "t.c is not null, t.c desc"),
        (MysqlRules(backend_name="mariadb"), "last", "t.c is null, t.c desc"),
    ],
)
def test_nulls_order(rules, nulls_ordering, expected):
    assert (
        rules.nulls_order(
            full_column="t.c", ordered="t.c desc", nulls_ordering=nulls_ordering
        )
        == expected
    )


@pytest.mark.parametrize(
    "rules, field_type, descending, expected",
    [
        (SqliteRules(backend_name="sqlite"), bool, False, "min"),
        (MysqlRules(backend_name="mysql"), bool, True, "max"),
        (PostgresRules(backend_name="postgresql"), bool, False, "bool_or"),
        (PostgresRules(backend_name="postgresql"), bool, True, "bool_or"),
        (PostgresRules(backend_name="postgresql"), int, True, "max"),
        (PostgresRules(backend_name="postgresql"), int, False, "min"),
    ],
)
def test_ordering_aggregate(rules, field_type, descending, expected):
    assert (
        rules.ordering_aggregate(field_type=field_type, descending=descending)
        == expected
    )
