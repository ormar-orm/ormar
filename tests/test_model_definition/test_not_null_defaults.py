import decimal
import typing
import uuid
import warnings

import pydantic
import pytest
import sqlalchemy
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.schema import CreateTable

import ormar
from ormar.warnings import OrmarDeprecatedSince031
from tests.lifespan import init_tests
from tests.schema_helpers import AddColumn
from tests.settings import create_config

base_ormar_config = create_config()
not_null_config = base_ormar_config.copy(not_null_defaults=True)


class Tag(ormar.Model):
    ormar_config = not_null_config.copy(tablename="nn_tags")

    id: int = ormar.Integer(primary_key=True)


class Setting(ormar.Model):
    ormar_config = not_null_config.copy(tablename="nn_settings")

    id: int = ormar.Integer(primary_key=True)
    label: str = ormar.String(max_length=50)
    flag: bool = ormar.Boolean(default=False)
    count: int = ormar.Integer(default=0)
    ratio: float = ormar.Float(default=0.5)
    price: decimal.Decimal = ormar.Decimal(
        default=decimal.Decimal("1.50"), max_digits=10, decimal_places=2
    )
    name: str = ormar.String(max_length=20, default="main")
    notes: str = ormar.Text(default="note")
    token: uuid.UUID = ormar.UUID(default=uuid.uuid4)
    optional_name: typing.Optional[str] = ormar.String(
        max_length=20, default="x", nullable=True
    )
    column_only: typing.Optional[int] = ormar.Integer(
        default=1, nullable=True, sql_nullable=False
    )
    nullable_column: int = ormar.Integer(default=2, sql_nullable=True)
    tags: list[Tag] = ormar.ManyToMany(Tag)


class LegacySetting(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="nn_legacy", not_null_defaults=False
    )

    id: int = ormar.Integer(primary_key=True)
    flag: bool = ormar.Boolean(default=False)
    tags: list[Tag] = ormar.ManyToMany(Tag)


class AuditMixin:
    revision: int = ormar.Integer(default=1)


class BaseEntry(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True)

    active: bool = ormar.Boolean(default=True)
    note: typing.Optional[str] = ormar.String(max_length=20, default="-", nullable=True)


class StrictEntry(BaseEntry, AuditMixin):
    ormar_config = not_null_config.copy(tablename="nn_strict_entries")

    id: int = ormar.Integer(primary_key=True)


class LooseEntry(BaseEntry, AuditMixin):
    ormar_config = base_ormar_config.copy(
        tablename="nn_loose_entries", not_null_defaults=False
    )

    id: int = ormar.Integer(primary_key=True)


create_test_database = init_tests(base_ormar_config)

NOT_NULL_FIELDS = ["flag", "count", "ratio", "price", "name"]


def deprecations(caught: list[warnings.WarningMessage]) -> list[str]:
    """Messages of the not_null_defaults deprecation warnings caught."""
    return [
        str(item.message)
        for item in caught
        if issubclass(item.category, OrmarDeprecatedSince031)
    ]


def test_flag_makes_literal_defaults_not_null():
    columns = Setting.ormar_config.table.columns
    for field_name in NOT_NULL_FIELDS:
        field = Setting.ormar_config.model_fields[field_name]
        assert not field.nullable, field_name
        assert not field.sql_nullable, field_name
        assert not columns[field_name].nullable, field_name
        assert columns[field_name].server_default is not None, field_name
    for field_name in ["notes", "token", "optional_name"]:
        assert Setting.ormar_config.model_fields[field_name].nullable, field_name
        assert columns[field_name].nullable, field_name


def test_explicit_nullable_wins():
    fields = Setting.ormar_config.model_fields
    columns = Setting.ormar_config.table.columns
    assert fields["optional_name"].nullable
    assert columns["optional_name"].nullable
    assert fields["column_only"].nullable
    assert not columns["column_only"].nullable
    assert not fields["nullable_column"].nullable
    assert columns["nullable_column"].nullable


def test_pydantic_types_are_not_optional_under_the_flag():
    annotations = {
        field_name: field.annotation
        for field_name, field in Setting.model_fields.items()
    }
    assert annotations["flag"] is bool
    assert annotations["count"] is int
    assert annotations["name"] is str
    assert annotations["optional_name"] == typing.Optional[str]
    assert annotations["column_only"] == typing.Optional[int]
    properties = Setting.model_json_schema()["properties"]
    assert properties["count"]["type"] == "integer"
    assert "anyOf" in properties["optional_name"]
    assert "count" not in Setting.model_json_schema().get("required", [])
    generated = Setting.get_pydantic(exclude={"tags"})
    assert generated.model_fields["flag"].annotation is bool
    assert LegacySetting.model_fields["flag"].annotation == typing.Optional[bool]
    assert (
        LegacySetting.get_pydantic().model_fields["flag"].annotation
        == (typing.Optional[bool])
    )


def test_none_is_rejected_under_the_flag():
    setting = Setting(label="defaults")
    assert setting.flag is False
    assert setting.count == 0
    with pytest.raises(pydantic.ValidationError):
        Setting(label="none", flag=None)
    assert LegacySetting(flag=None).flag is None


def test_config_copy_passes_the_flag_on():
    assert not_null_config.copy().not_null_defaults is True
    assert not_null_config.copy(not_null_defaults=False).not_null_defaults is False
    assert base_ormar_config.not_null_defaults is None
    assert base_ormar_config.copy().not_null_defaults is None


def test_inherited_fields_follow_each_concrete_config():
    for field_name in ["active", "revision"]:
        assert not StrictEntry.ormar_config.model_fields[field_name].nullable
        assert not StrictEntry.ormar_config.table.columns[field_name].nullable
        assert StrictEntry.model_fields[field_name].annotation in (bool, int)
        assert LooseEntry.ormar_config.model_fields[field_name].nullable
        assert LooseEntry.ormar_config.table.columns[field_name].nullable
    assert BaseEntry.ormar_config.model_fields["active"].nullable
    assert StrictEntry.ormar_config.model_fields["note"].nullable
    assert StrictEntry.ormar_config.table.columns["note"].nullable


def test_through_model_columns_are_not_affected():
    strict_columns = Setting.ormar_config.model_fields["tags"].through
    legacy_columns = LegacySetting.ormar_config.model_fields["tags"].through
    assert [
        column.nullable for column in strict_columns.ormar_config.table.columns
    ] == [column.nullable for column in legacy_columns.ormar_config.table.columns]


def test_unset_flag_warns_once_per_model_listing_the_fields():
    metadata = sqlalchemy.MetaData()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")

        class Product(ormar.Model):
            ormar_config = base_ormar_config.copy(
                metadata=metadata, tablename="products"
            )

            id: int = ormar.Integer(primary_key=True)
            stock: int = ormar.Integer(default=0)
            visible: bool = ormar.Boolean(default=True)
            kept: int = ormar.Integer(default=1, nullable=True)

        class ProductProxy(Product):
            ormar_config = base_ormar_config.copy(metadata=metadata, proxy=True)

        class Plain(ormar.Model):
            ormar_config = base_ormar_config.copy(metadata=metadata, tablename="plain")

            id: int = ormar.Integer(primary_key=True)
            title: str = ormar.String(max_length=10)

        class Draft(ormar.Model):
            ormar_config = base_ormar_config.copy(metadata=metadata, abstract=True)

            ready: bool = ormar.Boolean(default=False)

        class Article(Draft):
            ormar_config = base_ormar_config.copy(
                metadata=metadata, tablename="articles"
            )

            id: int = ormar.Integer(primary_key=True)

    messages = deprecations(caught=caught)
    assert len(messages) == 2
    assert (
        "Model Product: fields with a literal default (stock, visible)" in (messages[0])
    )
    assert "Deprecated in Ormar V0.31 to be removed in V0.32" in messages[0]
    assert "Model Article: fields with a literal default (ready)" in messages[1]
    assert caught[0].filename == __file__
    assert Product.ormar_config.model_fields["stock"].nullable
    assert Product.ormar_config.table.columns["stock"].nullable
    assert ProductProxy.ormar_config.table is Product.ormar_config.table
    assert not Plain.ormar_config.model_fields["title"].nullable
    assert Article.ormar_config.model_fields["ready"].nullable


def test_set_flag_does_not_warn():
    metadata = sqlalchemy.MetaData()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        for flag in (True, False):

            class Order(ormar.Model):
                ormar_config = base_ormar_config.copy(
                    metadata=metadata,
                    tablename=f"orders_{flag}",
                    not_null_defaults=flag,
                )

                id: int = ormar.Integer(primary_key=True)
                paid: bool = ormar.Boolean(default=False)

            assert Order.ormar_config.model_fields["paid"].nullable is not flag


@pytest.mark.parametrize(
    "dialect,expected_fragments",
    [
        (
            sqlite.dialect(),
            [
                "flag BOOLEAN DEFAULT false NOT NULL",
                "ratio FLOAT DEFAULT (0.5) NOT NULL",
                "name VARCHAR(20) DEFAULT 'main' NOT NULL",
            ],
        ),
        (
            postgresql.dialect(),
            [
                "flag BOOLEAN DEFAULT false NOT NULL",
                "ratio FLOAT DEFAULT 0.5 NOT NULL",
                "name VARCHAR(20) DEFAULT 'main' NOT NULL",
            ],
        ),
        (
            mysql.dialect(),
            [
                "flag BOOL NOT NULL DEFAULT false",
                "count INTEGER NOT NULL DEFAULT 0",
                "name VARCHAR(20) NOT NULL DEFAULT 'main'",
            ],
        ),
    ],
)
def test_create_table_ddl(dialect, expected_fragments):
    ddl = str(CreateTable(Setting.ormar_config.table).compile(dialect=dialect))
    for fragment in [*expected_fragments, "optional_name VARCHAR(20) DEFAULT 'x',"]:
        assert fragment in ddl


@pytest.mark.asyncio
async def test_saving_and_loading_under_the_flag():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Setting(label="defaults").save()
            loaded = await Setting.objects.get(label="defaults")
            assert loaded.flag is False
            assert loaded.count == 0
            assert loaded.price == decimal.Decimal("1.50")
            assert loaded.name == "main"
            assert loaded.optional_name == "x"


@pytest.mark.asyncio
async def test_added_not_null_column_fills_existing_rows():
    legacy_metadata = sqlalchemy.MetaData()
    legacy_table = sqlalchemy.Table(
        "nn_legacy_rows",
        legacy_metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    )
    setting_columns = Setting.ormar_config.table.columns
    async with base_ormar_config.engine.begin() as connection:
        await connection.run_sync(legacy_metadata.drop_all)
        await connection.run_sync(legacy_metadata.create_all)
        await connection.execute(legacy_table.insert(), [{"id": 1}, {"id": 2}])
        for field_name in NOT_NULL_FIELDS:
            await connection.execute(
                AddColumn(
                    table_name="nn_legacy_rows",
                    column=setting_columns[field_name],
                )
            )
        extended_table = sqlalchemy.Table(
            "nn_legacy_rows",
            sqlalchemy.MetaData(),
            sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
            *(setting_columns[field_name]._copy() for field_name in NOT_NULL_FIELDS),
        )
        result = await connection.execute(
            extended_table.select().order_by(extended_table.c.id)
        )
        rows = result.mappings().all()
        await connection.run_sync(legacy_metadata.drop_all)

    assert len(rows) == 2
    for row in rows:
        assert row["flag"] is False or row["flag"] == 0
        assert row["count"] == 0
        assert row["ratio"] == 0.5
        assert row["price"] == decimal.Decimal("1.50")
        assert row["name"] == "main"
