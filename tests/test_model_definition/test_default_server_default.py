import decimal
import enum
import uuid

import pytest
import sqlalchemy
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateColumn, CreateTable, ExecutableDDLElement
from sqlalchemy.sql.compiler import DDLCompiler

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Level(enum.IntEnum):
    LOW = 1


class AddColumn(ExecutableDDLElement):
    """``ALTER TABLE ... ADD COLUMN`` of a column, as a migration tool emits it."""

    def __init__(self, table_name: str, column: sqlalchemy.Column) -> None:
        self.table_name = table_name
        self.column = column


@compiles(AddColumn)
def compile_add_column(
    element: AddColumn, compiler: DDLCompiler, **kwargs: object
) -> str:
    """Render the column specification with the dialect's DDL compiler."""
    column_spec = compiler.get_column_specification(element.column)
    return f"ALTER TABLE {element.table_name} ADD COLUMN {column_spec}"


class Gadget(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="default_gadgets")

    id: int = ormar.Integer(primary_key=True)
    label: str = ormar.String(max_length=50)
    flag: bool = ormar.Boolean(default=False)
    active: bool = ormar.Boolean(default=True)
    count: int = ormar.Integer(default=5)
    offset: int = ormar.Integer(default=-3)
    big_count: int = ormar.BigInteger(default=7)
    ratio: float = ormar.Float(default=0.5)
    whole_ratio: float = ormar.Float(default=2)
    price: decimal.Decimal = ormar.Decimal(
        default=decimal.Decimal("1.50"), max_digits=10, decimal_places=2
    )
    whole_price: decimal.Decimal = ormar.Decimal(
        default=3, max_digits=10, decimal_places=2
    )
    name: str = ormar.String(max_length=20, default="it's")
    backslash: str = ormar.String(max_length=20, default="C:\\path\\n")
    percent: str = ormar.String(max_length=20, default="50%% off :name %s")
    multiline: str = ormar.String(max_length=20, default="first\nsecond")
    accented: str = ormar.String(max_length=20, default="zażółć 🙂")
    lone_percent: str = ormar.String(max_length=20, default="100%")


class Excluded(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="default_excluded")

    id: int = ormar.Integer(primary_key=True)
    notes: str = ormar.Text(default="note")
    payload: dict = ormar.JSON(default={"key": 1})
    data: bytes = ormar.LargeBinary(max_length=10, default=b"bytes")
    color: Color = ormar.Enum(enum_class=Color, default=Color.RED)
    shade: str = ormar.String(max_length=10, default=Color.BLUE)
    token: uuid.UUID = ormar.UUID(default=uuid.uuid4)
    counter: int = ormar.Integer(default=lambda: 1)
    flag_as_int: bool = ormar.Boolean(default=0)
    number_as_str: int = ormar.Integer(default="5")
    number_as_enum: int = ormar.Integer(default=Level.LOW)
    number_as_bool: int = ormar.Integer(default=True)
    huge_ratio: float = ormar.Float(default=10**400)
    not_a_number: float = ormar.Float(default=float("nan"))
    infinite_price: decimal.Decimal = ormar.Decimal(
        default=decimal.Decimal("Infinity"), max_digits=10, decimal_places=2
    )
    secret: str = ormar.String(
        max_length=200,
        default="hidden",
        encrypt_secret="secret123",
        encrypt_backend=ormar.EncryptBackends.FERNET,
    )


class StringPk(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="default_string_pk")

    code: str = ormar.String(max_length=10, primary_key=True, default="main")


class Account(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="default_accounts")

    tenant_id: int = ormar.Integer(primary_key=True)
    login: str = ormar.String(max_length=20, primary_key=True)


class Membership(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="default_memberships")

    id: int = ormar.Integer(primary_key=True)
    account: Account = ormar.ForeignKey(
        Account,
        columns={"tenant_id": "tenant_id", "login": "account_login"},
        related_name="memberships",
    )
    tenant_id: int = ormar.Integer(default=4, nullable=False)


class Explicit(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="default_explicit")

    id: int = ormar.Integer(primary_key=True)
    count: int = ormar.Integer(default=1, server_default=sqlalchemy.text("2"))
    flag: bool = ormar.Boolean(default=False, nullable=False)
    strict_flag: bool = ormar.Boolean(default=True, sql_nullable=False)


create_test_database = init_tests(base_ormar_config)

DERIVED_DEFAULTS = {
    "flag": False,
    "active": True,
    "count": 5,
    "offset": -3,
    "big_count": 7,
    "ratio": 0.5,
    "whole_ratio": 2.0,
    "price": decimal.Decimal("1.50"),
    "whole_price": decimal.Decimal("3.00"),
    "name": "it's",
    "backslash": "C:\\path\\n",
    "percent": "50%% off :name %s",
    "multiline": "first\nsecond",
    "accented": "zażółć 🙂",
    "lone_percent": "100%",
}


def server_default_sql(table: sqlalchemy.Table, column_name: str) -> str:
    """Render the server default of a column as PostgreSQL DDL text."""
    column = table.columns[column_name]
    ddl = str(CreateColumn(column).compile(dialect=postgresql.dialect()))
    return ddl.split(" DEFAULT ", 1)[1].removesuffix(" NOT NULL")


def test_simple_literal_defaults_get_a_server_default():
    table = Gadget.ormar_config.table
    assert server_default_sql(table=table, column_name="flag") == "false"
    assert server_default_sql(table=table, column_name="active") == "true"
    assert server_default_sql(table=table, column_name="count") == "5"
    assert server_default_sql(table=table, column_name="offset") == "-3"
    assert server_default_sql(table=table, column_name="big_count") == "7"
    assert server_default_sql(table=table, column_name="ratio") == "0.5"
    assert server_default_sql(table=table, column_name="whole_ratio") == "2.0"
    assert server_default_sql(table=table, column_name="price") == "1.50"
    assert server_default_sql(table=table, column_name="whole_price") == "3"
    assert server_default_sql(table=table, column_name="name") == "'it''s'"
    assert server_default_sql(table=table, column_name="multiline") == "'first\nsecond'"
    assert table.columns["label"].server_default is None


def test_other_defaults_get_no_server_default():
    columns = Excluded.ormar_config.table.columns
    for column_name in Excluded.ormar_config.model_fields:
        assert columns[column_name].server_default is None, column_name
    assert StringPk.ormar_config.table.columns["code"].server_default is None


def test_shared_column_takes_the_plain_field_default():
    column = Membership.ormar_config.table.columns["tenant_id"]
    assert not column.nullable
    assert (
        server_default_sql(table=Membership.ormar_config.table, column_name="tenant_id")
        == "4"
    )


def test_explicit_server_default_and_nullable_win():
    columns = Explicit.ormar_config.table.columns
    assert columns["count"].server_default.arg.text == "2"
    assert not columns["flag"].nullable
    assert (
        server_default_sql(table=Explicit.ormar_config.table, column_name="flag")
        == "false"
    )
    assert not columns["strict_flag"].nullable
    assert Explicit.ormar_config.model_fields["strict_flag"].nullable


def test_field_server_default_stays_unset():
    for field_name in DERIVED_DEFAULTS:
        field = Gadget.ormar_config.model_fields[field_name]
        assert field.server_default is None
        assert field.has_default(use_server=False)
        assert field.nullable
        assert Gadget.ormar_config.table.columns[field_name].nullable
    assert not Gadget.ormar_config.model_fields["label"].has_default()


@pytest.mark.parametrize(
    "dialect,expected_fragments",
    [
        (
            sqlite.dialect(),
            ["flag BOOLEAN DEFAULT false", "ratio FLOAT DEFAULT (0.5)"],
        ),
        (
            postgresql.dialect(),
            ["flag BOOLEAN DEFAULT false", "ratio FLOAT DEFAULT 0.5"],
        ),
        (mysql.dialect(), ["flag BOOL DEFAULT false", "count INTEGER DEFAULT 5"]),
    ],
)
def test_create_table_ddl(dialect, expected_fragments):
    gadget_ddl = str(CreateTable(Gadget.ormar_config.table).compile(dialect=dialect))
    excluded_ddl = str(
        CreateTable(Excluded.ormar_config.table).compile(dialect=dialect)
    )
    for fragment in [*expected_fragments, "name VARCHAR(20) DEFAULT 'it''s'"]:
        assert fragment in gadget_ddl
    assert "notes TEXT," in excluded_ddl
    assert "DEFAULT" not in excluded_ddl


@pytest.mark.asyncio
async def test_saving_keeps_runtime_defaults_and_explicit_none():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with_defaults = await Gadget(label="defaults").save()
            explicit_none = await Gadget(label="none", flag=None, count=None).save()

            loaded = await Gadget.objects.get(label="defaults")
            for field_name, value in DERIVED_DEFAULTS.items():
                assert getattr(with_defaults, field_name) == value
                assert getattr(loaded, field_name) == value

            loaded_none = await Gadget.objects.get(label="none")
            assert explicit_none.flag is None
            assert loaded_none.flag is None
            assert loaded_none.count is None
            assert loaded_none.ratio == 0.5


@pytest.mark.asyncio
async def test_added_column_fills_existing_rows():
    legacy_metadata = sqlalchemy.MetaData()
    legacy_table = sqlalchemy.Table(
        "default_legacy_gadgets",
        legacy_metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    )
    gadget_columns = Gadget.ormar_config.table.columns
    async with base_ormar_config.engine.begin() as connection:
        await connection.run_sync(legacy_metadata.drop_all)
        await connection.run_sync(legacy_metadata.create_all)
        await connection.execute(legacy_table.insert(), [{"id": 1}, {"id": 2}])
        # the column DDL comes from the dialect compiler directly; how alembic
        # renders these defaults is not covered as alembic is not a test dependency
        for field_name in DERIVED_DEFAULTS:
            await connection.execute(
                AddColumn(
                    table_name="default_legacy_gadgets",
                    column=gadget_columns[field_name],
                )
            )
        extended_table = sqlalchemy.Table(
            "default_legacy_gadgets",
            sqlalchemy.MetaData(),
            sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
            *(gadget_columns[field_name]._copy() for field_name in DERIVED_DEFAULTS),
        )
        result = await connection.execute(
            extended_table.select().order_by(extended_table.c.id)
        )
        rows = result.mappings().all()
        await connection.run_sync(legacy_metadata.drop_all)

    assert len(rows) == 2
    for row in rows:
        for field_name, value in DERIVED_DEFAULTS.items():
            assert row[field_name] == value, field_name
