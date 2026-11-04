# type: ignore
import datetime
import decimal
import typing

import pydantic
import pytest
import sqlalchemy

import ormar
from ormar.exceptions import ModelDefinitionError
from ormar.models import Model
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class ExampleModel(Model):
    ormar_config = base_ormar_config.copy(tablename="example")

    test: int = ormar.Integer(primary_key=True)
    test_string: str = ormar.String(max_length=250)
    test_text: str = ormar.Text(default="")
    test_bool: bool = ormar.Boolean(nullable=False)
    test_float = ormar.Float(nullable=True)
    test_datetime = ormar.DateTime(default=datetime.datetime.now)
    test_date = ormar.Date(default=datetime.date.today)
    test_time = ormar.Time(default=datetime.time)
    test_json = ormar.JSON(default={})
    test_bigint: int = ormar.BigInteger(default=0)
    test_smallint: int = ormar.SmallInteger(default=0)
    test_decimal = ormar.Decimal(scale=2, precision=10)
    test_decimal2 = ormar.Decimal(max_digits=10, decimal_places=2)


fields_to_check = [
    "test",
    "test_text",
    "test_string",
    "test_datetime",
    "test_date",
    "test_text",
    "test_float",
    "test_bigint",
    "test_json",
]


class ExampleModel2(Model):
    ormar_config = base_ormar_config.copy(tablename="example2")

    test: int = ormar.Integer(primary_key=True)
    test_string: str = ormar.String(max_length=250)


class User(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users")

    id: int = ormar.Integer(primary_key=True)


class Account(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="accounts")

    id: int = ormar.Integer(primary_key=True)
    user: User = ormar.ForeignKey(User, index=False)


class Purchase(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="purchases")

    id: int = ormar.Integer(primary_key=True)
    user: User = ormar.ForeignKey(User, index=True)


create_test_database = init_tests(base_ormar_config)


@pytest.fixture()
def example():
    return ExampleModel(
        pk=1,
        test_string="test",
        test_bool=True,
        test_decimal=decimal.Decimal(3.5),
        test_decimal2=decimal.Decimal(5.5),
    )


def test_not_nullable_field_is_required():
    with pytest.raises(pydantic.ValidationError):
        ExampleModel(test=1, test_string="test")


def test_model_attribute_access(example):
    assert example.test == 1
    assert example.test_string == "test"
    assert example.test_datetime.year == datetime.datetime.now().year
    assert example.test_date == datetime.date.today()
    assert example.test_text == ""
    assert example.test_float is None
    assert example.test_bigint == 0
    assert example.test_json == {}
    assert example.test_decimal == 3.5
    assert example.test_decimal2 == 5.5

    example.test = 12
    assert example.test == 12

    example._orm_saved = True
    assert example._orm_saved


def test_model_attribute_json_access(example):
    example.test_json = dict(aa=12)
    assert example.test_json == dict(aa=12)


def test_missing_metadata():
    with pytest.raises(ModelDefinitionError):

        class JsonSample2(ormar.Model):
            ormar_config = ormar.OrmarConfig(
                tablename="jsons2",
                database=base_ormar_config.database,
            )

            id: int = ormar.Integer(primary_key=True)
            test_json = ormar.JSON(nullable=True)


def test_missing_database():
    with pytest.raises(ModelDefinitionError):

        class JsonSample3(ormar.Model):
            ormar_config = ormar.OrmarConfig(tablename="jsons3")

            id: int = ormar.Integer(primary_key=True)
            test_json = ormar.JSON(nullable=True)


def test_wrong_pydantic_config():
    with pytest.raises(ModelDefinitionError):

        class ErrorSample(ormar.Model):
            model_config = ["test"]
            ormar_config = ormar.OrmarConfig(tablename="jsons3")

            id: int = ormar.Integer(primary_key=True)
            test_json = ormar.JSON(nullable=True)


def test_non_existing_attr(example):
    with pytest.raises(ValueError):
        example.new_attr = 12


def test_primary_key_access_and_setting(example):
    assert example.pk == 1
    example.pk = 2

    assert example.pk == 2
    assert example.test == 2


def test_pydantic_model_is_created(example):
    assert issubclass(example.__class__, pydantic.BaseModel)
    assert all([field in example.__class__.model_fields for field in fields_to_check])
    assert example.test == 1


def test_sqlalchemy_table_is_created(example):
    assert issubclass(example.ormar_config.table.__class__, sqlalchemy.Table)
    assert all(
        [field in example.ormar_config.table.columns for field in fields_to_check]
    )


@typing.no_type_check
def test_no_pk_in_model_definition():
    with pytest.raises(ModelDefinitionError):  # type: ignore

        class ExampleModel2(Model):  # type: ignore
            ormar_config = base_ormar_config.copy(tablename="example2")

            test_string: str = ormar.String(max_length=250)  # type: ignore


@typing.no_type_check
def test_two_pks_in_model_definition():
    @typing.no_type_check
    class ExampleModel2(Model):
        ormar_config = base_ormar_config.copy(tablename="example3")

        id: int = ormar.Integer(primary_key=True)
        test_string: str = ormar.String(max_length=250, primary_key=True)

    assert ExampleModel2.ormar_config.pk.is_composite
    assert ExampleModel2.ormar_config.pk.fields == ("id", "test_string")


@typing.no_type_check
def test_decimal_error_in_model_definition():
    with pytest.raises(ModelDefinitionError):

        class ExampleModel2(Model):
            ormar_config = base_ormar_config.copy(tablename="example5")

            test: decimal.Decimal = ormar.Decimal(primary_key=True)


@typing.no_type_check
def test_binary_error_without_length_model_definition():
    with pytest.raises(ModelDefinitionError):

        class ExampleModel2(Model):
            ormar_config = base_ormar_config.copy(tablename="example6")

            test: bytes = ormar.LargeBinary(primary_key=True, max_length=-1)


@typing.no_type_check
def test_string_error_in_model_definition():
    with pytest.raises(ModelDefinitionError):

        class ExampleModel2(Model):
            ormar_config = base_ormar_config.copy(tablename="example6")

            test: str = ormar.String(primary_key=True, max_length=0)


@typing.no_type_check
def test_json_conversion_in_model():
    with pytest.raises(pydantic.ValidationError):
        ExampleModel(
            test_json=datetime.datetime.now(),
            test=1,
            test_string="test",
            test_bool=True,
        )


def test_foreign_key_index():
    assert Account.ormar_config.table.columns.user.index is False
    assert Purchase.ormar_config.table.columns.user.index is True
