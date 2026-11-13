from typing import Optional

import pydantic
import pytest
import sqlalchemy
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.schema import CreateTable

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="unsigned_owners")

    id: int = ormar.Integer(primary_key=True, unsigned=True)
    big: int = ormar.BigInteger(unsigned=True, default=0)
    small: int = ormar.SmallInteger(unsigned=True, minimum=10, default=10)


class Pet(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="unsigned_pets")

    id: int = ormar.Integer(primary_key=True)
    owner: Optional[Owner] = ormar.ForeignKey(Owner)


create_test_database = init_tests(base_ormar_config)


def create_table_sql(model: type[ormar.Model], dialect) -> str:
    return str(CreateTable(model.ormar_config.table).compile(dialect=dialect))


def test_unsigned_columns_on_mysql():
    owners = create_table_sql(Owner, mysql.dialect())
    assert "id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT" in owners
    assert "big BIGINT UNSIGNED" in owners
    assert "small SMALLINT UNSIGNED" in owners
    assert "owner INTEGER UNSIGNED" in create_table_sql(Pet, mysql.dialect())
    assert "CHECK" not in owners


def test_unsigned_columns_keep_plain_type_on_other_databases():
    owners = create_table_sql(Owner, postgresql.dialect())
    assert "id SERIAL NOT NULL" in owners
    assert "big BIGINT" in owners
    assert "UNSIGNED" not in owners
    for dialect in (postgresql.dialect(), sqlite.dialect()):
        owners = create_table_sql(Owner, dialect)
        assert "CHECK (id >= 0)" in owners
        assert "CHECK (big >= 0)" in owners
        assert "CHECK (small >= 0)" in owners


def test_unsigned_rejects_negative_values():
    with pytest.raises(pydantic.ValidationError):
        Owner(big=-1)
    with pytest.raises(pydantic.ValidationError):
        Owner(small=5)
    big_schema = Owner.model_json_schema()["properties"]["big"]
    assert {"minimum": 0, "type": "integer"} in big_schema["anyOf"]


@pytest.mark.asyncio
async def test_unsigned_columns_store_and_load_values():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            owner = await Owner.objects.create(big=2**40, small=300)
            await Pet.objects.create(owner=owner)

            pet = await Pet.objects.select_related("owner").get()
            assert pet.owner.id == owner.id
            assert (pet.owner.big, pet.owner.small) == (2**40, 300)


@pytest.mark.asyncio
async def test_database_rejects_negative_unsigned_value():
    async with base_ormar_config.database:
        with pytest.raises(sqlalchemy.exc.DBAPIError):
            async with base_ormar_config.database.connection() as connection:
                await connection.execute(
                    sqlalchemy.text(
                        "INSERT INTO unsigned_owners (big, small) VALUES (-1, 10)"
                    )
                )
