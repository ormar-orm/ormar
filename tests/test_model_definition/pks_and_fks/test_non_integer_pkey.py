import random

import pytest
import sqlalchemy

import ormar
from ormar.databases.connection import DatabaseConnection
from tests.settings import ASYNC_DATABASE_URL, SYNC_DATABASE_URL

database = DatabaseConnection(ASYNC_DATABASE_URL)
metadata = sqlalchemy.MetaData()


def key():
    return "".join(random.choice("abcdefgh123456") for _ in range(8))


class Model(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        tablename="models",
        metadata=metadata,
        database=database,
    )

    id: str = ormar.String(primary_key=True, default=key, max_length=8)
    name: str = ormar.String(max_length=32)


@pytest.fixture(autouse=True, scope="function")
def create_test_database():
    engine = sqlalchemy.create_engine(SYNC_DATABASE_URL)
    metadata.create_all(engine)
    yield
    metadata.drop_all(engine)


@pytest.mark.asyncio
async def test_pk_1():
    async with database:
        model = await Model.objects.create(name="NAME")
        assert isinstance(model.id, str)


@pytest.mark.asyncio
async def test_pk_2():
    async with database:
        model = await Model.objects.create(name="NAME")
        assert await Model.objects.all() == [model]
