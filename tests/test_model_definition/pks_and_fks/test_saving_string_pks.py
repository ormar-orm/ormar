from random import choice
from string import ascii_uppercase

import pytest
import pytest_asyncio
import sqlalchemy
from sqlalchemy import create_engine

import ormar
from ormar import Float, String
from ormar.databases.connection import DatabaseConnection
from tests.settings import ASYNC_DATABASE_URL, SYNC_DATABASE_URL

database = DatabaseConnection(ASYNC_DATABASE_URL)
metadata = sqlalchemy.MetaData()


def get_id() -> str:
    return "".join(choice(ascii_uppercase) for _ in range(12))


base_ormar_config = ormar.OrmarConfig(
    metadata=metadata,
    database=database,
)


class PositionOrm(ormar.Model):
    ormar_config = base_ormar_config.copy()

    name: str = String(primary_key=True, max_length=50)
    x: float = Float()
    y: float = Float()
    degrees: float = Float()


class PositionOrmDef(ormar.Model):
    ormar_config = base_ormar_config.copy()

    name: str = String(primary_key=True, max_length=50, default=get_id)
    x: float = Float()
    y: float = Float()
    degrees: float = Float()


@pytest.fixture(autouse=True, scope="module")
def create_test_database():
    engine = create_engine(SYNC_DATABASE_URL)
    metadata.create_all(engine)
    yield
    metadata.drop_all(engine)


@pytest_asyncio.fixture(scope="function")
async def cleanup():
    yield
    async with database:
        await PositionOrm.objects.delete(each=True)
        await PositionOrmDef.objects.delete(each=True)


@pytest.mark.asyncio
async def test_creating_a_position(cleanup):
    async with database:
        instance = PositionOrm(name="my_pos", x=1.0, y=2.0, degrees=3.0)
        await instance.save()
        assert instance.saved
        assert instance.name == "my_pos"

        instance2 = PositionOrmDef(x=1.0, y=2.0, degrees=3.0)
        await instance2.save()
        assert instance2.saved
        assert instance2.name is not None
        assert len(instance2.name) == 12

        instance3 = PositionOrmDef(x=1.0, y=2.0, degrees=3.0)
        await instance3.save()
        assert instance3.saved
        assert instance3.name is not None
        assert len(instance3.name) == 12
        assert instance2.name != instance3.name
