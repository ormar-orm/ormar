import uuid

import pytest
import sqlalchemy
from sqlalchemy import create_engine

import ormar
from ormar.databases.connection import DatabaseConnection
from tests.settings import ASYNC_DATABASE_URL, SYNC_DATABASE_URL

metadata = sqlalchemy.MetaData()
db = DatabaseConnection(ASYNC_DATABASE_URL)


class User(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        tablename="user",
        metadata=metadata,
        database=db,
    )

    id: uuid.UUID = ormar.UUID(
        primary_key=True, default=uuid.uuid4, uuid_format="string"
    )
    username = ormar.String(index=True, unique=True, null=False, max_length=255)
    email = ormar.String(index=True, unique=True, nullable=False, max_length=255)
    hashed_password = ormar.String(null=False, max_length=255)
    is_active = ormar.Boolean(default=True, nullable=False)
    is_superuser = ormar.Boolean(default=False, nullable=False)


class Token(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        tablename="token",
        metadata=metadata,
        database=db,
    )

    id = ormar.Integer(primary_key=True)
    text = ormar.String(max_length=4, unique=True)
    user = ormar.ForeignKey(User, related_name="tokens")
    created_at = ormar.DateTime(server_default=sqlalchemy.func.now())


@pytest.fixture(autouse=True, scope="module")
def create_test_database():
    engine = create_engine(SYNC_DATABASE_URL)
    metadata.create_all(engine)
    yield
    metadata.drop_all(engine)


@pytest.mark.asyncio
async def test_uuid_fk():
    async with db:
        async with db.transaction(force_rollback=True):
            user = await User.objects.create(
                username="User1",
                email="email@example.com",
                hashed_password="^$EDACVS(&A&Y@2131aa",
                is_active=True,
                is_superuser=False,
            )
            await Token.objects.create(text="AAAA", user=user)
            await Token.objects.order_by("-created_at").all()
