import os

import sqlalchemy
from sqlalchemy.ext.asyncio import create_async_engine

import ormar
from ormar.databases.connection import DatabaseConnection

MYSQL_DRIVER = os.getenv("MYSQL_DRIVER", "asyncmy")


def convert_to_async_url(url: str) -> str:  # pragma: nocover
    """Convert database URL to async driver version."""
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    elif url.startswith("mysql://"):
        return url.replace("mysql://", f"mysql+{MYSQL_DRIVER}://", 1)
    elif url.startswith("sqlite://"):
        return url.replace("sqlite://", "sqlite+aiosqlite://", 1)
    return url


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///test.db")
ASYNC_DATABASE_URL = convert_to_async_url(DATABASE_URL)
print("USED DB:", ASYNC_DATABASE_URL)


def create_config(**args):
    database_ = DatabaseConnection(ASYNC_DATABASE_URL, **args)
    metadata_ = sqlalchemy.MetaData()
    async_engine_ = create_async_engine(ASYNC_DATABASE_URL)

    return ormar.OrmarConfig(
        metadata=metadata_,
        database=database_,
        engine=async_engine_,
    )
