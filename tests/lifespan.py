from contextlib import asynccontextmanager
from typing import AsyncIterator

import pytest_asyncio
import sqlalchemy
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateSchema, DropSchema, DropTable

from ormar import OrmarConfig
from ormar.models.helpers.sqlalchemy import validate_cross_schema_constraints
from tests.settings import ASYNC_DATABASE_URL


class DropTableCascade(DropTable):
    """``DROP TABLE`` variant that emits ``CASCADE`` on PostgreSQL.

    SQLAlchemy's ``DropTable`` exposes only ``element`` and ``if_exists`` — no
    ``cascade`` flag (verified on 2.0.x and on the 2.1 branch; ``DropSchema``
    has ``cascade``, ``DropTable`` is the odd one out). The ``@compiles``
    extension below is upstream's recommended workaround. We need ``CASCADE``
    because tests share a database between modules and may leave dependent
    objects from earlier runs that block a plain drop.
    """


@compiles(DropTableCascade, "postgresql")
def _compile_drop_table_cascade(
    element: DropTableCascade,
    compiler: sqlalchemy.sql.compiler.DDLCompiler,
    **_: object,
) -> str:  # pragma: no cover
    return compiler.visit_drop_table(element) + " CASCADE"


def lifespan(config):
    @asynccontextmanager
    async def do_lifespan(_: FastAPI) -> AsyncIterator[None]:
        if not config.database.is_connected:
            await config.database.connect()

        yield

        if config.database.is_connected:
            await config.database.disconnect()

    return do_lifespan


def collect_schemas(metadata: sqlalchemy.MetaData) -> list[str]:
    """Returns the deduplicated, sorted list of non-default schemas in metadata."""
    return sorted({t.schema for t in metadata.sorted_tables if t.schema})


def create_schemas(
    connection: sqlalchemy.Connection, config: OrmarConfig
) -> None:  # pragma: no cover
    schemas = collect_schemas(config.metadata)
    if connection.dialect.name not in ("postgresql", "mysql"):
        return
    for schema in schemas:
        connection.execute(CreateSchema(schema, if_not_exists=True))


def drop_schemas(
    connection: sqlalchemy.Connection, config: OrmarConfig
) -> None:  # pragma: no cover
    schemas = collect_schemas(config.metadata)
    dialect = connection.dialect.name
    if dialect not in ("postgresql", "mysql"):
        return
    # MySQL's DROP SCHEMA aliases DROP DATABASE and rejects CASCADE; on MySQL
    # the database drop already cascades to the contained tables.
    cascade = dialect == "postgresql"
    for schema in schemas:
        connection.execute(DropSchema(schema, cascade=cascade, if_exists=True))


def drop_tables(
    connection: sqlalchemy.Connection, config: OrmarConfig
):  # pragma: no cover
    if connection.dialect.name == "postgresql":
        for table in reversed(config.metadata.sorted_tables):
            connection.execute(DropTableCascade(table, if_exists=True))
    else:
        config.metadata.drop_all(connection)


def init_tests(config, scope="module"):
    @pytest_asyncio.fixture(autouse=True, scope=scope)
    async def create_database():

        # Drop and create tables in a single connection to avoid event loop issues
        async with config.engine.begin() as conn:

            def setup_tables(connection):  # pragma: no cover
                validate_cross_schema_constraints(
                    config.metadata, connection.dialect.name
                )
                drop_tables(connection, config)
                drop_schemas(connection, config)
                create_schemas(connection, config)
                config.metadata.create_all(connection)

            await conn.run_sync(setup_tables)

        # For PostgreSQL and MySQL, recreate engine to avoid event loop conflicts
        # asyncpg and aiomysql are strict about event loops
        if config.engine.dialect.name in ("postgresql", "mysql"):  # pragma: no cover
            await config.engine.dispose()
            config._original_engine = config.engine
            config.engine = create_async_engine(ASYNC_DATABASE_URL)

        yield

        # Restore the original engine if it was swapped
        if hasattr(config, "_original_engine"):  # pragma: no cover
            await config.engine.dispose()
            config.engine = config._original_engine
            delattr(config, "_original_engine")

        async with config.engine.begin() as conn:

            def teardown_tables(connection):  # pragma: no cover
                drop_tables(connection, config)
                drop_schemas(connection, config)

            await conn.run_sync(teardown_tables)

        await config.engine.dispose()

    return create_database
