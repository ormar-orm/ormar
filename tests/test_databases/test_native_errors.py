import pytest
import pytest_asyncio
import sqlalchemy as sa

from ormar.databases.native.adapters import select_adapter
from ormar.databases.native.backend import NativeBackend
from ormar.databases.native.compiler import NativeCompiler
from ormar.databases.native.errors import raise_translated
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, MYSQL_DRIVER, create_config

pytestmark = pytest.mark.skipif(
    DATABASE_URL.startswith("mysql") and MYSQL_DRIVER != "asyncmy",
    reason="native execution is not available for this driver",
)

base_ormar_config = create_config()
parents = sa.Table(
    "native_error_parents",
    base_ormar_config.metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String(50), nullable=False, unique=True),
)
children = sa.Table(
    "native_error_children",
    base_ormar_config.metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("parent_id", sa.Integer, sa.ForeignKey("native_error_parents.id")),
)
create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def backend():
    async with base_ormar_config.database as database:
        dialect = database.engine.dialect
        yield NativeBackend(dialect, select_adapter(dialect), NativeCompiler(dialect))


async def native_error(backend, conn, stmt):
    bound = backend.compiler.bind(stmt)
    raw = await conn.get_raw_connection()
    try:
        await backend.adapter.execute(raw, bound)
    except backend.adapter.driver_errors as error:
        try:
            await raise_translated(
                error,
                backend=backend,
                sa_connection=conn,
                sql=bound.sql,
                args=bound.args,
            )
        except sa.exc.DBAPIError as translated:
            return translated
    raise AssertionError("statement did not fail")  # pragma: no cover


async def sqlalchemy_error(conn, stmt):
    try:
        await conn.execute(stmt)
    except sa.exc.DBAPIError as error:
        return error
    raise AssertionError("statement did not fail")  # pragma: no cover


async def assert_same_error(backend, stmt, expected_cls):
    database = base_ormar_config.database
    async with database._autocommit_engine.connect() as conn:
        native = await native_error(backend, conn, stmt)
    async with database._autocommit_engine.connect() as conn:
        reference = await sqlalchemy_error(conn, stmt)
    assert isinstance(native, expected_cls)
    assert type(native) is type(reference)
    assert type(native.orig) is type(reference.orig)
    assert native.connection_invalidated is False


@pytest.mark.asyncio
async def test_unique_violation_raises_sqlalchemy_integrity_error(backend):
    async with base_ormar_config.database._autocommit_engine.connect() as conn:
        await conn.execute(parents.insert().values(id=1, name="taken"))
    await assert_same_error(
        backend, parents.insert().values(id=2, name="taken"), sa.exc.IntegrityError
    )


@pytest.mark.asyncio
async def test_not_null_violation_raises_integrity_error(backend):
    await assert_same_error(
        backend,
        parents.insert().values(id=3, name=None),
        (sa.exc.IntegrityError, sa.exc.OperationalError),
    )


@pytest.mark.asyncio
async def test_fk_violation_raises_integrity_error(backend):
    await assert_same_error(
        backend, children.insert().values(id=1, parent_id=999), sa.exc.IntegrityError
    )


@pytest.mark.asyncio
async def test_bad_sql_raises_same_class_as_sqlalchemy(backend):
    await assert_same_error(
        backend, sa.text("SELEC broken FROM nowhere"), sa.exc.DBAPIError
    )


@pytest.mark.asyncio
@pytest.mark.skipif(
    DATABASE_URL.startswith("sqlite"), reason="SQLite connections cannot be killed"
)
async def test_disconnect_invalidates_connection(backend):  # pragma: no cover
    database = base_ormar_config.database
    is_pg = DATABASE_URL.startswith("postgresql")
    own_id = sa.text("SELECT pg_backend_pid()" if is_pg else "SELECT CONNECTION_ID()")
    async with database._autocommit_engine.connect() as conn:
        connection_id = (await conn.execute(own_id)).scalar()
        async with database._autocommit_engine.connect() as killer:
            kill = (
                f"SELECT pg_terminate_backend({connection_id})"
                if is_pg
                else f"KILL {connection_id}"
            )
            await killer.execute(sa.text(kill))
        bound = backend.compiler.bind(sa.select(sa.literal(1)))
        raw = await conn.get_raw_connection()
        with pytest.raises(sa.exc.DBAPIError) as caught:
            try:
                await backend.adapter.fetch_all(raw, bound)
            except backend.adapter.driver_errors as error:
                await raise_translated(
                    error,
                    backend=backend,
                    sa_connection=conn,
                    sql=bound.sql,
                    args=bound.args,
                )
        assert caught.value.connection_invalidated is True
        assert conn.invalidated is True
    async with database._autocommit_engine.connect() as fresh:
        assert (await fresh.execute(sa.text("SELECT 1"))).scalar() == 1
