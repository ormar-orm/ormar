import asyncio
import contextlib
import datetime
import decimal
import enum
import uuid
from typing import Optional

import pytest
import sqlalchemy as sa
from sqlalchemy import event

import ormar
from ormar.databases.connection import DatabaseConnection
from ormar.databases.native.adapters.aiosqlite import AiosqliteAdapter
from ormar.databases.native.adapters.asyncmy import AsyncmyAdapter
from ormar.databases.native.adapters.asyncpg import AsyncpgAdapter
from ormar.databases.query_executor import QueryExecutor
from tests.lifespan import init_tests
from tests.settings import ASYNC_DATABASE_URL, DATABASE_URL, MYSQL_DRIVER, create_config

base_ormar_config = create_config()
IS_SQLITE = DATABASE_URL.startswith("sqlite")
HAS_NATIVE = not (DATABASE_URL.startswith("mysql") and MYSQL_DRIVER != "asyncmy")
EXPECTED_ADAPTERS = {
    "aiosqlite": AiosqliteAdapter,
    "asyncpg": AsyncpgAdapter,
    "asyncmy": AsyncmyAdapter,
}


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Everything(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="native_everything")

    id: int = ormar.Integer(primary_key=True)
    big: int = ormar.BigInteger(default=10**12)
    small: int = ormar.SmallInteger(default=3)
    ratio: float = ormar.Float(default=1.5)
    amount: decimal.Decimal = ormar.Decimal(max_digits=10, decimal_places=2)
    name: str = ormar.String(max_length=100, unique=True)
    body: str = ormar.Text(default="")
    flag: bool = ormar.Boolean(default=True)
    day: datetime.date = ormar.Date()
    moment: datetime.time = ormar.Time()
    stamp: datetime.datetime = ormar.DateTime()
    stamp_tz: datetime.datetime = ormar.DateTime(timezone=True)
    payload: dict = ormar.JSON(default={})
    blob: bytes = ormar.LargeBinary(max_length=100)
    blob64: str = ormar.LargeBinary(max_length=100, represent_as_base64_str=True)
    uid_hex: uuid.UUID = ormar.UUID(uuid_format="hex", default=uuid.uuid4)
    uid_str: uuid.UUID = ormar.UUID(uuid_format="string", default=uuid.uuid4)
    color: Color = ormar.Enum(enum_class=Color)
    secret: str = ormar.String(
        max_length=200,
        encrypt_secret="native-secret",
        encrypt_backend=ormar.EncryptBackends.FERNET,
    )
    maybe_amount: Optional[decimal.Decimal] = ormar.Decimal(
        max_digits=10, decimal_places=2, nullable=True
    )
    maybe_stamp: Optional[datetime.datetime] = ormar.DateTime(nullable=True)
    maybe_payload: Optional[dict] = ormar.JSON(nullable=True)
    maybe_uid: Optional[uuid.UUID] = ormar.UUID(nullable=True)


class Note(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="native_notes")

    id: int = ormar.Integer(primary_key=True)
    text: str = ormar.String(max_length=100)


create_test_database = init_tests(base_ormar_config)


class StatementCapture:  # pragma: no cover
    """``before_cursor_execute`` listener recording SQLAlchemy-executed SQL."""

    def __init__(self):
        self.statements = []

    def __call__(self, conn, cursor, statement, parameters, context, executemany):
        self.statements.append(statement)


@contextlib.contextmanager
def sqlalchemy_path(database: DatabaseConnection):
    backend = database.native_backend
    database.native_backend = None
    try:
        yield
    finally:
        database.native_backend = backend


def make_everything(name: str, **overrides) -> Everything:
    values = dict(
        name=name,
        amount=decimal.Decimal("12.34"),
        day=datetime.date(2026, 10, 19),
        moment=datetime.time(12, 30, 15),
        stamp=datetime.datetime(2026, 10, 19, 12, 30, 15, 123456),
        stamp_tz=datetime.datetime(2026, 10, 19, 12, 30, tzinfo=datetime.timezone.utc),
        payload={"nested": {"list": [1, 2.5, "x", None]}},
        blob=b"\x00\x01binary",
        blob64=b"base64-bytes",
        color=Color.BLUE,
        secret="top secret",
    )
    values.update(overrides)
    return Everything(**values)


async def snapshot() -> dict:
    query = Everything.objects.order_by("id")
    return {
        "all": [m.model_dump() for m in await query.all()],
        "values": await query.values(),
        "values_list": await query.values_list(),
        "get": (await Everything.objects.get(name="native-0")).model_dump(),
        "count": await Everything.objects.count(),
        "exists": await Everything.objects.filter(name="nope").exists(),
        "max": await Everything.objects.max("amount"),
        "min": await Everything.objects.min("stamp"),
        "sum": await Everything.objects.sum("small"),
        "avg": await Everything.objects.avg("small"),
    }


@pytest.mark.asyncio
async def test_native_backend_selected_per_driver():
    database = DatabaseConnection(ASYNC_DATABASE_URL)
    async with database:
        expected = EXPECTED_ADAPTERS.get(database.engine.dialect.driver)
        backend = database.native_backend
        assert (backend.adapter.__class__ if backend else None) is expected
    disabled = DatabaseConnection(ASYNC_DATABASE_URL, native_execution=False)
    async with disabled:
        assert disabled.native_backend is None
    assert database.native_backend is None


@pytest.mark.asyncio
async def test_aiomysql_has_no_native_backend():
    database = DatabaseConnection(
        "mysql+aiomysql://user:pass@localhost/db", min_connections=0
    )
    async with database:
        assert database.native_backend is None


@pytest.mark.asyncio
async def test_results_identical_native_vs_sqlalchemy():
    database = base_ormar_config.database
    async with database:
        async with database.transaction(force_rollback=True):
            native_pk = (await make_everything("native-0").save()).pk
            await make_everything(
                "native-1",
                maybe_amount=decimal.Decimal("0.01"),
                maybe_stamp=datetime.datetime(2020, 1, 1),
                maybe_payload=[1, 2],
                maybe_uid=uuid.UUID(int=7),
                color=Color.RED,
            ).save()
            with sqlalchemy_path(database):
                sa_pk = (await make_everything("sa-0").save()).pk
            assert type(native_pk) is type(sa_pk)
            native = await snapshot()
            with sqlalchemy_path(database):
                reference = await snapshot()
    assert native == reference
    assert native["count"] == 3


@pytest.mark.asyncio
async def test_large_in_list():
    database = base_ormar_config.database
    async with database:
        async with database.transaction(force_rollback=True):
            await Note.objects.bulk_create([Note(text=f"n{i}") for i in range(1000)])
            ids = [note.id for note in await Note.objects.all()]
            native = await Note.objects.filter(id__in=ids).count()
            with sqlalchemy_path(database):
                reference = await Note.objects.filter(id__in=ids).count()
    assert native == reference == 1000


@pytest.mark.asyncio
async def test_every_executor_method_on_both_paths():
    database = base_ormar_config.database
    table = Note.ormar_config.table
    results = []
    async with database:
        for use_native in (True, False):
            context = (
                contextlib.nullcontext() if use_native else sqlalchemy_path(database)
            )
            with context:
                async with database.get_query_executor(transactional=True) as ex:
                    pk = await ex.execute(table.insert().values(text="exec"))
                    await ex.execute_many(
                        table.update()
                        .where(table.c.id == sa.bindparam("new_id"))
                        .values(text=sa.bindparam("new_text")),
                        [{"new_id": pk, "new_text": "updated"}],
                    )
                    query = table.select().where(table.c.id == pk)
                    one = await ex.fetch_one(query)
                    val = await ex.fetch_val(
                        sa.select(table.c.text).where(table.c.id == pk)
                    )
                    rows = await ex.fetch_all(query)
                    streamed = [row async for row in ex.iterate(query)]
                    missing = await ex.fetch_one(table.select().where(table.c.id < 0))
                    deleted = await ex.execute(table.delete().where(table.c.id == pk))
            results.append(
                (
                    dict(one)["text"],
                    val,
                    [dict(r)["text"] for r in rows],
                    [dict(r)["text"] for r in streamed],
                    missing,
                    deleted,
                )
            )
    assert (
        results[0]
        == results[1]
        == (
            "updated",
            "updated",
            ["updated"],
            ["updated"],
            None,
            1,
        )
    )


@pytest.mark.asyncio
@pytest.mark.skipif(not HAS_NATIVE, reason="no native backend for this driver")
async def test_fallback_plan_runs_through_sqlalchemy():  # pragma: no cover
    database = base_ormar_config.database
    table = Note.ormar_config.table
    capture = StatementCapture()
    seen = capture.statements

    async with database:
        query = table.select().where(table.c.text == "fallback")
        bound = database.native_backend.compiler.bind(query)
        event.listen(database.engine.sync_engine, "before_cursor_execute", capture)
        try:
            await Note.objects.filter(text="native").all()
            assert seen == []
            bound.plan.fallback = True
            async with database.get_query_executor() as executor:
                assert await executor.fetch_all(query) == []
        finally:
            bound.plan.fallback = False
            event.remove(database.engine.sync_engine, "before_cursor_execute", capture)
    assert len(seen) == 1


@pytest.mark.asyncio
async def test_integrity_error_through_ormar_api():
    database = base_ormar_config.database
    async with database:
        async with database.transaction(force_rollback=True):
            await make_everything("dup").save()
            for context in (contextlib.nullcontext(), sqlalchemy_path(database)):
                with context:
                    with pytest.raises(sa.exc.IntegrityError):
                        async with database.transaction():
                            await make_everything("dup").save()


@pytest.mark.asyncio
async def test_query_inside_iterate_loop_matches_sqlalchemy():
    database = base_ormar_config.database
    outcomes = []
    async with database:
        async with database.transaction(force_rollback=True):
            await Note.objects.bulk_create([Note(text=f"loop{i}") for i in range(3)])
            for context in (contextlib.nullcontext(), sqlalchemy_path(database)):
                with context:
                    try:
                        texts = []
                        async for note in Note.objects.order_by("id").iterate():
                            texts.append((await Note.objects.get(id=note.id)).text)
                        outcomes.append(texts)
                    except Exception as error:  # pragma: no cover
                        outcomes.append(type(error))
    assert outcomes[0] == outcomes[1]


@pytest.mark.asyncio
@pytest.mark.skipif(IS_SQLITE, reason="SQLite has no server-side sleep to cancel")
async def test_cancelled_query_does_not_poison_pool():  # pragma: no cover
    database = base_ormar_config.database
    sleep = "SELECT pg_sleep(5)" if "postgresql" in DATABASE_URL else "SELECT SLEEP(5)"
    async with database:
        with pytest.raises(asyncio.TimeoutError):
            async with database.get_query_executor() as executor:
                await asyncio.wait_for(executor.fetch_all(sa.text(sleep)), 0.2)
        for _ in range(20):
            assert await Note.objects.count() >= 0


@pytest.mark.asyncio
@pytest.mark.skipif(IS_SQLITE, reason="SQLite has no server-side sleep to cancel")
async def test_cancelled_query_invalidates_connection():  # pragma: no cover
    database = base_ormar_config.database
    sleep = (
        sa.select(sa.func.pg_sleep(5))
        if "postgresql" in DATABASE_URL
        else sa.select(sa.func.sleep(5))
    )
    async with database:
        for backend in (database.native_backend, None):
            async with database._autocommit_engine.connect() as conn:
                executor = QueryExecutor(conn, backend)
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(executor.fetch_all(sleep), 0.2)
                assert conn.invalidated, backend
        assert await Note.objects.count() >= 0


@pytest.mark.asyncio
async def test_query_after_early_exit_from_iterate():
    database = base_ormar_config.database
    table = Note.ormar_config.table
    async with database:
        async with database.transaction(force_rollback=True):
            await Note.objects.bulk_create([Note(text=f"early{i}") for i in range(250)])
            async with database.get_query_executor() as executor:
                stream = executor.iterate(table.select().order_by(table.c.id))
                async with contextlib.aclosing(stream):
                    async for _ in stream:
                        break
                assert (
                    await executor.fetch_val(sa.select(sa.func.count(table.c.id)))
                    == 250
                )
