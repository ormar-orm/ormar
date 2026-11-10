"""``QuerySet.select_for_update()`` row locks (issue #781).

The config has no ``force_rollback``: the cross-connection tests need each
transaction on its own pooled connection, started from outside any
transaction so the gathered tasks do not inherit a shared one.
"""

import asyncio
from typing import Optional

import pytest
import sqlalchemy
from sqlalchemy.dialects import mysql, postgresql, sqlite

import ormar
from ormar import Prefetch
from ormar.exceptions import QueryDefinitionError
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, create_config

lock_config = create_config()

cross_connection = pytest.mark.skipif(
    DATABASE_URL.startswith("sqlite"), reason="SQLite has no row locks"
)


class LockAuthor(ormar.Model):
    ormar_config = lock_config.copy(tablename="lock_authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class LockJob(ormar.Model):
    ormar_config = lock_config.copy(tablename="lock_jobs")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    author: Optional[LockAuthor] = ormar.ForeignKey(LockAuthor, related_name="jobs")


create_test_database = init_tests(lock_config)


async def create_jobs() -> list[LockJob]:
    author = await LockAuthor.objects.create(name="a")
    return [
        await LockJob.objects.create(name=name, author=author)
        for name in ("j1", "j2", "j3")
    ]


async def cleanup() -> None:
    await LockJob.objects.delete(each=True)
    await LockAuthor.objects.delete(each=True)


def compiled(query: sqlalchemy.sql.Select, dialect: sqlalchemy.Dialect) -> str:
    return str(query.compile(dialect=dialect))


def mysql8_dialect() -> sqlalchemy.Dialect:
    dialect = mysql.dialect()
    dialect.supports_for_update_of = True
    dialect.use_mysql_for_share = True
    return dialect


async def hold_lock(
    job_id: int, locked: asyncio.Event, release: asyncio.Event
) -> None:  # pragma: no cover
    async with lock_config.database.transaction():
        await LockJob.objects.select_for_update().get(id=job_id)
        locked.set()
        await release.wait()


@cross_connection
@pytest.mark.asyncio
async def test_nowait_raises_on_locked_row():  # pragma: no cover
    async with lock_config.database:
        try:
            jobs = await create_jobs()
            locked, release = asyncio.Event(), asyncio.Event()

            async def probe() -> None:
                await locked.wait()
                try:
                    with pytest.raises(sqlalchemy.exc.DBAPIError):
                        async with lock_config.database.transaction():
                            await LockJob.objects.select_for_update(nowait=True).get(
                                id=jobs[0].id
                            )
                finally:
                    release.set()

            await asyncio.gather(hold_lock(jobs[0].id, locked, release), probe())
        finally:
            await cleanup()


@cross_connection
@pytest.mark.asyncio
async def test_skip_locked_returns_unlocked_rows():  # pragma: no cover
    async with lock_config.database:
        try:
            jobs = await create_jobs()
            locked, release = asyncio.Event(), asyncio.Event()
            found: dict[str, list] = {}

            async def probe() -> None:
                await locked.wait()
                try:
                    async with lock_config.database.transaction():
                        queue = LockJob.objects.select_for_update(
                            skip_locked=True
                        ).order_by("id")
                        found["all"] = await queue.all()
                        found["one"] = await queue.limit(1).all()
                finally:
                    release.set()

            await asyncio.gather(hold_lock(jobs[0].id, locked, release), probe())
            assert [job.id for job in found["all"]] == [jobs[1].id, jobs[2].id]
            assert [job.id for job in found["one"]] == [jobs[1].id]
        finally:
            await cleanup()


@pytest.mark.asyncio
async def test_lock_with_joins_limit_and_prefetch():
    async with lock_config.database:
        try:
            jobs = await create_jobs()
            async with lock_config.database.transaction():
                locked = (
                    await LockJob.objects.select_for_update()
                    .select_related("author")
                    .prefetch_related("author__jobs")
                    .order_by("id")
                    .limit(2)
                    .all()
                )
                assert [job.id for job in locked] == [jobs[0].id, jobs[1].id]
                assert locked[0].author.name == "a"
                assert len(locked[0].author.jobs) == 3
                filtered = await LockJob.objects.select_for_update(read=True).get(
                    author__name="a", id=jobs[2].id
                )
                assert filtered.id == jobs[2].id
                values = await LockJob.objects.select_for_update().values_list(
                    "id", flatten=True
                )
                assert sorted(values) == [job.id for job in jobs]
        finally:
            await cleanup()


@cross_connection
@pytest.mark.asyncio
async def test_joined_lock_locks_main_rows():  # pragma: no cover
    async with lock_config.database:
        try:
            jobs = await create_jobs()
            locked, release = asyncio.Event(), asyncio.Event()

            async def holder() -> None:
                async with lock_config.database.transaction():
                    await (
                        LockJob.objects.select_for_update()
                        .select_related("author")
                        .order_by("id")
                        .limit(1)
                        .all()
                    )
                    locked.set()
                    await release.wait()

            async def probe() -> None:
                await locked.wait()
                try:
                    with pytest.raises(sqlalchemy.exc.DBAPIError):
                        async with lock_config.database.transaction():
                            await LockJob.objects.select_for_update(nowait=True).get(
                                id=jobs[0].id
                            )
                    async with lock_config.database.transaction():
                        free = await LockJob.objects.select_for_update(nowait=True).get(
                            id=jobs[1].id
                        )
                        assert free.id == jobs[1].id
                finally:
                    release.set()

            await asyncio.gather(holder(), probe())
        finally:
            await cleanup()


@pytest.mark.asyncio
async def test_lock_guards():
    async with lock_config.database:
        locking = LockJob.objects.select_for_update()
        with pytest.raises(QueryDefinitionError, match="transaction"):
            await locking.get(id=1)
        author = await LockAuthor.objects.create(name="guarded")
        async with lock_config.database.transaction():
            assert await locking.all() == []
            for read in (locking.count(), locking.exists(), locking.max("id")):
                with pytest.raises(QueryDefinitionError, match="get_or_none"):
                    await read
            with pytest.raises(QueryDefinitionError, match="select_for_update"):
                await LockAuthor.objects.prefetch_related(
                    Prefetch("jobs", queryset=locking)
                ).all()
        cached = LockJob.objects.select_for_update()
        with pytest.raises(QueryDefinitionError, match="transaction"):
            await cached.all()
        with pytest.raises(QueryDefinitionError, match="transaction"):
            async for _ in cached.iterate():
                pass  # pragma: no cover
        with pytest.raises(QueryDefinitionError, match="nowait"):
            LockJob.objects.select_for_update(nowait=True, skip_locked=True)
        await author.delete()


@pytest.mark.asyncio
async def test_lock_sql_shapes():
    async with lock_config.database:
        plain = LockJob.objects.select_for_update().build_select_expression()
        assert compiled(plain, postgresql.dialect()).endswith("FOR UPDATE OF lock_jobs")
        assert compiled(plain, mysql8_dialect()).endswith("FOR UPDATE OF lock_jobs")
        assert "FOR" not in compiled(plain, sqlite.dialect())

        joined = (
            LockJob.objects.select_for_update(nowait=True)
            .select_related("author")
            .limit(2)
            .build_select_expression()
        )
        sql = compiled(joined, postgresql.dialect())
        assert sql.endswith("FOR UPDATE OF lock_jobs NOWAIT")
        assert sql.count("FOR UPDATE") == 1

        shapes = {
            "FOR SHARE OF lock_jobs SKIP LOCKED": {"read": True, "skip_locked": True},
            "FOR NO KEY UPDATE OF lock_jobs": {"no_key": True},
            "FOR KEY SHARE OF lock_jobs": {"read": True, "no_key": True},
        }
        for suffix, options in shapes.items():
            query = LockJob.objects.select_for_update(
                **options
            ).build_select_expression()
            assert compiled(query, postgresql.dialect()).endswith(suffix)
        shared = LockJob.objects.select_for_update(read=True).build_select_expression()
        assert compiled(shared, mysql8_dialect()).endswith("FOR SHARE OF lock_jobs")


@pytest.mark.asyncio
async def test_lock_cache_keys():
    async with lock_config.database:

        def key(queryset: ormar.QuerySet) -> tuple:
            return queryset.select_cache_key(
                variant=(), limit=None, offset=None, order_bys=None
            )

        plain = key(LockJob.objects)
        locked = key(LockJob.objects.select_for_update())
        assert plain != locked
        assert locked != key(LockJob.objects.select_for_update(nowait=True))
        assert locked == key(LockJob.objects.select_for_update())
        assert key(LockJob.objects.select_for_update().filter(name="x")) == key(
            LockJob.objects.filter(name="x").select_for_update()
        )
