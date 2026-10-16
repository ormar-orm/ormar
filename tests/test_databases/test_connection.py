import pytest
from sqlalchemy import event, text

import ormar
from ormar.databases.connection import DatabaseConnection
from tests.lifespan import init_tests
from tests.settings import ASYNC_DATABASE_URL, DATABASE_URL, create_config

base_ormar_config = create_config()


class Team(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="teams")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


create_test_database = init_tests(base_ormar_config)


def test_url_is_replaced_to_async_and_accessible():
    database = DatabaseConnection(ASYNC_DATABASE_URL)
    assert database.url == ASYNC_DATABASE_URL

    expected_drivers = {
        "mysql": "mysql+aiomysql",
        "sqlite": "sqlite+aiosqlite",
        "postgresql": "postgresql+asyncpg",
    }

    dialect = DATABASE_URL.split(":")[0]
    assert dialect in expected_drivers
    assert expected_drivers[dialect] in database.url


@pytest.mark.asyncio
async def test_getting_raw_connection():
    database = DatabaseConnection(ASYNC_DATABASE_URL)
    async with database:
        async with database.connection() as conn:
            result = await conn.execute(text("SELECT 123"))
            async with database.transaction():
                async with database.connection() as conn2:
                    assert conn2 != conn
                async with database.connection() as conn3:
                    assert conn3 == conn2
                    async with database.connection() as conn4:
                        assert conn4 != conn
                        assert conn3 == conn4
            async with database.connection() as conn5:
                assert conn5 != conn4
                assert conn5 != conn
    assert result.fetchone()[0] == 123


@pytest.mark.asyncio
async def test_getting_commit_in_transaction():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True) as tran:
            assert tran._depth == 0
            async with base_ormar_config.database.transaction() as tran2:
                assert tran2._depth == 1
                await Team.objects.create(name="Red Team")
                await Team.objects.create(name="Blue Team")
                async with base_ormar_config.database.transaction() as tran3:
                    assert tran3._depth == 2
                    await Team.objects.create(name="Yellow Team")
                yellow = await Team.objects.get(name="Yellow Team")
                yellow.name = "Green Team"
                await yellow.update()

            teams = await Team.objects.all()
            assert len(teams) == 3
            assert {teams.name for teams in teams} == {
                "Red Team",
                "Blue Team",
                "Green Team",
            }


@pytest.mark.asyncio
async def test_exception_in_transaction_rollbacks():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            async with base_ormar_config.database.transaction():
                await Team.objects.create(name="Red Team")
                await Team.objects.create(name="Blue Team")
                try:
                    async with base_ormar_config.database.transaction() as tran2:
                        assert tran2._depth == 2
                        await Team.objects.create(name="Yellow Team")
                        raise ValueError("test")
                except ValueError:
                    pass

            teams = await Team.objects.all()
            assert len(teams) == 2
            assert {teams.name for teams in teams} == {
                "Red Team",
                "Blue Team",
            }


@pytest.mark.asyncio
async def test_parent_rollback_cascades_to_children():
    """Test that rolling back parent transaction also rolls back child transactions."""
    async with base_ormar_config.database:
        try:
            async with base_ormar_config.database.transaction() as parent:
                assert parent._depth == 0
                await Team.objects.create(name="Parent Team")

                async with base_ormar_config.database.transaction() as child1:
                    assert child1._depth == 1
                    await Team.objects.create(name="Child Team 1")

                async with base_ormar_config.database.transaction() as child2:
                    assert child2._depth == 1
                    await Team.objects.create(name="Child Team 2")

                    async with base_ormar_config.database.transaction() as grandchild:
                        assert grandchild._depth == 2
                        await Team.objects.create(name="Grandchild Team")

                raise ValueError("rollback parent")
        except ValueError:
            pass

        teams = await Team.objects.all()
        assert len(teams) == 0


@pytest.mark.asyncio
async def test_force_rollback_cascades():
    """Test that force_rollback also cascades to children."""
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(
            force_rollback=True
        ) as parent:
            assert parent._depth == 0
            await Team.objects.create(name="Parent Team")

            async with base_ormar_config.database.transaction() as child:
                assert child._depth == 1
                await Team.objects.create(name="Child Team")

        teams = await Team.objects.all()
        assert len(teams) == 0


@pytest.mark.asyncio
async def test_standalone_query_uses_autocommit():
    """Standalone CRUD must not wrap each statement in BEGIN/COMMIT, while
    statements inside ``database.transaction()`` must stay invisible to other
    connections until it commits (see discussion #1629).
    """
    seen_autocommit: list[bool] = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        seen_autocommit.append(conn._is_autocommit_isolation())

    database = base_ormar_config.database
    async with database:
        event.listen(database.engine.sync_engine, "before_cursor_execute", capture)
        try:
            await Team.objects.create(name="Standalone")
            await Team.objects.get(name="Standalone")
        finally:
            event.remove(database.engine.sync_engine, "before_cursor_execute", capture)

        async with database.transaction():
            await Team.objects.create(name="InsideTransaction")
            async with database.engine.connect() as other:
                visible = await other.scalar(
                    text("SELECT COUNT(*) FROM teams WHERE name = 'InsideTransaction'")
                )
        assert await Team.objects.filter(name="InsideTransaction").count() == 1

    assert seen_autocommit == [True, True]
    assert visible == 0


@pytest.mark.asyncio
async def test_bulk_update_is_atomic_under_autocommit():
    """``bulk_update`` must run inside a transaction so that if anything
    after the write raises, the write is rolled back.

    The mock below lets the real ``execute_many`` run first (so the rows
    actually get updated inside the open transaction) and only then raises.
    If the transaction wrapper around ``execute_many`` were removed,
    AUTOCOMMIT would already have persisted the renames and the post-update
    assertion would see ``renamed-*`` instead of the original names.
    """
    async with base_ormar_config.database:
        seeded = [await Team.objects.create(name=f"bulk-{n}") for n in "XYZ"]
        ids = [t.id for t in seeded]
        for idx, team in enumerate(seeded):
            team.name = f"renamed-{idx}"

        from ormar.databases.query_executor import QueryExecutor

        orig = QueryExecutor.execute_many

        async def execute_then_raise(self, query, values):
            await orig(self, query, values)
            raise RuntimeError("boom")

        QueryExecutor.execute_many = execute_then_raise  # type: ignore[assignment]
        try:
            with pytest.raises(RuntimeError, match="boom"):
                await Team.objects.bulk_update(seeded)
        finally:
            QueryExecutor.execute_many = orig  # type: ignore[assignment]

        reloaded = await Team.objects.filter(id__in=ids).all()
        assert {t.name for t in reloaded} == {"bulk-X", "bulk-Y", "bulk-Z"}
