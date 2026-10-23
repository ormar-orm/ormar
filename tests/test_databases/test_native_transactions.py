import pytest
import sqlalchemy as sa

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Entry(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="native_tx_entries")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


create_test_database = init_tests(base_ormar_config)


class Rollback(Exception):
    pass


async def names() -> list:
    return sorted(entry.name for entry in await Entry.objects.all())


async def insert_with_sqlalchemy(name: str) -> None:
    connection = base_ormar_config.database.get_transaction_connection()
    await connection.execute(Entry.ormar_config.table.insert().values(name=name))


@pytest.mark.asyncio
async def test_native_write_first_then_rollback():
    database = base_ormar_config.database
    async with database:
        with pytest.raises(Rollback):
            async with database.transaction():
                await Entry.objects.create(name="native-first")
                await insert_with_sqlalchemy("sa-second")
                assert await names() == ["native-first", "sa-second"]
                raise Rollback()
        assert await names() == []


@pytest.mark.asyncio
async def test_sqlalchemy_write_first_then_rollback():
    database = base_ormar_config.database
    async with database:
        with pytest.raises(Rollback):
            async with database.transaction():
                await insert_with_sqlalchemy("sa-first")
                await Entry.objects.create(name="native-second")
                raise Rollback()
        assert await names() == []


@pytest.mark.asyncio
async def test_savepoint_rollback_keeps_outer_writes():
    database = base_ormar_config.database
    async with database:
        async with database.transaction():
            await Entry.objects.create(name="outer")
            with pytest.raises(Rollback):
                async with database.transaction():
                    await Entry.objects.create(name="inner")
                    raise Rollback()
            assert await names() == ["outer"]
        assert await names() == ["outer"]
        await Entry.objects.delete(each=True)


@pytest.mark.asyncio
async def test_force_rollback_undoes_native_writes():
    database = base_ormar_config.database
    async with database:
        async with database.transaction(force_rollback=True):
            await Entry.objects.create(name="forced")
            assert await names() == ["forced"]
        assert await names() == []


@pytest.mark.asyncio
async def test_iterate_streams_inside_transaction():
    database = base_ormar_config.database
    async with database:
        async with database.transaction(force_rollback=True):
            await Entry.objects.bulk_create(
                [Entry(name=f"row{i:02d}") for i in range(25)]
            )
            streamed = [e.name async for e in Entry.objects.order_by("name").iterate()]
    assert streamed == [f"row{i:02d}" for i in range(25)]


@pytest.mark.asyncio
async def test_standalone_native_write_is_visible_to_other_connection_immediately():
    database = base_ormar_config.database
    async with database:
        await Entry.objects.create(name="autocommitted")
        async with database.engine.connect() as other:
            visible = await other.scalar(
                sa.text(
                    "SELECT COUNT(*) FROM native_tx_entries "
                    "WHERE name = 'autocommitted'"
                )
            )
        await Entry.objects.delete(each=True)
    assert visible == 1
