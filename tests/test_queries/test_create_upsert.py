from typing import Optional, assert_type

import pytest
import sqlalchemy

import ormar
from ormar.exceptions import NoMatch, QueryDefinitionError
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, create_config

base_ormar_config = create_config()


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ups_owners")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Device(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ups_devices")

    id: int = ormar.Integer(primary_key=True)
    endpoint_hash: str = ormar.String(max_length=40, unique=True)
    serial: Optional[str] = ormar.String(max_length=20, nullable=True, unique=True)
    keys: str = ormar.String(max_length=40, name="keys_col")
    label: str = ormar.String(max_length=20, default="new")
    seen: int = ormar.Integer(server_default=sqlalchemy.text("7"))
    revision: int = ormar.Integer(default=0, on_update=1)
    owner: Optional[Owner] = ormar.ForeignKey(
        Owner, nullable=True, related_name="devices"
    )


class Slot(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ups_slots")

    day: int = ormar.Integer(primary_key=True, autoincrement=False)
    hour: int = ormar.Integer(primary_key=True, autoincrement=False)
    taken: int = ormar.Integer(default=0)


create_test_database = init_tests(base_ormar_config)


async def stored(endpoint_hash: str) -> Device:
    return await Device.objects.get(endpoint_hash=endpoint_hash)


@pytest.mark.asyncio
async def test_create_update_inserts_then_updates_the_stored_row() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            created = await Device.objects.create(
                endpoint_hash="h1",
                keys="k1",
                label="phone",
                on_conflict="update",
                conflict_target=["endpoint_hash"],
                update_fields=["keys"],
            )
            assert_type(created, Device)
            assert created.saved and created.pk is not None
            assert (created.keys, created.label, created.seen) == ("k1", "phone", 7)

            updated = await Device.objects.create(
                endpoint_hash="h1",
                keys="k2",
                label="ignored",
                on_conflict="update",
                conflict_target=["endpoint_hash"],
                update_fields=["keys"],
            )
            assert updated.pk == created.pk
            assert (updated.keys, updated.label, updated.seen) == ("k2", "phone", 7)
            assert updated.revision == 1
            assert updated.saved
            reloaded = await stored("h1")
            assert (reloaded.keys, reloaded.label, reloaded.revision) == (
                "k2",
                "phone",
                1,
            )
            assert await Device.objects.count() == 1


@pytest.mark.asyncio
async def test_create_update_defaults_to_all_passed_fields():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            owner = await Owner.objects.create(name="o")
            await Device.objects.create(endpoint_hash="h", keys="k1", label="a")
            updated = await Device.objects.create(
                endpoint_hash="h",
                keys="k2",
                label="b",
                owner=owner,
                on_conflict="update",
                conflict_target=["endpoint_hash"],
            )
            assert (updated.keys, updated.label, updated.owner.pk) == (
                "k2",
                "b",
                owner.pk,
            )
            assert updated.revision == 1
            assert (await stored("h")).label == "b"


@pytest.mark.asyncio
async def test_create_update_on_the_primary_key():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            first = await Slot.objects.create(day=1, hour=9, on_conflict="update")
            assert first.taken == 0
            second = await Slot.objects.create(
                day=1, hour=9, taken=3, on_conflict="update"
            )
            assert second.taken == 3
            device = await Device.objects.create(endpoint_hash="x", keys="k")
            same = await Device.objects.create(
                id=device.pk, endpoint_hash="x", keys="k9", on_conflict="update"
            )
            assert (same.pk, same.keys) == (device.pk, "k9")
            assert await Slot.objects.values_list("taken", flatten=True) == [3]


@pytest.mark.asyncio
async def test_create_update_through_a_relation():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            owner = await Owner.objects.create(name="o")
            for keys in ("k1", "k2"):
                device = await owner.devices.create(
                    endpoint_hash="r",
                    keys=keys,
                    on_conflict="update",
                    conflict_target=["endpoint_hash"],
                )
            assert device.keys == "k2"
            assert [item.keys for item in await owner.devices.all()] == ["k2"]


@pytest.mark.asyncio
async def test_create_update_signals():
    calls: list[str] = []

    @ormar.pre_save(Device)
    async def before(sender, instance, **kwargs):
        calls.append(f"pre:{instance.keys}")

    @ormar.post_save(Device)
    async def after(sender, instance, **kwargs):
        calls.append(f"post:{instance.keys}:{instance.label}")

    try:
        async with base_ormar_config.database:
            async with base_ormar_config.database.transaction(force_rollback=True):
                for keys in ("k1", "k2"):
                    await Device.objects.create(
                        endpoint_hash="s",
                        keys=keys,
                        label=keys,
                        on_conflict="update",
                        conflict_target=["endpoint_hash"],
                        update_fields=["keys"],
                    )
                assert calls == ["pre:k1", "post:k1:k1", "pre:k2", "post:k2:k1"]
    finally:
        Device.ormar_config.signals.pre_save.disconnect(before)
        Device.ormar_config.signals.post_save.disconnect(after)


@pytest.mark.asyncio
async def test_create_update_arguments():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with pytest.raises(QueryDefinitionError, match="need on_conflict"):
                await Device.objects.create(
                    endpoint_hash="a", keys="k", conflict_target=["endpoint_hash"]
                )
            with pytest.raises(QueryDefinitionError, match="need on_conflict"):
                await Device.objects.create(
                    endpoint_hash="a",
                    keys="k",
                    on_conflict="ignore",
                    update_fields=["keys"],
                )
            with pytest.raises(QueryDefinitionError, match=r"missing: \['id'\]"):
                await Device.objects.create(
                    endpoint_hash="a", keys="k", on_conflict="update"
                )
            with pytest.raises(QueryDefinitionError, match=r"missing: \['serial'\]"):
                await Device.objects.create(
                    endpoint_hash="a",
                    keys="k",
                    on_conflict="update",
                    conflict_target=["serial"],
                )
            assert await Device.objects.count() == 0


@pytest.mark.asyncio
@pytest.mark.skipif(
    not DATABASE_URL.startswith("mysql"),
    reason="only MySQL updates a row conflicting on another unique key",
)
async def test_mysql_conflict_on_another_unique_key():  # pragma: no cover
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Device.objects.create(endpoint_hash="a", serial="s1", keys="k")
            with pytest.raises(NoMatch, match="another unique key"):
                await Device.objects.create(
                    endpoint_hash="b",
                    serial="s1",
                    keys="k2",
                    on_conflict="update",
                    conflict_target=["endpoint_hash"],
                )
