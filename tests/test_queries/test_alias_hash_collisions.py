import hashlib
from typing import Optional

import pytest

import ormar
from ormar.relations.aliases import alias_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()

# "fid" and "fwc" have the same 3 byte blake2s digest (b4b8c6), so plain path
# hashes give both relations the same table alias in one statement
COLLIDING_PATHS = ("fid", "fwc")


def plain_alias(relation_path: str) -> str:
    return "t" + hashlib.blake2s(relation_path.encode(), digest_size=3).hexdigest()


class Animal(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="collision_animals")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Keeper(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="collision_keepers")

    id: int = ormar.Integer(primary_key=True)
    fid: Optional[Animal] = ormar.ForeignKey(Animal, related_name="as_fid")
    fwc: Optional[Animal] = ormar.ForeignKey(Animal, related_name="as_fwc")


create_test_database = init_tests(base_ormar_config)


def test_paths_really_collide_as_plain_hashes():
    assert plain_alias(COLLIDING_PATHS[0]) == plain_alias(COLLIDING_PATHS[1])


def test_colliding_paths_get_distinct_stable_aliases():
    first, second = (alias_for(path) for path in COLLIDING_PATHS)
    assert first != second
    assert plain_alias("fid") in (first, second)
    assert [alias_for(path) for path in COLLIDING_PATHS] == [first, second]
    alias_for.cache_clear()
    assert [alias_for(path) for path in COLLIDING_PATHS] == [first, second]
    for alias in (first, second):
        assert len(alias) == 7 and alias.startswith("t")
        int(alias[1:], 16)


def test_non_colliding_paths_keep_the_plain_hash():
    for path in ("aseller__city", "manufacturer", "manufacturer__hq__nicks"):
        assert alias_for(path) == plain_alias(path)


@pytest.mark.asyncio
async def test_select_related_over_colliding_paths():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            cat = await Animal.objects.create(name="cat")
            dog = await Animal.objects.create(name="dog")
            await Keeper.objects.create(fid=cat, fwc=dog)
            keeper = await Keeper.objects.select_related(["fid", "fwc"]).get()
            assert keeper.fid.name == "cat"
            assert keeper.fwc.name == "dog"
            keeper = (
                await Keeper.objects.select_related(["fwc", "fid"])
                .filter(fid__name="cat", fwc__name="dog")
                .order_by(["-fwc__name", "fid__name"])
                .get()
            )
            assert (keeper.fid.name, keeper.fwc.name) == ("cat", "dog")
            values = await Keeper.objects.select_related(["fid", "fwc"]).values(
                ["fid__name", "fwc__name"]
            )
            assert values == [{"fid__name": "cat", "fwc__name": "dog"}]
            animal = await Animal.objects.select_related(["as_fid", "as_fwc"]).get(
                name="dog"
            )
            assert [item.id for item in animal.as_fwc] == [keeper.id]
            assert animal.as_fid == []
