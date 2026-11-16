import hashlib
from typing import Optional

import pytest

import ormar
from ormar.relations.aliases import ALIAS_DIGEST_SIZE, alias_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()

# "cjzec" and "cqqdq" have the same 5 byte blake2s digest (19915ba8a8), so plain path
# hashes give both relations the same table alias in one statement
COLLIDING_PATHS = ("cjzec", "cqqdq")


def plain_alias(relation_path: str) -> str:
    return (
        "t"
        + hashlib.blake2s(
            relation_path.encode(), digest_size=ALIAS_DIGEST_SIZE
        ).hexdigest()
    )


class Animal(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="collision_animals")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Keeper(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="collision_keepers")

    id: int = ormar.Integer(primary_key=True)
    cjzec: Optional[Animal] = ormar.ForeignKey(Animal, related_name="as_cjzec")
    cqqdq: Optional[Animal] = ormar.ForeignKey(Animal, related_name="as_cqqdq")


create_test_database = init_tests(base_ormar_config)


def test_paths_really_collide_as_plain_hashes():
    assert plain_alias(COLLIDING_PATHS[0]) == plain_alias(COLLIDING_PATHS[1])


def test_colliding_paths_get_distinct_stable_aliases():
    first, second = (alias_for(path) for path in COLLIDING_PATHS)
    assert first != second
    assert plain_alias("cjzec") in (first, second)
    assert [alias_for(path) for path in COLLIDING_PATHS] == [first, second]
    alias_for.cache_clear()
    assert [alias_for(path) for path in COLLIDING_PATHS] == [first, second]
    for alias in (first, second):
        assert len(alias) == 11 and alias.startswith("t")
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
            await Keeper.objects.create(cjzec=cat, cqqdq=dog)
            keeper = await Keeper.objects.select_related(["cjzec", "cqqdq"]).get()
            assert keeper.cjzec.name == "cat"
            assert keeper.cqqdq.name == "dog"
            keeper = (
                await Keeper.objects.select_related(["cqqdq", "cjzec"])
                .filter(cjzec__name="cat", cqqdq__name="dog")
                .order_by(["-cqqdq__name", "cjzec__name"])
                .get()
            )
            assert (keeper.cjzec.name, keeper.cqqdq.name) == ("cat", "dog")
            values = await Keeper.objects.select_related(["cjzec", "cqqdq"]).values(
                ["cjzec__name", "cqqdq__name"]
            )
            assert values == [{"cjzec__name": "cat", "cqqdq__name": "dog"}]
            animal = await Animal.objects.select_related(["as_cjzec", "as_cqqdq"]).get(
                name="dog"
            )
            assert [item.id for item in animal.as_cqqdq] == [keeper.id]
            assert animal.as_cjzec == []
