import pytest
import pytest_asyncio
from sqlalchemy.dialects import mysql, postgresql, sqlite

import ormar
from ormar import F, Greatest, Random
from ormar.queryset.expressions import (
    RandomFloat,
    expression_key,
    expression_params,
)
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Player(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rnd_players")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    score: float = ormar.Float(default=0.0)
    bonus: float = ormar.Float(default=2.0)


create_test_database = init_tests(base_ormar_config)

PLAYER_COUNT = 20


@pytest_asyncio.fixture()
async def players():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Player.objects.bulk_create(
                [Player(name=f"p{number:02}") for number in range(PLAYER_COUNT)]
            )
            yield


async def scores() -> list[float]:
    return await Player.objects.values_list("score", flatten=True)


@pytest.mark.asyncio
async def test_update_with_random(players):
    await Player.objects.update(each=True, score=Random())
    drawn = await scores()
    assert all(0 <= score < 1 for score in drawn)
    assert len(set(drawn)) > 1

    await Player.objects.update(each=True, score=Random() * 10 + F("bonus"))
    assert all(2 <= score < 12 for score in await scores())

    await Player.objects.filter(name="p00").update(score=Greatest(Random(), 0.5))
    assert 0.5 <= (await Player.objects.get(name="p00")).score < 1


@pytest.mark.asyncio
async def test_filter_with_random(players):
    assert await Player.objects.filter(score__lt=Random() + 1).count() == PLAYER_COUNT
    assert await Player.objects.filter(Player.score > Random() + 1).count() == 0


@pytest.mark.asyncio
async def test_order_by_random(players):
    names = await Player.objects.order_by(Random()).values_list("name", flatten=True)
    assert sorted(names) == [f"p{number:02}" for number in range(PLAYER_COUNT)]
    player = await Player.objects.order_by([Random(), "name"]).first()
    assert player.name.startswith("p")


@pytest.mark.asyncio
async def test_update_with_random_reuses_the_template(players):
    cache = template_cache_for(Player)
    cache.clear()
    for _ in range(2):
        await Player.objects.update(each=True, score=Random() * 2)
    assert len(cache.entries) == 1


def test_random_has_no_fields_or_literals():
    assert expression_key(node=Random()) == ("random",)
    assert expression_key(node=Random() * 2) != expression_key(node=F("score") * 2)
    assert expression_params(value=Random() * 2, slot="ormar_v0") == {"ormar_v0_0": 2}
    assert expression_params(value=Random(), slot="ormar_v0") == {}


@pytest.mark.parametrize(
    "dialect, sql",
    [
        (postgresql.dialect(), "random()"),
        (mysql.dialect(), "rand()"),
        (sqlite.dialect(), "(abs(random() % 9007199254740992) / 9007199254740992.0)"),
    ],
)
def test_random_sql_per_dialect(dialect, sql):
    assert str(RandomFloat().compile(dialect=dialect)) == sql
