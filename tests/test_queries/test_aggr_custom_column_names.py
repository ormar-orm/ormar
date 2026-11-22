from typing import Optional

import pytest
import pytest_asyncio

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class League(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggr_leagues")

    id: int = ormar.Integer(primary_key=True)
    level: int = ormar.Integer(name="league_level")


class Player(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggr_players")

    id: int = ormar.Integer(primary_key=True)
    score: int = ormar.Integer(name="user_score")
    league: Optional[League] = ormar.ForeignKey(League, name="league_ref")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True, scope="function")
async def cleanup():
    yield
    async with base_ormar_config.database:
        await Player.objects.delete(each=True)
        await League.objects.delete(each=True)


async def sample_data():
    first_league = await League(level=2).save()
    second_league = await League(level=8).save()
    await Player(score=3, league=first_league).save()
    await Player(score=9, league=first_league).save()
    await Player(score=6, league=second_league).save()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [("max", 9), ("min", 3), ("sum", 18), ("avg", 6)],
)
async def test_aggregate_own_field_with_custom_column_name(func_name, expected):
    async with base_ormar_config.database:
        await sample_data()
        aggregate = getattr(Player.objects, func_name)
        assert float(await aggregate("score")) == expected
        result = await getattr(Player.objects, func_name)(["score", "id"])
        assert float(result["score"]) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected_forward, expected_reverse",
    [("max", 8, 9), ("min", 2, 3), ("sum", 12, 18), ("avg", 4, 6)],
)
async def test_aggregate_related_field_with_custom_column_name(
    func_name, expected_forward, expected_reverse
):
    async with base_ormar_config.database:
        await sample_data()
        forward_queryset = Player.objects.select_related("league")
        forward_result = await getattr(forward_queryset, func_name)("league__level")
        assert float(forward_result) == expected_forward

        reverse_queryset = League.objects.select_related("players")
        reverse_result = await getattr(reverse_queryset, func_name)("players__score")
        assert float(reverse_result) == expected_reverse

        filtered_queryset = forward_queryset.filter(league__level__gt=4)
        assert float(await getattr(filtered_queryset, func_name)("score")) == 6
