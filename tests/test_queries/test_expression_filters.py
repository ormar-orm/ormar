import enum
import uuid
from typing import Optional

import pytest
import pytest_asyncio
import sqlalchemy

import ormar
from ormar import Coalesce, F, Func, Greatest, Least
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, create_config

base_ormar_config = create_config()


class Team(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="exprfilter_teams")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    rank: int = ormar.Integer(default=0)


class Snippet(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="exprfilter_snippets")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    code: str = ormar.String(max_length=20)
    number: int = ormar.Integer(default=0)


class Shade(str, enum.Enum):
    red = "red"
    blue = "blue"


class Paint(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="exprfilter_paints")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    shade: Shade = ormar.Enum(enum_class=Shade)


class Player(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="exprfilter_players")

    id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=20, unique=True)
    name: str = ormar.String(max_length=20)
    nick: Optional[str] = ormar.String(max_length=20, nullable=True)
    score: int = ormar.Integer(default=0)
    bonus: int = ormar.Integer(default=0)
    uid: uuid.UUID = ormar.UUID(default=uuid.uuid4)
    team: Optional[Team] = ormar.ForeignKey(Team, nullable=True, related_name="players")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def players():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            red = await Team.objects.create(name="Red_Team", rank=2)
            blue = await Team.objects.create(name="Blue 100%", rank=5)
            await Player.objects.create(
                code="a-1", name="Ann", nick="ann", score=3, bonus=1, team=red
            )
            await Player.objects.create(
                code="b-2", name="Bob", nick=None, score=8, bonus=5, team=red
            )
            await Player.objects.create(
                code="c-3", name="Cyd", nick="CY", score=5, bonus=0, team=blue
            )
            await Player.objects.create(
                code="d-4", name="Dee_", nick="dee", score=1, bonus=4
            )
            yield


async def codes(*groups: ormar.queryset.clause.FilterGroup) -> list:
    queryset = Player.objects.filter(*groups).order_by("code")
    return await queryset.values_list("code", flatten=True)


@pytest.mark.asyncio
async def test_function_compared_with_a_value(players):
    assert await codes(Func("lower", F("name")) == "ann") == ["a-1"]
    assert await codes(Func("lower", Player.name) == "bob") == ["b-2"]
    assert await codes(Func("lower", F("name")) != "ann") == ["b-2", "c-3", "d-4"]
    player = await Player.objects.get(Func("upper", F("code")) == "C-3")
    assert player.name == "Cyd"


@pytest.mark.asyncio
async def test_arithmetic_and_functions_compared(players):
    assert await codes((F("score") - F("bonus")) > 2) == ["b-2", "c-3"]
    assert await codes((F("score") + 1) <= 4) == ["a-1", "d-4"]
    assert await codes(Greatest(F("score"), F("bonus")) >= 5) == ["b-2", "c-3"]
    assert await codes(Least(F("score"), F("bonus")) < 1) == ["c-3"]
    assert await codes(Coalesce(F("nick"), F("name")) == "Bob") == ["b-2"]
    assert await codes(F("score") == 8) == ["b-2"]
    length = Func("length", F("name"), type_=sqlalchemy.Integer)
    assert await codes(length > 3) == ["d-4"]
    assert await codes(length + F("score") >= 8) == ["b-2", "c-3"]


@pytest.mark.asyncio
async def test_lookups_on_expressions(players):
    lower_name = Func("lower", F("name"))
    assert await codes(lower_name.iexact("DEE_")) == ["d-4"]
    assert await codes(lower_name.iexact("D_E_")) == []
    assert await codes(lower_name.contains("e_")) == ["d-4"]
    assert await codes(lower_name.contains("_")) == ["d-4"]
    assert await codes(lower_name.icontains("NN")) == ["a-1"]
    assert await codes(lower_name.startswith("b")) == ["b-2"]
    assert await codes(lower_name.istartswith("C")) == ["c-3"]
    assert await codes(lower_name.endswith("_")) == ["d-4"]
    assert await codes(lower_name.iendswith("B")) == ["b-2"]
    assert await codes(lower_name.in_(["ann", "cyd"])) == ["a-1", "c-3"]


@pytest.mark.asyncio
async def test_null_checks_on_expressions(players):
    lower_nick = Func("lower", F("nick"))
    assert await codes(lower_nick.isnull(True)) == ["b-2"]
    assert await codes(lower_nick.isnull(False)) == ["a-1", "c-3", "d-4"]
    assert await codes(lower_nick == None) == ["b-2"]  # noqa: E711
    assert await codes(lower_nick != None) == ["a-1", "c-3", "d-4"]  # noqa: E711
    assert await codes(Func("nullif", F("name"), "Bob").isnull(True)) == ["b-2"]


@pytest.mark.asyncio
async def test_expression_compared_with_expression(players):
    assert await codes(Func("lower", F("name")) == Func("lower", F("nick"))) == ["a-1"]
    plain_code = Func("replace", F("code"), "-", "")
    numbered_nick = Func("replace", F("nick"), "ee", "4")
    assert await codes(plain_code == numbered_nick) == ["d-4"]
    assert await codes((F("score") * 2) == F("bonus") + 5) == ["a-1"]
    assert await codes(F("score") > F("team__rank") + 1) == ["b-2"]


@pytest.mark.asyncio
async def test_related_fields_are_joined(players):
    assert await codes(Func("lower", F("team__name")).icontains("red_")) == [
        "a-1",
        "b-2",
    ]
    assert await codes(Func("lower", Player.team.name).startswith("blue")) == ["c-3"]
    by_player = Team.objects.filter(Func("lower", F("players__name")) == "cyd")
    assert await by_player.values_list("name", flatten=True) == ["Blue 100%"]
    by_name = Team.objects.filter(Func("lower", F("name")).contains("100%"))
    assert await by_name.values_list("name", flatten=True) == ["Blue 100%"]
    assert await codes(
        Func("lower", F("team__name")).startswith("red"),
        (F("score") - F("team__rank")) > 1,
    ) == ["b-2"]


@pytest.mark.asyncio
async def test_groups_keep_each_others_joins(players):
    # without the join of the first group the players would be cross joined
    teams = Team.objects.filter(
        F("players__score") > 7, Func("lower", F("name")) == "blue 100%"
    )
    assert await teams.values_list("name", flatten=True) == []
    teams = Team.objects.filter(Team.players.score > 7, Team.name == "Blue 100%")
    assert await teams.values_list("name", flatten=True) == []
    teams = Team.objects.filter(
        Func("lower", F("name")) == "red_team", F("players__score") > 7
    )
    assert await teams.values_list("name", flatten=True) == ["Red_Team"]


@pytest.mark.asyncio
async def test_expression_filters_compose(players):
    lower_name = Func("lower", F("name"))
    assert await codes(ormar.or_(lower_name == "ann", score=5)) == ["a-1", "c-3"]
    assert await codes(ormar.and_(lower_name.startswith("b"), team__rank=2)) == ["b-2"]
    assert await codes(~(lower_name == "ann")) == ["b-2", "c-3", "d-4"]
    assert await codes((Player.score > 7) | (lower_name == "ann")) == ["a-1", "b-2"]
    assert await codes((lower_name != "ann") & (F("score") < 6)) == ["c-3", "d-4"]
    excluded = Player.objects.exclude(lower_name.startswith("b")).order_by("code")
    assert await excluded.values_list("code", flatten=True) == ["a-1", "c-3", "d-4"]
    mixed = Player.objects.filter(lower_name != "ann", code__in=["a-1", "b-2"])
    assert await mixed.values_list("code", flatten=True) == ["b-2"]


@pytest.mark.asyncio
async def test_expression_filters_in_queryset_methods(players):
    lower_name = Func("lower", F("name"))
    assert await Player.objects.filter(lower_name != "ann").count() == 3
    assert await Player.objects.filter(lower_name == "ann").exists()
    assert not await Player.objects.filter(lower_name == "zed").exists()
    first = await Player.objects.order_by("code").first(lower_name != "ann")
    assert first.code == "b-2"
    assert (await Player.objects.get(lower_name.iendswith("YD"))).code == "c-3"
    assert await Player.objects.get_or_none(lower_name == "zed") is None
    assert await Player.objects.filter(lower_name.in_(["ann", "bob"])).max("score") == 8


@pytest.mark.asyncio
async def test_pagination_limits_the_main_model(players):
    teams = (
        Team.objects.select_related("players")
        .filter(Func("lower", F("players__name")).in_(["ann", "bob", "cyd"]))
        .order_by("name")
    )
    first_page = await teams.limit(1).all()
    assert [team.name for team in first_page] == ["Blue 100%"]
    second_page = await teams.limit(1).offset(1).all()
    assert [team.name for team in second_page] == ["Red_Team"]
    assert sorted(player.code for player in second_page[0].players) == ["a-1", "b-2"]
    ranked = Team.objects.select_related("players").filter((F("rank") * 2) > 3)
    first_team = await ranked.order_by("id").limit(1).all()
    assert len(first_team) == 1
    assert sorted(player.code for player in first_team[0].players) == ["a-1", "b-2"]
    assert await ranked.count() == 2
    assert await ranked.exists()
    team = await ranked.order_by("-id").first()
    assert team.name == "Blue 100%"
    assert [player.code for player in team.players] == ["c-3"]
    team = await ranked.get(Func("lower", F("name")).startswith("red"))
    assert len(team.players) == 2


@pytest.mark.asyncio
async def test_update_and_delete_filtered_by_expressions(players):
    lower_name = Func("lower", F("name"))
    assert await Player.objects.filter(lower_name == "ann").update(score=10) == 1
    assert (await Player.objects.get(code="a-1")).score == 10
    updated = await Player.objects.filter(
        Func("lower", F("team__name")).startswith("blue")
    ).update(bonus=F("bonus") + 1)
    assert updated == 1
    assert (await Player.objects.get(code="c-3")).bonus == 1
    assert (
        await Player.objects.filter(
            Func("upper", F("team__name")) == "RED_TEAM"
        ).update(nick="red")
        == 2
    )
    assert await codes(F("nick") == "red") == ["a-1", "b-2"]
    assert await Player.objects.filter((F("score") - F("bonus")) < 0).delete() == 1
    assert (
        await Player.objects.filter(
            Func("upper", F("team__name")) == "BLUE 100%"
        ).delete()
        == 1
    )
    assert await Player.objects.exclude(lower_name.in_(["ann"])).delete() == 1
    assert await codes() == ["a-1"]


@pytest.mark.asyncio
async def test_update_and_delete_excluding_related_paths(players):
    red_team = Func("lower", F("team__name")) == "red_team"
    assert await Player.objects.exclude(red_team).update(bonus=9) == 1
    assert await codes(F("bonus") == 9) == ["c-3"]
    assert await Player.objects.exclude(red_team).delete() == 1
    assert await codes() == ["a-1", "b-2", "d-4"]


@pytest.mark.asyncio
async def test_function_compared_with_value_of_another_type(players):
    player = await Player.objects.get(code="a-1")
    upper_uid = Func("upper", Func("replace", F("uid"), "-", ""))
    assert await codes(upper_uid == player.uid.hex.upper()) == ["a-1"]
    assert await codes(upper_uid.in_([player.uid.hex.upper(), "X"])) == ["a-1"]
    assert await codes(Func("upper", F("uid")) == "X") == []
    uid_or_name = Coalesce(upper_uid, F("name"))
    assert await codes(uid_or_name == player.uid.hex.upper()) == ["a-1"]
    assert await codes(Greatest(upper_uid, F("name")) == "X") == []
    assert await codes(Func("abs", F("score") - 10) * 1 == 2) == ["b-2"]
    assert await codes(F("uid") == player.uid) == ["a-1"]


@pytest.mark.asyncio
async def test_templates_are_cached_by_shape(players):
    cache = template_cache_for(model_cls=Player)
    cache.clear()
    for name in ("ann", "bob"):
        found = await Player.objects.filter(Func("lower", F("name")) == name).all()
        assert [player.name.lower() for player in found] == [name]
    assert len(cache.entries) == 1
    assert await Player.objects.filter(Func("upper", F("name")) == "ANN").all()
    assert len(cache.entries) == 2
    for code, expected in (("A1", ["a-1"]), ("B2", ["b-2"])):
        # literals of both sides share one counter: "-", "" then the code
        same_code = Func("replace", F("code"), "-", "") == Func(
            "lower", Func("replace", F("name"), F("name"), code)
        )
        assert await codes(same_code) == expected


def test_expressions_stay_hashable():
    score = F("score")
    assert {score: 1}[score] == 1
    assert isinstance(score == 1, ormar.queryset.clause.FilterGroup)


@pytest.mark.asyncio
async def test_like_lookups_take_expression_values(players):
    lower_name = Func("lower", F("name"))
    lower_nick = Func("lower", F("nick"))
    assert await codes(lower_name.icontains(F("nick"))) == ["a-1", "c-3", "d-4"]
    assert await codes(lower_name.startswith(lower_nick)) == ["a-1", "c-3", "d-4"]
    assert await codes(lower_name.endswith(lower_nick)) == ["a-1"]
    assert await codes(lower_name.contains(Func("lower", "NN"))) == ["a-1"]
    assert await codes(F("name").iexact(F("nick"))) == ["a-1"]
    assert await codes(F("name").istartswith(F("nick"))) == ["a-1", "c-3", "d-4"]
    assert await codes(F("name").iendswith(Func("upper", "nn"))) == ["a-1"]


@pytest.mark.asyncio
async def test_like_lookups_take_expression_values_in_kwargs(players):
    by_nick = Player.objects.filter(name__istartswith=F("nick")).order_by("code")
    assert await by_nick.values_list("code", flatten=True) == ["a-1", "c-3", "d-4"]
    by_initial = Player.objects.filter(
        team__name__icontains=Func("substr", F("name"), 1, 1)
    )
    assert await by_initial.values_list("code", flatten=True) == ["a-1"]
    assert await codes(Player.name.iexact(Func("lower", "DEE_"))) == ["d-4"]
    teams = Team.objects.filter(
        players__name__icontains=Func("lower", F("players__nick"))
    ).order_by("name")
    assert await teams.values_list("name", flatten=True) == ["Blue 100%", "Red_Team"]


@pytest.mark.asyncio
async def test_like_expression_values_with_pagination(players):
    teams = (
        Team.objects.select_related("players")
        .filter(Func("lower", F("players__name")).icontains(F("players__nick")))
        .order_by("name")
    )
    first_page = await teams.limit(1).all()
    assert [team.name for team in first_page] == ["Blue 100%"]
    assert [player.code for player in first_page[0].players] == ["c-3"]
    second_page = await teams.limit(1).offset(1).all()
    assert [team.name for team in second_page] == ["Red_Team"]
    assert [player.code for player in second_page[0].players] == ["a-1"]
    players_page = (
        await Player.objects.select_related("team")
        .filter(name__istartswith=F("nick"))
        .order_by("code")
        .limit(2)
        .offset(1)
        .all()
    )
    assert [player.code for player in players_page] == ["c-3", "d-4"]
    assert players_page[0].team.name == "Blue 100%"
    assert players_page[1].team is None


async def snippet_names(*groups: ormar.queryset.clause.FilterGroup, **kwargs) -> list:
    queryset = Snippet.objects.filter(*groups, **kwargs).order_by("name")
    return await queryset.values_list("name", flatten=True)


@pytest.mark.asyncio
async def test_expression_values_match_literally(players):
    for name, code in (
        ("xaby", "a_b"),
        ("abc", "a_c"),
        ("xa_cx", "zz"),
        ("1005", "0%"),
        ("100%", "!b"),
        ("a!b", "a\\b"),
        ("ab", "%"),
        ("a\\b", "q_"),
    ):
        await Snippet.objects.create(name=name, code=code)
    without_underscore = Func("replace", F("code"), "_", "")
    assert await snippet_names(name__contains=without_underscore) == ["xaby"]
    assert await snippet_names(F("name").contains(Func("lower", "a_c"))) == ["xa_cx"]
    assert await snippet_names(F("name").contains(Func("lower", "0%"))) == ["100%"]
    assert await snippet_names(name__contains=F("code")) == []
    assert await snippet_names(name__endswith=Func("lower", "!b")) == ["a!b"]
    assert await snippet_names(name__iendswith=Func("lower", "\\b")) == ["a\\b"]
    assert await snippet_names(name__startswith=Func("lower", "%")) == []
    assert await snippet_names(name__istartswith=Func("lower", "_")) == []
    assert await snippet_names(name__iexact=Func("lower", "A_C")) == []
    assert await snippet_names(name__iexact=Func("lower", "XA_CX")) == ["xa_cx"]


@pytest.mark.asyncio
async def test_column_values_match_literally(players):
    for name, code in (
        ("50%", "0%"),
        ("1005", "0%"),
        ("ya_cy", "a_c"),
        ("abc", "a_c"),
        ("p!q\\r", "!q\\"),
        ("pq\\r", "!q\\"),
    ):
        await Snippet.objects.create(name=name, code=code)
    assert await snippet_names(name__contains=F("code")) == ["50%", "p!q\\r", "ya_cy"]


@pytest.mark.asyncio
async def test_like_lookups_take_enum_expression_values(players):
    await Paint.objects.create(name="dark red", shade=Shade.red)
    await Paint.objects.create(name="blue", shade=Shade.blue)
    await Paint.objects.create(name="red", shade=Shade.blue)
    by_shade = Paint.objects.filter(name__contains=F("shade")).order_by("name")
    assert await by_shade.values_list("name", flatten=True) == ["blue", "dark red"]
    by_shade = Paint.objects.filter(name__iexact=F("shade"))
    assert await by_shade.values_list("name", flatten=True) == ["blue"]


@pytest.mark.asyncio
async def test_iexact_expression_value_matches_literally(players):
    await Player.objects.create(code="e-5", name="ab", nick="a%")
    await Player.objects.create(code="f-6", name="A%", nick="a%")
    assert await codes(F("name").iexact(F("nick"))) == ["a-1", "f-6"]


@pytest.mark.asyncio
async def test_like_lookups_take_non_text_expression_values(players):
    await Snippet.objects.create(name="n1205", code="x", number=120)
    await Snippet.objects.create(name="n1300", code="y", number=13)
    assert await snippet_names(name__contains=F("number")) == ["n1205", "n1300"]
    assert await snippet_names(name__endswith=F("number") + 1085) == ["n1205"]
    assert await snippet_names(F("name").istartswith(F("number"))) == []


@pytest.mark.asyncio
async def test_like_wildcards_in_expression_literals_are_escaped(players):
    lower_name = Func("lower", F("name"))
    for literal, expected in (
        ("a_n", []),
        ("ann", ["a-1"]),
        ("%", []),
        ("_", ["d-4"]),
        ("nn", ["a-1"]),
    ):
        assert await codes(lower_name.contains(Func("lower", literal))) == expected
    nick_or_text = Func("lower", Coalesce(F("nick"), "b_"))
    assert await codes(lower_name.startswith(nick_or_text)) == ["a-1", "c-3", "d-4"]
    nick_or_text = Func("lower", Coalesce(F("nick"), "bo"))
    assert await codes(lower_name.startswith(nick_or_text)) == [
        "a-1",
        "b-2",
        "c-3",
        "d-4",
    ]
    assert await codes(F("name").iexact(Func("lower", "a_n"))) == []
    by_name = Team.objects.filter(
        Func("lower", F("name")).endswith(Func("lower", "0%"))
    )
    assert await by_name.values_list("name", flatten=True) == ["Blue 100%"]


@pytest.mark.asyncio
@pytest.mark.skipif(
    not DATABASE_URL.startswith("postgresql"),
    reason="unaccent is a PostgreSQL extension",
)
async def test_unaccent_search(players):  # pragma: no cover
    async with base_ormar_config.database.connection() as connection:
        await connection.execute(
            sqlalchemy.text("CREATE EXTENSION IF NOT EXISTS unaccent")
        )
    await Team.objects.create(name="Kraków")
    town = Func("unaccent", F("name"))
    teams = Team.objects.filter(town.icontains(Func("unaccent", "KRAKOW")))
    assert await teams.values_list("name", flatten=True) == ["Kraków"]
    teams = Team.objects.filter(name__icontains=Func("unaccent", "krakow"))
    assert await teams.values_list("name", flatten=True) == []


@pytest.mark.asyncio
async def test_invalid_expression_filters(players):
    with pytest.raises(QueryDefinitionError, match="in takes a list"):
        Player.objects.filter(Func("lower", F("name")).in_(F("nick")))
    with pytest.raises(QueryDefinitionError, match="isnull a bool"):
        Player.objects.filter(Func("lower", F("name")).isnull(F("nick")))
    with pytest.raises(QueryDefinitionError, match="in takes a list"):
        Player.objects.filter(name__in=Func("lower", F("nick")))
    with pytest.raises(QueryDefinitionError, match="non-relation field"):
        Player.objects.filter(Func("lower", F("team")) == "x")
    with pytest.raises(QueryDefinitionError, match="non-relation field"):
        Player.objects.filter(Func("lower", F("missing")) == "x")
