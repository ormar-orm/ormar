from typing import Optional

import pytest
import pytest_asyncio
from sqlalchemy.dialects import mysql, postgresql, sqlite

import ormar
from ormar import F, Func, Inserted
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.select_action import expression_columns
from ormar.queryset.expressions import expression_clause, expression_key
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Team(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="func_teams")

    id: int = ormar.Integer(primary_key=True)
    tag: str = ormar.String(max_length=20)


class Player(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="func_players")

    id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=20, unique=True)
    name: str = ormar.String(max_length=20, default="")
    nick: str = ormar.String(max_length=20, default="")
    score: int = ormar.Integer(default=0)
    team: Optional[Team] = ormar.ForeignKey(Team, nullable=True)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def players():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            team = await Team.objects.create(tag="red")
            await Player.objects.create(
                code="a-1", name="ann", nick="ANN", score=-3, team=team
            )
            await Player.objects.create(code="b-2", name="bob", nick="Bobby", score=8)
            yield


async def column(name: str) -> dict:
    rows = await Player.objects.order_by("code").values_list(["code", name])
    return dict(rows)


def compiled(node: Func, dialect: object) -> str:
    clause = expression_clause(
        node=node,
        columns=expression_columns(node=node, model=Player),
        slot="ormar_v0",
        reference_type=Player.ormar_config.table.c["name"].type,
    )
    return str(clause.compile(dialect=dialect))


@pytest.mark.asyncio
async def test_update_with_func(players):
    await Player.objects.update(each=True, nick=Func("lower", F("nick")))
    assert await column("nick") == {"a-1": "ann", "b-2": "bobby"}
    await Player.objects.update(each=True, name=Func("upper", Player.name))
    assert await column("name") == {"a-1": "ANN", "b-2": "BOB"}
    await Player.objects.update(each=True, score=Func("abs", F("score") - 5) * 2)
    assert await column("score") == {"a-1": 16, "b-2": 6}
    await Player.objects.update(
        each=True, nick=Func("upper", Func("replace", F("code"), "-", "_"))
    )
    assert await column("nick") == {"a-1": "A_1", "b-2": "B_2"}


@pytest.mark.asyncio
async def test_func_of_literals_takes_the_target_type(players):
    for text in ("cy", "dee"):
        await Player.objects.filter(code="a-1").update(name=Func("upper", text))
        assert (await column("name"))["a-1"] == text.upper()
    await Player.objects.filter(code="b-2").update(score=Func("abs", -7))
    assert (await column("score"))["b-2"] == 7


@pytest.mark.asyncio
async def test_filter_with_func(players):
    by_nick = Player.objects.filter(name=Func("lower", F("nick")))
    assert await by_nick.values_list("code", flatten=True) == ["a-1"]
    by_literal = Player.objects.filter(name=Func("lower", "BOB"))
    assert await by_literal.values_list("code", flatten=True) == ["b-2"]
    by_related = Player.objects.filter(
        name=Func("replace", F("team__tag"), "red", "ann")
    )
    assert await by_related.values_list("code", flatten=True) == ["a-1"]
    by_greatest = Player.objects.filter(score=Func("greatest", F("score"), 0))
    assert await by_greatest.values_list("code", flatten=True) == ["b-2"]


@pytest.mark.asyncio
async def test_upsert_update_values_with_func(players):
    for nick in ("Zed", "Kim"):
        await Player.objects.create(
            code="a-1",
            nick=nick,
            on_conflict="update",
            conflict_target=["code"],
            update_values={
                "nick": Func("lower", Inserted("nick")),
                "name": Func("upper", F("name")),
            },
        )
        assert await column("nick") == {"a-1": nick.lower(), "b-2": "Bobby"}
    assert (await column("name"))["a-1"] == "ANN"


@pytest.mark.asyncio
async def test_func_templates_are_cached_by_shape(players):
    cache = template_cache_for(model_cls=Player)
    cache.clear()
    for prefix in ("x", "y"):
        await Player.objects.filter(code="a-1").update(
            nick=Func("replace", F("code"), "a", prefix)
        )
    assert len(cache.entries) == 1
    await Player.objects.filter(code="a-1").update(nick=Func("upper", F("code")))
    assert len(cache.entries) == 2
    assert (await column("nick"))["a-1"] == "A-1"


def test_func_keys_include_the_name():
    assert expression_key(node=Func("lower", F("name"))) == ("lower", (("f", "name"),))
    assert expression_key(node=Func("lower", F("name"))) != expression_key(
        node=Func("upper", F("name"))
    )
    assert expression_key(node=Func("public.lower", F("name"))) == (
        "public.lower",
        (("f", "name"),),
    )
    assert expression_key(node=Func("now")) == ("now", ())


def test_func_compiles_per_dialect():
    unaccent = Func("extensions.unaccent", F("name"))
    assert compiled(node=unaccent, dialect=postgresql.dialect()) == (
        "extensions.unaccent(func_players.name)"
    )
    greatest = Func("greatest", F("score"), 0)
    assert compiled(node=greatest, dialect=sqlite.dialect()) == (
        "max(func_players.score, ?)"
    )
    assert compiled(node=greatest, dialect=mysql.dialect()) == (
        "greatest(func_players.score, %s)"
    )
    assert compiled(node=Func("now"), dialect=postgresql.dialect()) == "now()"
    assert compiled(node=Func("upper", "abc"), dialect=postgresql.dialect()) == (
        "upper(%(ormar_v0_0)s::VARCHAR)"
    )


@pytest.mark.parametrize(
    "name", ["", "1abc", "lower()", "a b", "a..b", "a.", ".a", "a;drop", None]
)
def test_func_rejects_invalid_names(name):
    with pytest.raises(QueryDefinitionError, match="SQL function name"):
        Func(name, F("name"))


def test_func_rejects_none_and_bool_literals():
    with pytest.raises(QueryDefinitionError, match="None or bool"):
        Func("coalesce", F("name"), None)
    with pytest.raises(QueryDefinitionError, match="None or bool"):
        Func("abs", True)
