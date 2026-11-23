from typing import Any, Optional

import pytest
import pytest_asyncio
from sqlalchemy.dialects import mysql

import ormar
from ormar import Case, F, Func, Greatest, Inserted, When
from ormar.databases.dialects import MysqlRules, list_variable_params
from ormar.databases.query_executor import QueryExecutor
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.select_action import resolve_expression
from ormar.queryset.expressions import expression_key
from ormar.queryset.templates import insert_statement, template_cache_for
from ormar.queryset.templates.writes import (
    build_conflict_insert,
    build_update_assignment,
    update_list_keys,
    update_values_params,
)
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, create_config

base_ormar_config = create_config()


class Team(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="case_teams")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    rank: int = ormar.Integer(default=0)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="case_tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Player(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="case_players")

    id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=20, unique=True)
    name: str = ormar.String(max_length=20)
    score: int = ormar.Integer(default=0)
    rank: Optional[int] = ormar.Integer(nullable=True)
    active: bool = ormar.Boolean(default=False)
    label: Optional[str] = ormar.String(max_length=20, nullable=True)
    revision: int = ormar.Integer(default=0, on_update=1)
    team: Optional[Team] = ormar.ForeignKey(Team, nullable=True, related_name="players")
    tags: Optional[list[Tag]] = ormar.ManyToMany(Tag, related_name="players")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def players():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            red = await Team.objects.create(name="red", rank=2)
            blue = await Team.objects.create(name="blue", rank=5)
            await Player.objects.create(code="a-1", name="Ann", score=3, team=red)
            await Player.objects.create(code="b-2", name="Bob", score=8, team=red)
            await Player.objects.create(code="c-3", name="Cyd", score=5, team=blue)
            await Player.objects.create(code="d-4", name="Dee", score=1)
            yield


async def column(name: str) -> dict:
    rows = await Player.objects.order_by("code").values_list(["code", name])
    return dict(rows)


async def codes(*groups: ormar.queryset.clause.FilterGroup, **kwargs: Any) -> list:
    queryset = Player.objects.filter(*groups, **kwargs).order_by("code")
    return await queryset.values_list("code", flatten=True)


def by_score() -> Case:
    return Case(
        When(score__gte=5, then="high"),
        When(score__gte=2, then="mid"),
        default="low",
    )


@pytest.mark.asyncio
async def test_update_with_literal_branches(players):
    await Player.objects.update(each=True, label=by_score())
    assert await column("label") == {
        "a-1": "mid",
        "b-2": "high",
        "c-3": "high",
        "d-4": "low",
    }
    await Player.objects.update(
        each=True, rank=Case(When(code="a-1", then=1), default=2)
    )
    assert await column("rank") == {"a-1": 1, "b-2": 2, "c-3": 2, "d-4": 2}


@pytest.mark.asyncio
async def test_update_without_default_writes_null(players):
    await Player.objects.update(each=True, rank=1)
    await Player.objects.update(
        each=True, rank=Case(When(code__in=["a-1", "c-3"], then=7))
    )
    assert await column("rank") == {"a-1": 7, "b-2": None, "c-3": 7, "d-4": None}


@pytest.mark.asyncio
async def test_update_with_expression_branches(players):
    await Player.objects.update(
        each=True,
        rank=Case(When(score__gt=4, then=F("score") * 2), default=F("score") + 1),
    )
    assert await column("rank") == {"a-1": 4, "b-2": 16, "c-3": 10, "d-4": 2}


@pytest.mark.asyncio
async def test_ranking_by_id_order(players):
    await Player.objects.filter(code="d-4").update(rank=99)
    order = [
        player.id
        for player in await Player.objects.filter(code__in=["c-3", "a-1", "b-2"])
        .order_by("-score")
        .all()
    ]
    await Player.objects.update(
        each=True,
        rank=Case(
            *(When(id=pk, then=rank) for rank, pk in enumerate(order, 1)),
            default=F("rank"),
        ),
    )
    assert await column("rank") == {"a-1": 3, "b-2": 1, "c-3": 2, "d-4": 99}


@pytest.mark.asyncio
async def test_forward_relation_conditions(players):
    await Player.objects.update(
        each=True, rank=Case(When(team=None, then=0), default=F("score"))
    )
    assert await column("rank") == {"a-1": 3, "b-2": 8, "c-3": 5, "d-4": 0}
    await Player.objects.filter(team__name="blue").update(
        rank=Case(When(score__gt=3, then=F("team__rank") + 10), default=-1)
    )
    assert (await column("rank"))["c-3"] == 15
    in_red = Case(When(team__name="red", then=1), default=0)
    assert await codes(active=False, score__gt=in_red) == ["a-1", "b-2", "c-3", "d-4"]
    assert await codes(in_red == 1) == ["a-1", "b-2"]
    found = await Player.objects.select_related("team").get(
        Case(When(team__rank=5, then=F("score")), default=0) == 5
    )
    assert found.code == "c-3"
    assert found.team.name == "blue"


@pytest.mark.asyncio
async def test_case_as_filter_value(players):
    assert await codes(label=None) == ["a-1", "b-2", "c-3", "d-4"]
    await Player.objects.filter(code__in=["b-2", "d-4"]).update(label="high")
    assert await codes(label=by_score()) == ["b-2"]
    small = Case(When(name__startswith="A", then=F("score")), default=4)
    assert await codes(score__gte=small) == ["a-1", "b-2", "c-3"]
    assert await Player.objects.exclude(score=small).count() == 3


@pytest.mark.asyncio
async def test_case_as_filtered_side(players):
    assert await codes(by_score() == "high") == ["b-2", "c-3"]
    assert await codes(by_score().in_(["mid", "low"])) == ["a-1", "d-4"]
    assert await codes(by_score().startswith("hi")) == ["b-2", "c-3"]
    assert await Player.objects.exclude(by_score() == "high").count() == 2
    # both sides hold branches, their conditions are numbered apart
    same = Case(When(score__gte=5, then=1), default=0) == Case(
        When(code="a-1", then=0), When(code="d-4", then=1), default=1
    )
    assert await codes(same) == ["a-1", "b-2", "c-3"]
    grouped = Case(
        When(ormar.or_(Player.name == "Dee", code="a-1"), then=F("score") + 100),
        When(Func("lower", F("name")) == "bob", then=F("score")),
        default=0,
    )
    assert await codes(grouped > 50) == ["a-1", "d-4"]
    assert await codes(grouped == 8) == ["b-2"]


@pytest.mark.asyncio
async def test_bool_and_none_branches(players):
    await Player.objects.update(
        each=True, active=Case(When(score__gte=5, then=True), default=False)
    )
    assert await column("active") == {
        "a-1": False,
        "b-2": True,
        "c-3": True,
        "d-4": False,
    }
    await Player.objects.update(
        each=True, label=Case(When(score__lt=2, then=None), default=F("name"))
    )
    assert await column("label") == {
        "a-1": "Ann",
        "b-2": "Bob",
        "c-3": "Cyd",
        "d-4": None,
    }
    assert await codes(active=Case(When(code="a-1", then=False), default=True)) == [
        "a-1",
        "b-2",
        "c-3",
    ]


@pytest.mark.asyncio
async def test_null_only_cases_are_typed(players):
    nothing = Case(When(code="a-1", then=None))
    await Player.objects.update(each=True, rank=1, label="x")
    await Player.objects.update(each=True, rank=nothing, label=nothing)
    assert await column("rank") == {"a-1": None, "b-2": None, "c-3": None, "d-4": None}
    assert set((await column("label")).values()) == {None}
    everyone = ["a-1", "b-2", "c-3", "d-4"]
    assert await codes(rank=nothing) == []
    assert await codes(nothing == 1) == []
    assert await codes(nothing == F("score")) == []
    assert await codes(nothing.in_([1, 2])) == []
    assert await codes(nothing.in_([])) == []
    assert await codes(nothing.in_([None, 1])) == []
    assert await codes(nothing.startswith("a")) == []
    assert await codes(nothing.isnull(True)) == everyone
    assert await codes(nothing == None) == everyone  # noqa: E711
    with pytest.raises(QueryDefinitionError, match="unknown type"):
        await codes(nothing == Func("now"))


@pytest.mark.asyncio
async def test_nested_and_combined_cases(players):
    nested = Case(
        When(score__gte=5, then=Case(When(code="b-2", then=1), default=2)),
        default=Greatest(Case(When(code="a-1", then=F("score")), default=0), 1),
    )
    await Player.objects.update(each=True, rank=nested + 10)
    assert await column("rank") == {"a-1": 13, "b-2": 11, "c-3": 12, "d-4": 11}
    assert await codes(rank=nested + 10) == ["a-1", "b-2", "c-3", "d-4"]


@pytest.mark.asyncio
async def test_upsert_update_values(players):
    await Player.objects.create(
        code="a-1",
        name="Ann",
        score=6,
        on_conflict="update",
        conflict_target=["code"],
        update_values={
            "rank": Case(
                When(score__lt=Inserted("score"), then=Inserted("score")),
                default=F("score"),
            ),
            "label": Case(When(name="Ann", then="again"), default=None),
        },
    )
    await Player.objects.bulk_create(
        [Player(code="b-2", name="Bob", score=1), Player(code="e-5", name="Eve")],
        on_conflict="update",
        conflict_target=["code"],
        update_values={"rank": Case(When(score__gt=5, then=5), default=0)},
    )
    assert await column("rank") == {
        "a-1": 6,
        "b-2": 5,
        "c-3": None,
        "d-4": None,
        "e-5": None,
    }
    assert (await column("label"))["a-1"] == "again"
    assert (await column("revision"))["a-1"] == 1


@pytest.mark.asyncio
async def test_upsert_conditions_read_the_inserted_row(players):
    newer = Case(
        When(Inserted("score") > F("score"), then=Inserted("name")), default=F("name")
    )
    rows = [
        Player(code="a-1", name="Ann2", score=1),
        Player(code="b-2", name="Bob2", score=9),
        Player(code="c-3", name="Cyd2", score=7),
    ]
    await Player.objects.bulk_create(
        rows,
        on_conflict="update",
        conflict_target=["code"],
        update_values={
            "name": newer,
            "rank": Case(
                When(
                    ormar.or_(
                        Player.code == "a-1",
                        Player.code == "c-3",
                        score__lt=Inserted("score"),
                    ),
                    then=Inserted("score") * 10,
                ),
                default=0,
            ),
        },
    )
    assert await column("name") == {
        "a-1": "Ann",
        "b-2": "Bob2",
        "c-3": "Cyd2",
        "d-4": "Dee",
    }
    assert await column("rank") == {"a-1": 10, "b-2": 90, "c-3": 70, "d-4": None}
    assert await column("revision") == {"a-1": 1, "b-2": 1, "c-3": 1, "d-4": 0}


@pytest.mark.asyncio
async def test_templates_are_cached_per_branch_count(players):
    cache = template_cache_for(model_cls=Player)
    cache.clear()
    for first, second in (("a-1", "b-2"), ("c-3", "d-4")):
        await Player.objects.update(
            each=True,
            rank=Case(When(code=first, then=1), When(code=second, then=2)),
        )
    assert len(cache.entries) == 1
    await Player.objects.update(each=True, rank=Case(When(code="a-1", then=1)))
    assert len(cache.entries) == 2
    for name in ("Ann", "Bob"):
        assert len(await codes(Case(When(name=name, then=1), default=0) == 1)) == 1
    assert len(cache.entries) == 3
    await Player.objects.update(each=True, active=Case(When(code="a-1", then=True)))
    await Player.objects.update(each=True, active=Case(When(code="a-1", then=False)))
    assert len(cache.entries) == 5


def test_keys_cover_conditions_and_values():
    def key_of(node: Case) -> object:
        return expression_key(node=resolve_expression(node=node, model=Player)[0])

    base = key_of(Case(When(code="a", then=1)))
    assert base == key_of(Case(When(code="b", then=2)))
    assert base != key_of(Case(When(name="a", then=1)))
    assert base != key_of(Case(When(code__gt="a", then=1)))
    assert base != key_of(Case(When(code="a", then="x")))
    assert base != key_of(Case(When(code="a", then=1), default=0))
    assert key_of(Case(When(code="a", then=True))) != key_of(
        Case(When(code="a", then=False))
    )
    assert key_of(Case(When(code="a", then=F("score")))) != key_of(
        Case(When(code="a", then=F("rank")))
    )


@pytest.mark.asyncio
async def test_expressions_are_not_changed_by_queries(players):
    shared = Case(When(code="a-1", then=F("score")), default=0)
    assert await codes(shared == 3) == ["a-1"]
    await Player.objects.update(each=True, rank=shared)
    when = shared._whens[0]
    assert when.actions == []
    assert when.condition.actions == []
    assert await codes(rank=shared) == ["a-1", "b-2", "c-3", "d-4"]


@pytest.mark.asyncio
async def test_invalid_cases(players):
    with pytest.raises(QueryDefinitionError, match="one or more When"):
        Case()
    with pytest.raises(QueryDefinitionError, match="one or more When"):
        Case(When(code="a", then=1), 2)  # type: ignore[arg-type]
    with pytest.raises(QueryDefinitionError, match="needs a condition"):
        When(then=1)
    with pytest.raises(QueryDefinitionError, match="through foreign keys"):
        await Team.objects.update(
            each=True, rank=Case(When(players__score=1, then=1), default=0)
        )
    with pytest.raises(QueryDefinitionError, match="through foreign keys"):
        Player.objects.filter(rank=Case(When(tags__name="x", then=1)))
    with pytest.raises(QueryDefinitionError, match="through foreign keys"):
        Player.objects.filter(Case(When(team__players__score=1, then=1)) == 1)
    with pytest.raises(QueryDefinitionError, match="read only fields of the updated"):
        await Player.objects.update(
            each=True, rank=Case(When(team__name="red", then=1), default=0)
        )
    with pytest.raises(QueryDefinitionError, match="only allowed in update_values"):
        Player.objects.filter(rank=Case(When(score=Inserted("score"), then=1)))
    with pytest.raises(QueryDefinitionError, match="only allowed in update_values"):
        await Player.objects.update(
            each=True, rank=Case(When(Inserted("score") > 1, then=1))
        )
    with pytest.raises(QueryDefinitionError, match="read no columns set"):
        await Player.objects.update(
            each=True, score=1, rank=Case(When(score=1, then=1))
        )
    with pytest.raises(QueryDefinitionError, match="only fields of the inserted"):
        await Player.objects.create(
            code="a-1",
            name="Ann",
            on_conflict="update",
            conflict_target=["code"],
            update_values={"rank": Case(When(team__name="red", then=1))},
        )
    with pytest.raises(QueryDefinitionError, match="read no fields set"):
        await Player.objects.create(
            code="a-1",
            name="Ann",
            on_conflict="update",
            conflict_target=["code"],
            update_values={"rank": Case(When(score=1, then=1)), "score": 2},
        )


@pytest.mark.parametrize(
    "condition, sql_condition, values",
    [
        (
            When(score__in=[1, 8], then=1),
            "case_players.score IN (@ormar_u0_w0_0_0, @ormar_u0_w0_0_1)",
            {"ormar_u0_w0_0_0": 1, "ormar_u0_w0_0_1": 8},
        ),
        (
            When(Inserted("score").in_([1, 2, 3]), then=1),
            "VALUES(score) IN (@ormar_u0_w0_0_0_0, @ormar_u0_w0_0_0_1, "
            "@ormar_u0_w0_0_0_2)",
            {"ormar_u0_w0_0_0_0": 1, "ormar_u0_w0_0_0_1": 2, "ormar_u0_w0_0_0_2": 3},
        ),
        (
            When(~F("score").in_([4]), then=1),
            "NOT (case_players.score IN (@ormar_u0_w0_0_0_0))",
            {"ormar_u0_w0_0_0_0": 4},
        ),
        (When(score__in=[], then=1), "WHEN (false)", {}),
        (When(~ormar.and_(score__in=[]), then=1), "WHEN NOT (false)", {}),
    ],
)
def test_mysql_batches_read_in_lists_from_one_variable_per_item(
    condition, sql_condition, values
):
    update_values = Player._conflict_updates(
        update_fields=None, update_values={"rank": Case(condition, default=0)}
    )[1]
    statement: Any = build_conflict_insert(
        table=Player.ormar_config.table,
        names=("code", "name"),
        rules=MysqlRules(backend_name="mysql"),
        on_conflict="update",
        target=("code",),
        update_names=(),
        on_update=("revision",),
        returning=(),
        model=Player,
        update_values=update_values,
        update_variables=True,
    )
    assert sql_condition in str(statement.compile(dialect=mysql.dialect()))
    assignment = build_update_assignment(
        model=Player, on_update=("revision",), update_values=update_values
    )
    assert str(assignment.compile(dialect=mysql.dialect())) == ", ".join(
        [
            "SET @ormar_on_update_0 = %s, @ormar_u0_0 = %s, @ormar_u0_1 = %s",
            *(f"@{name} = %s" for name in values),
        ]
    )
    list_keys = update_list_keys(model=Player, update_values=update_values)
    params = list_variable_params(
        params=update_values_params(update_values=update_values), list_keys=list_keys
    )
    assert params == {"ormar_u0_0": 1, "ormar_u0_1": 0, **values}


def test_upserts_reading_in_lists_from_variables_are_cached_per_list_length():
    def statement(list_lengths: tuple[int, ...]) -> Any:
        update_values = Player._conflict_updates(
            update_fields=None,
            update_values={"rank": Case(When(score__in=[1, 8], then=1), default=0)},
        )[1]
        return insert_statement(
            model_cls=Player,
            names=("code", "name"),
            on_conflict="update",
            conflict_target=["code"],
            update_values=update_values,
            update_variables=True,
            list_lengths=list_lengths,
        )

    assert statement(list_lengths=(2,)) is statement(list_lengths=(2,))
    assert statement(list_lengths=(2,)) is not statement(list_lengths=(3,))


@pytest.fixture()
def executed(monkeypatch):
    calls: list[str] = []
    for method_name in ("execute", "execute_many"):
        original = getattr(QueryExecutor, method_name)

        def recording(self, *args, _original=original, _name=method_name, **kwargs):
            calls.append(_name)
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(QueryExecutor, method_name, recording)
    return calls


async def bulk_upsert_rank(condition: Any, executed: list[str]) -> dict:
    """
    Upsert three existing players in one batch, setting the rank to the
    inserted score where the condition holds and to -2 elsewhere; MySQL sets
    the bound values, the in lists among them, in user variables first.
    """
    executed.clear()
    await Player.objects.bulk_create(
        [
            Player(code="a-1", name="Ann", score=20),
            Player(code="b-2", name="Bob", score=30),
            Player(code="d-4", name="Dee", score=40),
        ],
        on_conflict="update",
        conflict_target=["code"],
        update_values={
            "rank": Case(When(condition, then=Inserted("score")), default=-2)
        },
    )
    assert executed.count("execute_many") == 1
    assert executed.count("execute") == (1 if DATABASE_URL.startswith("mysql") else 0)
    return await column("rank")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "condition, ranks",
    [
        (ormar.and_(score__in=[1, 8]), {"a-1": -2, "b-2": 30, "d-4": 40}),
        (ormar.and_(score__in=[3, 5, 8]), {"a-1": 20, "b-2": 30, "d-4": -2}),
        (Inserted("score").in_([20, 40]), {"a-1": 20, "b-2": -2, "d-4": 40}),
        (ormar.and_(score__in=[]), {"a-1": -2, "b-2": -2, "d-4": -2}),
        (~F("score").in_([1, 8]), {"a-1": 20, "b-2": -2, "d-4": -2}),
        (~ormar.and_(score__in=[3]), {"a-1": -2, "b-2": 30, "d-4": 40}),
        (~ormar.and_(score__in=[]), {"a-1": 20, "b-2": 30, "d-4": 40}),
    ],
)
async def test_bulk_upsert_takes_in_conditions(players, executed, condition, ranks):
    assert await bulk_upsert_rank(condition=condition, executed=executed) == {
        **ranks,
        "c-3": None,
    }


@pytest.mark.asyncio
async def test_bulk_upsert_takes_in_lists_of_any_length_in_turn(players, executed):
    for scores, ranks in [
        ([1, 8], {"a-1": -2, "b-2": 30, "d-4": 40}),
        ([3, 5, 8], {"a-1": 20, "b-2": 30, "d-4": -2}),
        ([3], {"a-1": 20, "b-2": -2, "d-4": -2}),
        ([], {"a-1": -2, "b-2": -2, "d-4": -2}),
        ([8, 1], {"a-1": -2, "b-2": 30, "d-4": 40}),
        ([3, 1, 8, 5], {"a-1": 20, "b-2": 30, "d-4": 40}),
    ]:
        condition = ormar.and_(score__in=scores)
        assert await bulk_upsert_rank(condition=condition, executed=executed) == {
            **ranks,
            "c-3": None,
        }


@pytest.mark.asyncio
async def test_bulk_upsert_takes_the_suggested_equalities(players):
    rows = [
        Player(code="a-1", name="Ann", score=20),
        Player(code="b-2", name="Bob", score=30),
        Player(code="d-4", name="Dee", score=40),
    ]
    await Player.objects.bulk_create(
        rows,
        on_conflict="update",
        conflict_target=["code"],
        update_values={
            "rank": Case(
                When((Player.score == 1) | (Player.score == 8), then=Inserted("score")),
                default=-2,
            )
        },
    )
    assert await column("rank") == {"a-1": -2, "b-2": 30, "c-3": None, "d-4": 40}
    await Player.objects.bulk_create(
        rows,
        on_conflict="update",
        conflict_target=["code"],
        update_values={
            "label": Case(
                When(
                    ormar.or_(ormar.and_(score=1), ormar.and_(score=8)),
                    then="or",
                ),
                When(
                    (Inserted("score") == 20) | (Inserted("score") == 40),
                    then="inserted",
                ),
                default="no",
            )
        },
    )
    assert await column("label") == {
        "a-1": "inserted",
        "b-2": "or",
        "c-3": None,
        "d-4": "or",
    }
