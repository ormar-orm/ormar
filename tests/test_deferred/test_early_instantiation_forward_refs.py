"""
Models touched before update_forward_refs() resolves a ForwardRef of a related
model fail with a clear error and work normally once the reference is resolved.
"""

import collections
from typing import ForwardRef, Optional

import pydantic
import pytest
import sqlalchemy
from pydantic import PydanticUndefinedAnnotation
from pydantic._internal._mock_val_ser import MockValSer

import ormar
from ormar.exceptions import ModelError
from ormar.models.helpers.rebuild import PENDING_ATTR
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Writer(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="early_writers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Note(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="early_notes")

    id: int = ormar.Integer(primary_key=True)
    writer: Optional[Writer] = ormar.ForeignKey(Writer, related_name="notes")
    letter = ormar.ForeignKey(ForwardRef("Letter"), related_name="notes")


class Team(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="early_teams")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Employee(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="early_employees")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    team: Optional[Team] = ormar.ForeignKey(Team, related_name="employees")
    manager = ormar.ForeignKey(ForwardRef("Employee"), related_name="reports")
    office = ormar.ForeignKey(ForwardRef("Office"), related_name="employees")


class Club(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="early_clubs")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Player(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="early_players")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    club: Optional[Club] = ormar.ForeignKey(Club, related_name="players")
    coach = ormar.ForeignKey(ForwardRef("Coach"), related_name="players")


class Coach(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="early_coaches")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    club: Optional[Club] = ormar.ForeignKey(Club, related_name="coaches")
    favourite = ormar.ForeignKey(ForwardRef("Player"), related_name="fans")
    league = ormar.ForeignKey(ForwardRef("League"), related_name="coaches")


early_errors = {}
for early_model in (Writer, Team, Club, Note, Employee, Player, Coach):
    try:
        early_model(id=1, name="early")
    except (PydanticUndefinedAnnotation, ModelError) as error:
        early_errors[early_model] = type(error)

pending_after_failure = {
    early_model: PENDING_ATTR in early_model.__dict__
    for early_model in (Writer, Team, Club)
}


class Letter(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="early_letters")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)


class Office(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="early_offices")

    id: int = ormar.Integer(primary_key=True)
    city: str = ormar.String(max_length=100)


class League(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="early_leagues")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


Note.update_forward_refs()
Employee.update_forward_refs()
Player.update_forward_refs()
Coach.update_forward_refs()


create_test_database = init_tests(base_ormar_config)


def test_early_instantiation_raises_and_keeps_models_pending() -> None:
    assert early_errors == {
        Writer: PydanticUndefinedAnnotation,
        Team: PydanticUndefinedAnnotation,
        Club: PydanticUndefinedAnnotation,
        Note: ModelError,
        Employee: ModelError,
        Player: ModelError,
        Coach: ModelError,
    }
    assert all(pending_after_failure.values())


def test_model_instantiated_too_early_works_after_resolving() -> None:
    writer = Writer(id=1, name="w", notes=[{"id": 2}])
    assert writer.model_dump() == {"id": 1, "name": "w", "notes": [{"id": 2}]}
    note = Note(id=2, writer=writer, letter={"id": 3, "title": "l"})
    assert note.model_dump() == {
        "id": 2,
        "writer": {"id": 1, "name": "w"},
        "letter": {"id": 3, "title": "l"},
    }


@pytest.mark.asyncio
async def test_resolved_models_create_select_and_dump() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            writer = await Writer.objects.create(name="w")
            letter = await Letter.objects.create(title="l")
            await Note.objects.create(writer=writer, letter=letter)
            loaded = await Writer.objects.select_related("notes__letter").get(
                id=writer.id
            )
            assert loaded.model_dump(exclude={"notes": {"id"}}) == {
                "id": writer.id,
                "name": "w",
                "notes": [{"letter": {"id": letter.id, "title": "l"}}],
            }


@pytest.mark.asyncio
async def test_self_referencing_forward_ref_after_early_instantiation() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            team = await Team.objects.create(name="t")
            boss = await Employee.objects.create(name="boss", team=team)
            await Employee.objects.bulk_create(
                objects=[
                    Employee(name="a", team=team, manager=boss),
                    Employee(name="b", team=team, manager=boss),
                ]
            )
            loaded = await Team.objects.select_related("employees__manager").get(
                id=team.id
            )
            reports = sorted(
                (
                    employee
                    for employee in loaded.employees
                    if employee.manager is not None
                ),
                key=lambda employee: employee.name,
            )
            assert [employee.name for employee in reports] == ["a", "b"]
            assert all(employee.manager.id == boss.id for employee in reports)
            dumped = loaded.model_dump(include={"name": ..., "employees": {"name"}})
            assert dumped["name"] == "t"
            assert sorted(item["name"] for item in dumped["employees"]) == [
                "a",
                "b",
                "boss",
            ]


@pytest.mark.asyncio
async def test_mutually_referencing_forward_refs_after_early_instantiation() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            club = await Club.objects.create(name="c")
            league = await League.objects.create(name="l")
            coach = await Coach.objects.create(name="coach", club=club, league=league)
            await Player.objects.bulk_create(
                objects=[
                    Player(name="p1", club=club, coach=coach),
                    Player(name="p2", club=club, coach=coach),
                ]
            )
            star = await Player.objects.get(name="p1")
            coach.favourite = star
            await coach.update()
            loaded = await Club.objects.select_related(
                ["players__coach", "coaches__favourite", "coaches__league"]
            ).get(id=club.id)
            assert sorted(player.name for player in loaded.players) == ["p1", "p2"]
            assert all(player.coach.id == coach.id for player in loaded.players)
            assert loaded.coaches[0].favourite.id == star.id
            assert loaded.coaches[0].league.name == "l"
            dumped = loaded.model_dump(
                include={"players": {"name"}, "coaches": {"name"}}
            )
            assert dumped["coaches"] == [{"name": "coach"}]
            assert sorted(item["name"] for item in dumped["players"]) == ["p1", "p2"]


@pytest.fixture
def rebuild_counter(monkeypatch: pytest.MonkeyPatch) -> collections.Counter:
    """
    Counts successful pydantic model_rebuild calls per model name.
    """
    counter: collections.Counter = collections.Counter()
    original = pydantic.BaseModel.model_rebuild.__func__  # type: ignore[attr-defined]

    def counting_rebuild(cls, **kwargs):  # type: ignore[no-untyped-def]
        result = original(cls, **kwargs)
        counter[cls.__name__] += 1
        return result

    monkeypatch.setattr(
        pydantic.BaseModel, "model_rebuild", classmethod(counting_rebuild)
    )
    return counter


def test_failed_early_rebuild_is_retried_once(
    rebuild_counter: collections.Counter,
) -> None:
    config = base_ormar_config.copy(metadata=sqlalchemy.MetaData())

    class Shop(ormar.Model):
        ormar_config = config.copy(tablename="shops")

        id: int = ormar.Integer(primary_key=True)

    class Order(ormar.Model):
        ormar_config = config.copy(tablename="orders")

        id: int = ormar.Integer(primary_key=True)
        shop: Optional[Shop] = ormar.ForeignKey(Shop, related_name="orders")
        client = ormar.ForeignKey(ForwardRef("Client"), related_name="orders")

    for _ in range(2):
        with pytest.raises(PydanticUndefinedAnnotation):
            Shop(id=1)
    assert PENDING_ATTR in Shop.__dict__
    assert isinstance(Shop.__pydantic_validator__, MockValSer)
    assert rebuild_counter["Shop"] == 0

    class Client(ormar.Model):
        ormar_config = config.copy(tablename="clients")

        id: int = ormar.Integer(primary_key=True)

    Order.update_forward_refs(Client=Client)
    shop = Shop(id=1, orders=[{"id": 2}])
    Shop(id=2).model_dump()
    order = Order(id=3, shop=shop, client={"id": 4})
    assert order.model_dump() == {"id": 3, "shop": {"id": 1}, "client": {"id": 4}}
    assert Client(id=1, orders=[{"id": 2}]).model_dump() == {
        "id": 1,
        "orders": [{"id": 2}],
    }
    assert shop.model_dump() == {
        "id": 1,
        "orders": [{"id": 2}, {"id": 3, "client": {"id": 4}}],
    }
    assert PENDING_ATTR not in Shop.__dict__
    assert rebuild_counter["Shop"] == 1
    assert all(count == 1 for count in rebuild_counter.values())
