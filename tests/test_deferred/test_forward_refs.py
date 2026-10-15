# type: ignore
from typing import ForwardRef, Optional

import pytest
import pytest_asyncio
import sqlalchemy as sa

import ormar
from ormar.exceptions import ModelError
from ormar.relations.aliases import alias_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


PersonRef = ForwardRef("Person")


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    supervisor: PersonRef = ormar.ForeignKey(PersonRef, related_name="employees")


Person.update_forward_refs()

GameRef = ForwardRef("Game")
ChildRef = ForwardRef("Child")
ChildFriendRef = ForwardRef("ChildFriend")


class Child(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    favourite_game: GameRef = ormar.ForeignKey(GameRef, related_name="liked_by")
    least_favourite_game: GameRef = ormar.ForeignKey(
        GameRef, related_name="not_liked_by"
    )
    friends = ormar.ManyToMany(
        ChildRef, through=ChildFriendRef, related_name="also_friends"
    )


class ChildFriend(ormar.Model):
    ormar_config = base_ormar_config.copy()


class Game(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


Child.update_forward_refs()


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(scope="function")
async def cleanup():
    yield
    async with base_ormar_config.database:
        await ChildFriend.objects.delete(each=True)
        await Child.objects.delete(each=True)
        await Game.objects.delete(each=True)
        await Person.objects.delete(each=True)


@pytest.mark.asyncio
async def test_not_updated_model_raises_errors():
    Person2Ref = ForwardRef("Person2")

    class Person2(ormar.Model):
        ormar_config = base_ormar_config.copy()

        id: int = ormar.Integer(primary_key=True)
        name: str = ormar.String(max_length=100)
        supervisor: Person2Ref = ormar.ForeignKey(Person2Ref, related_name="employees")

    with pytest.raises(ModelError):
        await Person2.objects.create(name="Test")

    with pytest.raises(ModelError):
        Person2(name="Test")

    with pytest.raises(ModelError):
        await Person2.objects.get()


@pytest.mark.asyncio
async def test_not_updated_model_m2m_raises_errors():
    Person3Ref = ForwardRef("Person3")

    class PersonFriend(ormar.Model):
        ormar_config = base_ormar_config.copy()

    class Person3(ormar.Model):
        ormar_config = base_ormar_config.copy()

        id: int = ormar.Integer(primary_key=True)
        name: str = ormar.String(max_length=100)
        supervisors: Person3Ref = ormar.ManyToMany(
            Person3Ref, through=PersonFriend, related_name="employees"
        )

    with pytest.raises(ModelError):
        await Person3.objects.create(name="Test")

    with pytest.raises(ModelError):
        Person3(name="Test")

    with pytest.raises(ModelError):
        await Person3.objects.get()


@pytest.mark.asyncio
async def test_not_updated_model_m2m_through_raises_errors():
    PersonPetRef = ForwardRef("PersonPet")

    class Pet(ormar.Model):
        ormar_config = base_ormar_config.copy()

        id: int = ormar.Integer(primary_key=True)
        name: str = ormar.String(max_length=100)

    class Person4(ormar.Model):
        ormar_config = base_ormar_config.copy()

        id: int = ormar.Integer(primary_key=True)
        name: str = ormar.String(max_length=100)
        pets: list[Pet] = ormar.ManyToMany(
            Pet, through=PersonPetRef, related_name="owners"
        )

    class PersonPet(ormar.Model):
        ormar_config = base_ormar_config.copy()

    with pytest.raises(ModelError):
        await Person4.objects.create(name="Test")

    with pytest.raises(ModelError):
        Person4(name="Test")

    with pytest.raises(ModelError):
        await Person4.objects.get()


def test_proper_field_init():
    assert "supervisor" in Person.ormar_config.model_fields
    assert Person.ormar_config.model_fields["supervisor"].to == Person

    assert "supervisor" in Person.model_fields
    assert Person.model_fields["supervisor"].annotation == Optional[Person]

    assert "supervisor" in Person.ormar_config.table.columns
    assert isinstance(
        Person.ormar_config.table.columns["supervisor"].type, sa.sql.sqltypes.Integer
    )
    assert len(Person.ormar_config.table.columns["supervisor"].foreign_keys) > 0

    action = Person.objects.filter(supervisor__name="x").filter_clauses[0]
    assert action.target_model == Person
    assert action.table_prefix == alias_for("supervisor")


@pytest.mark.asyncio
async def test_self_relation():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            sam = await Person.objects.create(name="Sam")
            joe = await Person(name="Joe", supervisor=sam).save()
            assert joe.supervisor.name == "Sam"

            joe_check = await Person.objects.select_related("supervisor").get(
                name="Joe"
            )
            assert joe_check.supervisor.name == "Sam"

            sam_check = await Person.objects.select_related("employees").get(name="Sam")
            assert sam_check.name == "Sam"
            assert sam_check.employees[0].name == "Joe"


@pytest.mark.asyncio
async def test_other_forwardref_relation(cleanup):
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            checkers = await Game.objects.create(name="checkers")
            uno = await Game(name="Uno").save()

            await Child(
                name="Billy", favourite_game=uno, least_favourite_game=checkers
            ).save()
            await Child(
                name="Kate", favourite_game=checkers, least_favourite_game=uno
            ).save()

            billy_check = await Child.objects.select_related(
                ["favourite_game", "least_favourite_game"]
            ).get(name="Billy")
            assert billy_check.favourite_game == uno
            assert billy_check.least_favourite_game == checkers

            uno_check = await Game.objects.select_related(
                ["liked_by", "not_liked_by"]
            ).get(name="Uno")
            assert uno_check.liked_by[0].name == "Billy"
            assert uno_check.not_liked_by[0].name == "Kate"


@pytest.mark.asyncio
async def test_m2m_self_forwardref_relation(cleanup):
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            checkers = await Game.objects.create(name="Checkers")
            uno = await Game(name="Uno").save()
            jenga = await Game(name="Jenga").save()

            billy = await Child(
                name="Billy", favourite_game=uno, least_favourite_game=checkers
            ).save()
            kate = await Child(
                name="Kate", favourite_game=checkers, least_favourite_game=uno
            ).save()
            steve = await Child(
                name="Steve", favourite_game=jenga, least_favourite_game=uno
            ).save()

            await billy.friends.add(kate)
            await billy.friends.add(steve)

            billy_check = await Child.objects.select_related(
                [
                    "friends",
                    "favourite_game",
                    "least_favourite_game",
                    "friends__favourite_game",
                    "friends__least_favourite_game",
                ]
            ).get(name="Billy")
            assert len(billy_check.friends) == 2
            assert billy_check.friends[0].name == "Kate"
            assert billy_check.friends[0].favourite_game.name == "Checkers"
            assert billy_check.friends[0].least_favourite_game.name == "Uno"
            assert billy_check.friends[1].name == "Steve"
            assert billy_check.friends[1].favourite_game.name == "Jenga"
            assert billy_check.friends[1].least_favourite_game.name == "Uno"
            assert billy_check.favourite_game.name == "Uno"

            kate_check = await Child.objects.select_related(["also_friends"]).get(
                name="Kate"
            )

            assert len(kate_check.also_friends) == 1
            assert kate_check.also_friends[0].name == "Billy"

            billy_check = (
                await Child.objects.select_related(
                    [
                        "friends",
                        "favourite_game",
                        "least_favourite_game",
                        "friends__favourite_game",
                        "friends__least_favourite_game",
                    ]
                )
                .filter(friends__favourite_game__name="Checkers")
                .get(name="Billy")
            )
            assert len(billy_check.friends) == 1
            assert billy_check.friends[0].name == "Kate"
            assert billy_check.friends[0].favourite_game.name == "Checkers"
            assert billy_check.friends[0].least_favourite_game.name == "Uno"
