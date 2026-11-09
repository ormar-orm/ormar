from typing import ForwardRef

import pytest

import ormar
from ormar.exceptions import ModelDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Player(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="clash_players")

    id: int = ormar.Integer(primary_key=True)
    coach = ormar.ForeignKey(ForwardRef("Coach"), related_name="players")


class Coach(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="clash_coaches")

    id: int = ormar.Integer(primary_key=True)
    fans = ormar.ManyToMany(ForwardRef("Player"), related_name="idols")


Player.update_forward_refs()
Coach.update_forward_refs()


create_test_database = init_tests(base_ormar_config)


def test_reverse_m2m_registered_next_to_fk_named_like_owner() -> None:
    assert "idols" in Player.ormar_config.model_fields
    assert "players" in Coach.ormar_config.model_fields
    coach = Coach(id=1)
    player = Player(id=1, idols=[coach])
    assert player.idols[0] == coach


@pytest.mark.asyncio
async def test_reverse_m2m_next_to_fk_named_like_owner_saves() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            coach = await Coach.objects.create(id=1)
            player = await Player.objects.create(id=1, coach=coach)
            await coach.fans.add(player)

            loaded = await Player.objects.select_related("idols").get(id=1)
            assert loaded.idols[0].id == 1
            loaded_coach = await Coach.objects.select_related(["fans", "players"]).get(
                id=1
            )
            assert loaded_coach.fans[0].id == 1
            assert loaded_coach.players[0].id == 1


@pytest.mark.parametrize("coach_first", [True, False])
def test_fk_and_m2m_forward_refs_in_any_order(coach_first: bool) -> None:
    config = create_config()

    class Player(ormar.Model):
        ormar_config = config.copy(tablename="players")

        id: int = ormar.Integer(primary_key=True)
        coach = ormar.ForeignKey(ForwardRef("Coach"), related_name="players")

    class Coach(ormar.Model):
        ormar_config = config.copy(tablename="coaches")

        id: int = ormar.Integer(primary_key=True)
        fans = ormar.ManyToMany(ForwardRef("Player"), related_name="idols")

    models = [Coach, Player] if coach_first else [Player, Coach]
    for model in models:
        model.update_forward_refs(Coach=Coach, Player=Player)

    assert "idols" in Player.ormar_config.model_fields
    assert "players" in Coach.ormar_config.model_fields
    assert Player(id=1, idols=[Coach(id=1)]).idols[0].id == 1


def test_concrete_m2m_to_model_with_unresolved_fk_named_like_owner() -> None:
    config = create_config()

    class Player(ormar.Model):
        ormar_config = config.copy(tablename="players")

        id: int = ormar.Integer(primary_key=True)
        coach = ormar.ForeignKey(ForwardRef("Coach"), related_name="players")

    class Coach(ormar.Model):
        ormar_config = config.copy(tablename="coaches")

        id: int = ormar.Integer(primary_key=True)
        fans = ormar.ManyToMany(Player, related_name="idols")

    Player.update_forward_refs(Coach=Coach)

    assert "idols" in Player.ormar_config.model_fields
    assert "players" in Coach.ormar_config.model_fields


def test_concrete_fk_named_like_owner_of_m2m_forward_ref() -> None:
    config = create_config()

    class Coach(ormar.Model):
        ormar_config = config.copy(tablename="coaches")

        id: int = ormar.Integer(primary_key=True)
        fans = ormar.ManyToMany(ForwardRef("Player"), related_name="idols")

    class Player(ormar.Model):
        ormar_config = config.copy(tablename="players")

        id: int = ormar.Integer(primary_key=True)
        coach = ormar.ForeignKey(Coach, related_name="players")

    Coach.update_forward_refs(Player=Player)

    assert "idols" in Player.ormar_config.model_fields
    assert Player(id=1, idols=[Coach(id=1)]).idols[0].id == 1


def test_concrete_field_named_like_owner_pointing_elsewhere() -> None:
    config = create_config()

    class Trainer(ormar.Model):
        ormar_config = config.copy(tablename="trainers")

        id: int = ormar.Integer(primary_key=True)

    class Player(ormar.Model):
        ormar_config = config.copy(tablename="players")

        id: int = ormar.Integer(primary_key=True)
        coach = ormar.ForeignKey(Trainer, related_name="trainees")

    class Coach(ormar.Model):
        ormar_config = config.copy(tablename="coaches")

        id: int = ormar.Integer(primary_key=True)
        fans = ormar.ManyToMany(Player, related_name="idols")

    assert "idols" in Player.ormar_config.model_fields
    assert "trainees" in Trainer.ormar_config.model_fields


@pytest.mark.parametrize("coach_first", [True, False])
def test_fk_pair_named_like_the_other_model(coach_first: bool) -> None:
    config = create_config()

    class Player(ormar.Model):
        ormar_config = config.copy(tablename="players")

        id: int = ormar.Integer(primary_key=True)
        coach = ormar.ForeignKey(ForwardRef("Coach"), related_name="players")

    class Coach(ormar.Model):
        ormar_config = config.copy(tablename="coaches")

        id: int = ormar.Integer(primary_key=True)
        player = ormar.ForeignKey(ForwardRef("Player"), related_name="coaches")

    models = [Coach, Player] if coach_first else [Player, Coach]
    for model in models:
        model.update_forward_refs(Coach=Coach, Player=Player)

    assert "players" in Coach.ormar_config.model_fields
    assert "coaches" in Player.ormar_config.model_fields
    coach = Coach(id=1)
    player = Player(id=1, coach=coach, coaches=[Coach(id=2)])
    assert player.coaches[0].id == 2
    assert coach.players[0] == player


def test_self_reference_named_like_model() -> None:
    config = create_config()

    class Node(ormar.Model):
        ormar_config = config.copy(tablename="nodes")

        id: int = ormar.Integer(primary_key=True)
        node = ormar.ForeignKey(ForwardRef("Node"), related_name="children")

    Node.update_forward_refs()

    assert "children" in Node.ormar_config.model_fields
    parent = Node(id=1)
    child = Node(id=2, node=parent)
    assert parent.children[0] == child


def test_duplicated_related_name_on_unresolved_field_raises_on_resolution() -> None:
    config = create_config()

    class Team(ormar.Model):
        ormar_config = config.copy(tablename="teams")

        id: int = ormar.Integer(primary_key=True)

    class Player(ormar.Model):
        ormar_config = config.copy(tablename="players")

        id: int = ormar.Integer(primary_key=True)
        idols = ormar.ForeignKey(ForwardRef("Team"), related_name="members")

    class Coach(ormar.Model):
        ormar_config = config.copy(tablename="coaches")

        id: int = ormar.Integer(primary_key=True)
        fans = ormar.ManyToMany(Player, related_name="idols")

    with pytest.raises(ModelDefinitionError, match="already used by model Team"):
        Player.update_forward_refs(Team=Team)


def test_duplicated_related_name_on_resolved_field_raises() -> None:
    config = create_config()

    class Team(ormar.Model):
        ormar_config = config.copy(tablename="teams")

        id: int = ormar.Integer(primary_key=True)

    class Player(ormar.Model):
        ormar_config = config.copy(tablename="players")

        id: int = ormar.Integer(primary_key=True)
        idols = ormar.ForeignKey(Team, related_name="members")

    with pytest.raises(ModelDefinitionError, match="already used by model Team"):

        class Coach(ormar.Model):  # pragma: no cover
            ormar_config = config.copy(tablename="coaches")

            id: int = ormar.Integer(primary_key=True)
            fans = ormar.ManyToMany(Player, related_name="idols")
