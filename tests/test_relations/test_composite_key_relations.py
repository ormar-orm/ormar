from typing import ForwardRef, Optional

import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import (
    ModelDefinitionError,
    ModelPersistenceError,
    QueryDefinitionError,
)
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Member(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ckr_members")

    tenant_id: int = ormar.Integer(primary_key=True)
    login: str = ormar.String(max_length=20, primary_key=True)
    name: Optional[str] = ormar.String(max_length=20, nullable=True)


class Team(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ckr_teams")

    tenant_id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=10, primary_key=True)
    members = ormar.ManyToMany(
        Member, through=ForwardRef("Membership"), related_name="teams"
    )


class Membership(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ckr_memberships")

    tenant_id: int = ormar.Integer(primary_key=True)
    member: Member = ormar.ForeignKey(
        Member,
        primary_key=True,
        columns={"tenant_id": "tenant_id", "login": "member_login"},
        related_name="memberships",
    )
    team: Team = ormar.ForeignKey(
        Team,
        primary_key=True,
        columns={"tenant_id": "tenant_id", "code": "team_code"},
        related_name="memberships",
    )
    role: Optional[str] = ormar.String(max_length=10, nullable=True)


Team.update_forward_refs()


class Assignment(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ckr_assignments")

    member: Member = ormar.ForeignKey(
        Member,
        primary_key=True,
        columns={"tenant_id": "tenant_id", "login": "member_login"},
        related_name="assignments",
    )
    team: Team = ormar.ForeignKey(
        Team,
        primary_key=True,
        columns={"tenant_id": "tenant_id", "code": "team_code"},
        related_name="assignments",
    )
    hours: int = ormar.Integer(default=0)


class Task(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ckr_tasks")

    id: int = ormar.Integer(primary_key=True)
    tenant_id: int = ormar.Integer()
    title: str = ormar.String(max_length=20)
    assignee: Optional[Member] = ormar.ForeignKey(
        Member,
        columns={"tenant_id": "tenant_id", "login": "assignee_login"},
        related_name="tasks",
    )


class Profile(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ckr_profiles")

    member: Member = ormar.ForeignKey(Member, primary_key=True, related_name="profiles")
    bio: Optional[str] = ormar.String(max_length=50, nullable=True)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def data():
    async with base_ormar_config.database:
        for model in (Profile, Task, Assignment, Membership, Team, Member):
            await model.objects.delete(each=True)
        alice = await Member.objects.create(tenant_id=1, login="alice")
        bob = await Member.objects.create(tenant_id=1, login="bob")
        carol = await Member.objects.create(tenant_id=2, login="carol")
        core = await Team.objects.create(tenant_id=1, code="core")
        docs = await Team.objects.create(tenant_id=1, code="docs")
        other = await Team.objects.create(tenant_id=2, code="core")
        yield alice, bob, carol, core, docs, other


def foreign_key_columns(model: type[ormar.Model]) -> set[tuple[str, ...]]:
    return {
        tuple(column.name for column in constraint.columns)
        for constraint in model.ormar_config.table.foreign_key_constraints
    }


def test_link_table_shares_the_tenant_column():
    table = Membership.ormar_config.table
    assert list(table.c.keys()) == ["tenant_id", "member_login", "team_code", "role"]
    assert [column.name for column in table.primary_key.columns] == [
        "tenant_id",
        "member_login",
        "team_code",
    ]
    assert foreign_key_columns(Membership) == {
        ("tenant_id", "member_login"),
        ("tenant_id", "team_code"),
    }
    assert Membership.ormar_config.pk.column_aliases() == (
        "tenant_id",
        "member_login",
        "team_code",
    )
    assignments = Assignment.ormar_config.table
    assert [column.name for column in assignments.primary_key.columns] == [
        "tenant_id",
        "member_login",
        "team_code",
    ]
    assert list(Task.ormar_config.table.c.keys()) == [
        "id",
        "tenant_id",
        "title",
        "assignee_login",
    ]


def test_a_single_foreign_key_of_several_columns_as_the_whole_key():
    key = Profile.ormar_config.pk
    assert key.is_composite
    assert key.column_aliases() == ("member_tenant_id", "member_login")


def test_shared_column_definition_errors():
    with pytest.raises(ModelDefinitionError, match="Column value"):

        class BadPlain(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="ckr_bad_plain")

            id: int = ormar.Integer(primary_key=True)
            first: int = ormar.Integer(name="value")
            second: int = ormar.Integer(name="value")

    with pytest.raises(ModelDefinitionError, match="tenant_id"):

        class BadType(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="ckr_bad_type")

            id: int = ormar.Integer(primary_key=True)
            tenant_id: str = ormar.String(max_length=10)
            member: Optional[Member] = ormar.ForeignKey(
                Member,
                columns={"tenant_id": "tenant_id", "login": "member_login"},
                related_name="bad_types",
            )


def test_relations_to_a_model_keyed_by_a_foreign_key_of_several_columns_raise():
    with pytest.raises(ModelDefinitionError, match="not supported yet"):

        class BadForeignKey(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="ckr_bad_fk")

            id: int = ormar.Integer(primary_key=True)
            membership: Optional[Membership] = ormar.ForeignKey(
                Membership, related_name="bad_fks"
            )

    with pytest.raises(ModelDefinitionError, match="not supported yet"):

        class BadManyToMany(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="ckr_bad_m2m")

            id: int = ormar.Integer(primary_key=True)
            memberships = ormar.ManyToMany(Membership, related_name="bad_m2ms")


@pytest.mark.asyncio
async def test_link_table_crud(data):
    alice, bob, _, core, docs, _ = data
    membership = await Membership.objects.create(member=alice, team=core, role="dev")
    assert membership.pk == (1, "alice", "core")
    assert membership.tenant_id == 1
    await Membership.objects.create(tenant_id=1, member=bob, team=core)

    fetched = await Membership.objects.get(pk=(1, "alice", "core"))
    assert fetched.role == "dev"
    assert fetched.member.pk == (1, "alice")
    assert fetched.team.pk == (1, "core")
    same = await Membership.objects.get(member=alice, team=core)
    assert same.pk == fetched.pk
    by_dict = await Membership.objects.get(
        pk={
            "tenant_id": 1,
            "member": {"tenant_id": 1, "login": "alice"},
            "team": (1, "core"),
        }
    )
    assert by_dict.pk == fetched.pk
    assert Membership.ormar_config.pk.value_from(data={"team": core}) is None

    loaded = Membership(member=alice, team=core)
    await loaded.load()
    assert loaded.role == "dev"

    fetched.role = "lead"
    await fetched.update()
    assert (await Membership.objects.get(pk=(1, "alice", "core"))).role == "lead"
    await Membership.objects.filter(pk=(1, "alice", "core")).update(role="ops")
    assert (await Membership.objects.get(member=alice, team=core)).role == "ops"

    upserted = Membership(member=bob, team=docs, role="new")
    await upserted.upsert()
    assert await Membership.objects.count() == 3

    await fetched.delete()
    assert await Membership.objects.count() == 2
    assert [
        membership.pk for membership in await Membership.objects.order_by("pk").all()
    ] == [(1, "bob", "core"), (1, "bob", "docs")]
    assert [
        membership.pk for membership in await Membership.objects.order_by("-pk").all()
    ] == [(1, "bob", "docs"), (1, "bob", "core")]


@pytest.mark.asyncio
async def test_prefetching_from_and_to_the_link_table(data):
    alice, bob, _, core, docs, _ = data
    await Membership.objects.create(member=alice, team=core, role="dev")
    await Membership.objects.create(member=bob, team=docs)
    rows = (
        await Membership.objects.prefetch_related(["member", "team"])
        .order_by("pk")
        .all()
    )
    assert [(m.member.login, m.team.code) for m in rows] == [
        ("alice", "core"),
        ("bob", "docs"),
    ]
    members = (
        await Member.objects.prefetch_related("memberships")
        .order_by(["tenant_id", "login"])
        .all()
    )
    assert [len(member.memberships) for member in members] == [1, 1, 0]


@pytest.mark.asyncio
async def test_link_table_without_a_plain_tenant_field(data):
    alice, _, _, core, docs, _ = data
    assignment = await Assignment.objects.create(member=alice, team=core, hours=3)
    assert assignment.pk == (1, "alice", "core")
    fetched = await Assignment.objects.select_related(["member", "team"]).get(
        pk=(1, "alice", "core")
    )
    assert fetched.member.login == "alice"
    assert fetched.team.code == "core"
    await Assignment.objects.bulk_create([Assignment(member=alice, team=docs, hours=1)])
    assert await Assignment.objects.filter(member=alice).count() == 2
    assert await Assignment.objects.filter(team__code="docs").count() == 1


@pytest.mark.asyncio
async def test_conflicting_shared_column_values_raise(data):
    alice, _, carol, core, _, other = data
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Membership.objects.create(member=carol, team=core)
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Membership.objects.create(tenant_id=2, member=alice, team=core)
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Assignment.objects.bulk_create([Assignment(member=alice, team=other)])
    await Assignment.objects.create(member=alice, team=core)
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Assignment.objects.filter(member=alice).update(member=carol, team=core)
    assignment = await Assignment.objects.get()
    assignment.team = other
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await assignment.update()
    assert await Assignment.objects.count() == 1


@pytest.mark.asyncio
async def test_nullable_foreign_key_sharing_a_column(data):
    alice, bob, _, _, _, _ = data
    task = await Task.objects.create(tenant_id=1, title="a", assignee=alice)
    assert (await Task.objects.select_related("assignee").get()).assignee.login == (
        "alice"
    )
    task.assignee = None
    await task.update()
    fetched = await Task.objects.get()
    assert fetched.assignee is None
    assert fetched.tenant_id == 1
    fetched.assignee = bob
    await fetched.update()
    assert (await Task.objects.get()).assignee.pk == (1, "bob")
    await Task.objects.filter(id=task.id).update(assignee=None)
    assert (await Task.objects.get()).tenant_id == 1
    unassigned = await Task.objects.create(tenant_id=2, title="b")
    assert unassigned.assignee is None
    assert await Task.objects.filter(assignee__isnull=True).count() == 2
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Task.objects.create(tenant_id=2, title="c", assignee=alice)


@pytest.mark.asyncio
async def test_moving_a_key_relation_updates_the_saved_row(data):
    alice, bob, _, core, docs, _ = data
    membership = await Membership.objects.create(member=alice, team=core)
    membership.team = docs
    await membership.update()
    assert [m.pk for m in await Membership.objects.all()] == [(1, "alice", "docs")]

    moved = await Membership.objects.get()
    moved.member = bob
    await Membership.objects.bulk_update([moved])
    assert [m.pk for m in await Membership.objects.all()] == [(1, "bob", "docs")]

    moved.pk = (1, "alice", "core")
    assert moved.member.pk == (1, "alice")
    assert moved.team.pk == (1, "core")
    await moved.update()
    assert [m.pk for m in await Membership.objects.all()] == [(1, "alice", "core")]


@pytest.mark.asyncio
async def test_many_to_many_through_a_link_table_keyed_by_foreign_keys(data):
    alice, bob, _, core, docs, _ = data
    await core.members.add(alice, role="dev")
    await core.members.add(bob)
    await docs.members.add(alice)
    assert {member.login for member in await core.members.all()} == {"alice", "bob"}
    assert await alice.teams.count() == 2
    assert (await Membership.objects.get(member=alice, team=core)).role == "dev"

    joined = await Team.objects.select_related("members").get(pk=(1, "core"))
    assert {member.login for member in joined.members} == {"alice", "bob"}
    fetched = (
        await Team.objects.prefetch_related("members")
        .order_by(["tenant_id", "code"])
        .all()
    )
    assert [len(team.members) for team in fetched] == [2, 1, 0]
    member = await Member.objects.prefetch_related("teams").get(login="alice")
    assert sorted(team.code for team in member.teams) == ["core", "docs"]
    assert await Team.objects.filter(members__login="bob").count() == 1
    assert await Member.objects.filter(teams__code="docs").count() == 1

    await core.members.remove(bob)
    assert await core.members.count() == 1
    await core.members.clear()
    assert await core.members.count() == 0
    created = await docs.members.create(tenant_id=1, login="dave")
    assert created.pk == (1, "dave")
    assert await Membership.objects.filter(team=docs).count() == 2


@pytest.mark.asyncio
async def test_many_to_many_through_a_link_table_rejects_another_tenant(data):
    _, _, carol, core, _, _ = data
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await core.members.add(carol)
    assert await Membership.objects.count() == 0


@pytest.mark.asyncio
async def test_reverse_relations_of_the_link_table(data):
    alice, _, _, core, docs, _ = data
    await alice.memberships.create(team=core, role="dev")
    created, _ = await Membership.objects.get_or_create(
        member=alice, team=docs, _defaults={"role": "ops"}
    )
    assert created.role == "ops"
    again, was_created = await Membership.objects.get_or_create(member=alice, team=docs)
    assert not was_created
    assert again.pk == (1, "alice", "docs")
    member = await Member.objects.select_related("memberships").get(login="alice")
    assert sorted(m.team.code for m in member.memberships) == ["core", "docs"]
    assert await Member.objects.filter(memberships__role="dev").count() == 1


@pytest.mark.asyncio
async def test_link_table_serialization_and_values(data):
    alice, _, _, core, _, _ = data
    await Membership.objects.create(member=alice, team=core, role="dev")
    fetched = await Membership.objects.get()
    assert fetched.model_dump() == {
        "tenant_id": 1,
        "member": {"tenant_id": 1, "login": "alice"},
        "team": {"tenant_id": 1, "code": "core"},
        "role": "dev",
    }
    assert fetched.model_dump(flatten_fields={"member", "team"}) == {
        "tenant_id": 1,
        "member": {"tenant_id": 1, "login": "alice"},
        "team": {"tenant_id": 1, "code": "core"},
        "role": "dev",
    }
    assert await Membership.objects.values_list(["tenant_id", "role"]) == [(1, "dev")]
    assert await Membership.objects.values(["member", "team"]) == [
        {
            "member": {"tenant_id": 1, "login": "alice"},
            "team": {"tenant_id": 1, "code": "core"},
        }
    ]
    assert await Membership.objects.values_list(["tenant_id", "member"]) == [
        (1, (1, "alice"))
    ]


@pytest.mark.asyncio
async def test_single_foreign_key_of_several_columns_as_the_whole_key(data):
    alice, bob, _, _, _, _ = data
    profile = await Profile.objects.create(member=alice, bio="hi")
    assert profile.pk == (1, "alice")
    await Profile.objects.create(member=bob)
    fetched = await Profile.objects.get(pk=(1, "alice"))
    assert fetched.bio == "hi"
    member = await Member.objects.select_related("profiles").get(login="alice")
    assert member.profiles[0].bio == "hi"
    fetched.bio = "hello"
    await fetched.update()
    assert (await Profile.objects.get(member=alice)).bio == "hello"
    await fetched.delete()
    assert await Profile.objects.count() == 1
    with pytest.raises(QueryDefinitionError):
        await Profile.objects.get(pk=(1, None))
