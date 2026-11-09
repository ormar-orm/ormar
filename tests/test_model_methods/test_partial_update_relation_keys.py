from typing import Any, Optional

import pytest
import pytest_asyncio

import ormar
import ormar.models.model
from ormar.exceptions import ModelPersistenceError
from ormar.queryset.templates import update_template
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()
partial_config = base_ormar_config.copy(partial_updates=True)


class Tenant(ormar.Model):
    ormar_config = partial_config.copy(tablename="prk_tenants")

    id: int = ormar.Integer(primary_key=True, autoincrement=False)


class Member(ormar.Model):
    ormar_config = partial_config.copy(tablename="prk_members")

    tenant_id: int = ormar.Integer(primary_key=True)
    login: str = ormar.String(max_length=20, primary_key=True)


class Team(ormar.Model):
    ormar_config = partial_config.copy(tablename="prk_teams")

    tenant_id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=10, primary_key=True)


class Membership(ormar.Model):
    ormar_config = partial_config.copy(tablename="prk_memberships")

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
    hours: int = ormar.Integer(default=0)


class Assignment(ormar.Model):
    ormar_config = partial_config.copy(tablename="prk_assignments")

    id: int = ormar.Integer(primary_key=True)
    member: Optional[Member] = ormar.ForeignKey(
        Member,
        columns={"tenant_id": "tenant_id", "login": "member_login"},
        related_name="assignments",
    )
    team: Optional[Team] = ormar.ForeignKey(
        Team,
        columns={"tenant_id": "tenant_id", "code": "team_code"},
        related_name="assignments",
    )
    note: Optional[str] = ormar.String(max_length=10, nullable=True)


class Task(ormar.Model):
    ormar_config = partial_config.copy(tablename="prk_tasks")

    id: int = ormar.Integer(primary_key=True)
    tenant_id: int = ormar.Integer()
    title: str = ormar.String(max_length=20)
    assignee: Optional[Member] = ormar.ForeignKey(
        Member,
        columns={"tenant_id": "tenant_id", "login": "assignee_login"},
        related_name="tasks",
    )


class Ticket(ormar.Model):
    ormar_config = partial_config.copy(tablename="prk_tickets")

    id: int = ormar.Integer(primary_key=True)
    tenant: Optional[int] = ormar.Integer(name="tenant_id", nullable=True)
    assignee: Optional[Member] = ormar.ForeignKey(
        Member,
        columns={"tenant_id": "tenant_id", "login": "assignee_login"},
        related_name="tickets",
    )


class Lead(ormar.Model):
    ormar_config = partial_config.copy(tablename="prk_leads")

    id: int = ormar.Integer(primary_key=True)
    tenant: Optional[Tenant] = ormar.ForeignKey(Tenant, name="tenant_id")
    member: Optional[Member] = ormar.ForeignKey(
        Member,
        columns={"tenant_id": "tenant_id", "login": "member_login"},
        related_name="leads",
    )
    note: Optional[str] = ormar.String(max_length=10, nullable=True)


class Shift(ormar.Model):
    ormar_config = partial_config.copy(tablename="prk_shifts")

    id: int = ormar.Integer(primary_key=True)
    membership: Optional[Membership] = ormar.ForeignKey(
        Membership, related_name="shifts"
    )
    hours: int = ormar.Integer(default=0)


create_test_database = init_tests(base_ormar_config)


@pytest.fixture
def written_columns(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Records the column names of every ``Model.update()`` statement."""
    calls: list[list[str]] = []

    def spy(model_cls: Any, values: Any, pk: Any) -> Any:
        calls.append(sorted(values))
        return update_template(model_cls=model_cls, values=values, pk=pk)

    monkeypatch.setattr(ormar.models.model, "update_template", spy)
    return calls


@pytest_asyncio.fixture(autouse=True)
async def data():
    async with base_ormar_config.database:
        for model in (
            Shift,
            Lead,
            Ticket,
            Task,
            Assignment,
            Membership,
            Team,
            Member,
            Tenant,
        ):
            await model.objects.delete(each=True)
        alice = await Member.objects.create(tenant_id=1, login="alice")
        bob = await Member.objects.create(tenant_id=1, login="bob")
        carol = await Member.objects.create(tenant_id=2, login="carol")
        core = await Team.objects.create(tenant_id=1, code="core")
        docs = await Team.objects.create(tenant_id=1, code="docs")
        other = await Team.objects.create(tenant_id=2, code="other")
        yield alice, bob, carol, core, docs, other


@pytest.mark.asyncio
async def test_foreign_keys_sharing_a_column_write_it_once(data, written_columns):
    alice, bob, carol, core, _, other = data
    assignment = await Assignment.objects.create(member=alice, team=core)
    concurrent = await Assignment.objects.get()
    await concurrent.update(note="busy")

    assignment.member = bob
    await assignment.update()
    assert written_columns[-1] == ["member_login", "tenant_id"]
    row = await Assignment.objects.get()
    assert row.member.pk == (1, "bob")
    assert row.team.pk == (1, "core")
    assert row.note == "busy"

    assignment.member = carol
    assignment.team = other
    await assignment.update()
    assert written_columns[-1] == ["member_login", "team_code", "tenant_id"]
    row = await Assignment.objects.get()
    assert row.member.pk == (2, "carol")
    assert row.team.pk == (2, "other")
    assert row.note == "busy"


@pytest.mark.asyncio
async def test_conflicting_shared_column_still_raises(data, written_columns):
    alice, _, carol, core, _, _ = data
    assignment = await Assignment.objects.create(member=alice, team=core)
    assignment.member = carol
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await assignment.update()
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await assignment.update(_columns=["member"])
    assert written_columns == []
    row = await Assignment.objects.get()
    assert row.member.pk == (1, "alice")


@pytest.mark.asyncio
async def test_clearing_one_foreign_key_keeps_the_shared_column(data, written_columns):
    alice, _, _, core, _, _ = data
    assignment = await Assignment.objects.create(member=alice, team=core)
    assignment.member = None
    await assignment.update()
    assert written_columns[-1] == ["member_login", "tenant_id"]
    row = await Assignment.objects.get()
    assert row.member is None
    assert row.team.pk == (1, "core")

    # the same columns as a full update, which also keeps tenant_id
    row.team = None
    await row.update()
    assert written_columns[-1] == ["team_code"]
    row = await Assignment.objects.get()
    assert row.team is None


@pytest.mark.asyncio
async def test_plain_field_sharing_a_column_with_a_foreign_key(data, written_columns):
    alice, bob, carol, _, _, _ = data
    task = await Task.objects.create(tenant_id=1, title="a", assignee=alice)
    concurrent = await Task.objects.get()
    await concurrent.update(title="b")

    task.assignee = bob
    await task.update()
    assert written_columns[-1] == ["assignee_login", "tenant_id"]
    row = await Task.objects.get()
    assert row.assignee.pk == (1, "bob")
    assert row.title == "b"

    task.tenant_id = 2
    task.assignee = carol
    await task.update()
    assert written_columns[-1] == ["assignee_login", "tenant_id"]
    row = await Task.objects.get()
    assert row.tenant_id == 2
    assert row.assignee.pk == (2, "carol")

    task.assignee = None
    await task.update()
    assert written_columns[-1] == ["assignee_login", "tenant_id"]
    row = await Task.objects.get()
    assert row.assignee is None
    assert row.tenant_id == 2
    assert row.title == "b"

    task.assignee = alice
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await task.update()
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await task.update(_columns=["title"], title="c")
    assert (await Task.objects.get()).assignee is None


@pytest.mark.asyncio
async def test_plain_field_assignment_alone_writes_only_its_column(
    data, written_columns
):
    alice, _, _, _, _, _ = data
    task = await Task.objects.create(tenant_id=1, title="a")
    task.tenant_id = 2
    await task.update()
    assert written_columns[-1] == ["tenant_id"]
    assert (await Task.objects.get()).tenant_id == 2


@pytest.mark.asyncio
async def test_foreign_key_fills_a_renamed_plain_field_sharing_its_column(
    data, written_columns
):
    alice, _, carol, _, _, _ = data
    ticket = await Ticket.objects.create()
    ticket.assignee = alice
    await ticket.update()
    assert written_columns[-1] == ["assignee_login", "tenant_id"]
    row = await Ticket.objects.select_related("assignee").get()
    assert row.tenant == 1
    assert row.assignee.pk == (1, "alice")

    row.assignee = carol
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await row.update()


@pytest.mark.asyncio
async def test_multi_column_foreign_key_fills_a_single_column_one(
    data, written_columns
):
    alice, _, carol, _, _, _ = data
    await Tenant.objects.create(id=1)
    lead = await Lead.objects.create(note="x")
    concurrent = await Lead.objects.get()
    await concurrent.update(note="y")

    lead.member = alice
    await lead.update()
    assert written_columns[-1] == ["member_login", "tenant_id"]
    row = await Lead.objects.get()
    assert row.tenant.pk == 1
    assert row.member.pk == (1, "alice")
    assert row.note == "y"

    row.member = carol
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await row.update()


@pytest.mark.asyncio
async def test_key_relation_change_moves_the_row(data, written_columns):
    alice, bob, _, core, docs, _ = data
    membership = await Membership.objects.create(member=alice, team=core)
    concurrent = await Membership.objects.get()
    await concurrent.update(role="dev")

    membership.team = docs
    await membership.update(hours=2)
    assert written_columns[-1] == ["hours", "member_login", "team_code", "tenant_id"]
    rows = await Membership.objects.all()
    assert [(m.pk, m.role, m.hours) for m in rows] == [((1, "alice", "docs"), "dev", 2)]

    membership.member = bob
    await membership.update()
    assert written_columns[-1] == ["member_login", "team_code", "tenant_id"]
    assert [m.pk for m in await Membership.objects.all()] == [(1, "bob", "docs")]

    await membership.update(role="ops")
    assert written_columns[-1] == ["role"]
    rows = await Membership.objects.all()
    assert [(m.pk, m.role, m.hours) for m in rows] == [((1, "bob", "docs"), "ops", 2)]


@pytest.mark.asyncio
async def test_key_relation_change_across_tenants_raises(data, written_columns):
    alice, _, carol, core, _, _ = data
    membership = await Membership.objects.create(member=alice, team=core)
    membership.member = carol
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await membership.update()
    assert written_columns == []
    assert [m.pk for m in await Membership.objects.all()] == [(1, "alice", "core")]


@pytest.mark.asyncio
async def test_foreign_key_to_a_link_table(data, written_columns):
    alice, bob, _, core, _, _ = data
    first = await Membership.objects.create(member=alice, team=core)
    second = await Membership.objects.create(member=bob, team=core)
    shift = await Shift.objects.create(membership=first)
    concurrent = await Shift.objects.get()
    await concurrent.update(hours=4)

    shift.membership = second
    await shift.update()
    assert written_columns[-1] == [
        "membership_member_login",
        "membership_team_code",
        "membership_tenant_id",
    ]
    row = await Shift.objects.get()
    assert row.membership.pk == (1, "bob", "core")
    assert row.hours == 4

    shift.membership = None
    await shift.update()
    assert len(written_columns[-1]) == 3
    row = await Shift.objects.get()
    assert row.membership is None
    assert row.hours == 4


@pytest.mark.asyncio
async def test_bulk_update_with_relation_keys(data):
    alice, bob, carol, core, docs, other = data
    await Membership.objects.create(member=alice, team=core)
    await Assignment.objects.create(member=alice, team=core)
    await Task.objects.create(tenant_id=1, title="a", assignee=alice)

    membership = await Membership.objects.get()
    membership.team = docs
    await Membership.objects.bulk_update([membership])
    assert [m.pk for m in await Membership.objects.all()] == [(1, "alice", "docs")]
    assert membership.__setattr_fields__ == set()

    assignment = await Assignment.objects.get()
    assignment.member = carol
    assignment.team = other
    await Assignment.objects.bulk_update([assignment], columns=["member", "team"])
    row = await Assignment.objects.get()
    assert row.member.pk == (2, "carol")
    assert row.team.pk == (2, "other")

    assignment.member = alice
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Assignment.objects.bulk_update([assignment])

    task = await Task.objects.get()
    task.assignee = bob
    await Task.objects.bulk_update([task], columns=["assignee"])
    row = await Task.objects.get()
    assert row.assignee.pk == (1, "bob")
    assert row.tenant_id == 1
