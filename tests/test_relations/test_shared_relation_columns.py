import itertools
from typing import ForwardRef, Optional

import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import ModelPersistenceError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Tenant(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="src_tenants")

    id: int = ormar.Integer(primary_key=True, autoincrement=False)


class Member(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="src_members")

    tenant_id: int = ormar.Integer(primary_key=True)
    login: str = ormar.String(max_length=20, primary_key=True)


class Team(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="src_teams")

    tenant_id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=10, primary_key=True)


class Seat(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="src_seats")

    tenant: Tenant = ormar.ForeignKey(
        Tenant, name="tenant_id", primary_key=True, related_name="seats"
    )
    member: Member = ormar.ForeignKey(
        Member,
        primary_key=True,
        columns={"tenant_id": "tenant_id", "login": "member_login"},
        related_name="seats",
    )
    team: Team = ormar.ForeignKey(
        Team,
        primary_key=True,
        columns={"tenant_id": "tenant_id", "code": "team_code"},
        related_name="seats",
    )
    note: Optional[str] = ormar.String(max_length=10, nullable=True)


class Badge(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="src_badges")

    tenant: int = ormar.Integer(primary_key=True, name="tenant_id")
    member: Member = ormar.ForeignKey(
        Member,
        primary_key=True,
        columns={"tenant_id": "tenant_id", "login": "member_login"},
        related_name="badges",
    )


NodeRef = ForwardRef("Node")


class Node(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="src_nodes")

    tenant_id: int = ormar.Integer(primary_key=True)
    id: int = ormar.Integer(primary_key=True, autoincrement=False)
    name: str = ormar.String(max_length=10)
    parent: Optional[NodeRef] = ormar.ForeignKey(  # type: ignore[valid-type]
        NodeRef,
        columns={"tenant_id": "tenant_id", "id": "parent_id"},
        related_name="children",
    )


Node.update_forward_refs()


create_test_database = init_tests(base_ormar_config)

SEAT_RELATIONS = ("tenant", "member", "team")


class OrderedNames(set):
    """A set of names iterated in a given order."""

    def __init__(self, names: list[str]) -> None:
        super().__init__(names)
        self.names = names

    def __iter__(self):
        return iter(self.names)


@pytest_asyncio.fixture(autouse=True)
async def data():
    async with base_ormar_config.database:
        for model in (Node, Badge, Seat, Team, Member, Tenant):
            await model.objects.delete(each=True)
        first = await Tenant.objects.create(id=1)
        second = await Tenant.objects.create(id=2)
        alice = await Member.objects.create(tenant_id=1, login="alice")
        core = await Team.objects.create(tenant_id=1, code="core")
        yield first, second, alice, core


@pytest.fixture(params=list(itertools.permutations(SEAT_RELATIONS)), ids="-".join)
def relation_order(request, monkeypatch):
    """Processes the foreign keys of Seat in every order a set could yield."""
    order = request.param
    related = Seat.extract_related_names()
    db_related = Seat._extract_db_related_names()

    def ordered(names: set[str]) -> "OrderedNames":
        rest = sorted(names - set(order))
        return OrderedNames([name for name in order if name in names] + rest)

    monkeypatch.setattr(
        Seat, "extract_related_names", classmethod(lambda cls: ordered(related))
    )
    monkeypatch.setattr(
        Seat, "_extract_db_related_names", classmethod(lambda cls: ordered(db_related))
    )
    return order


@pytest.mark.asyncio
async def test_single_column_foreign_key_shares_a_column(data, relation_order):
    first, second, alice, core = data
    seat = await Seat.objects.create(tenant=first, member=alice, team=core)
    assert seat.pk == (1, "alice", "core")
    fetched = await Seat.objects.select_related(["tenant", "member", "team"]).get()
    assert fetched.tenant.id == 1
    assert fetched.member.pk == (1, "alice")
    assert fetched.team.pk == (1, "core")

    fetched.tenant = second
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await fetched.update()
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Seat.objects.bulk_update([fetched])
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Seat.objects.create(tenant=second, member=alice, team=core)
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Seat.objects.filter(member=alice).update(tenant=second, member=alice)
    await Seat.objects.filter(member=alice).update(tenant=first, member=alice)
    assert [seat.pk for seat in await Seat.objects.all()] == [(1, "alice", "core")]


@pytest.mark.asyncio
async def test_single_column_foreign_key_shares_a_column_in_bulk(data, relation_order):
    first, second, alice, core = data
    await Seat.objects.bulk_create([Seat(tenant=first, member=alice, team=core)])
    assert await Seat.objects.count() == 1
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Seat.objects.bulk_create(
            [Seat(tenant={"id": 2}, member=alice, team=core)]
        )
    seat = Seat(tenant={"id": 1}, member=alice, team=(1, "core"), note="x")
    await seat.upsert()
    assert (await Seat.objects.get()).note == "x"


def test_merging_ignores_the_order_of_foreign_keys(relation_order):
    model_dict = {
        "tenant": {"id": 2},
        "member": {"tenant_id": 1, "login": "alice"},
        "team": {"tenant_id": 1, "code": "core"},
    }
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        Seat.substitute_models_with_pks(model_dict=model_dict)
    model_dict = {
        "tenant": {"id": 1},
        "member": {"tenant_id": 1, "login": "alice"},
        "team": {"tenant_id": 1, "code": "core"},
    }
    assert Seat.substitute_models_with_pks(model_dict=model_dict) == {
        "tenant": 1,
        "member_login": "alice",
        "team_code": "core",
    }


@pytest.mark.asyncio
async def test_plain_field_renamed_with_name_shares_the_column(data):
    _, _, alice, _ = data
    badge = await Badge.objects.create(member=alice)
    assert badge.tenant == 1
    assert badge.pk == (1, "alice")
    fetched = await Badge.objects.select_related("member").get()
    assert fetched.tenant == 1
    assert fetched.member.login == "alice"
    assert await Badge.objects.filter(member=alice).count() == 1
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Badge.objects.create(tenant=2, member=alice)


@pytest.mark.asyncio
async def test_self_referential_foreign_key_of_several_columns(data):
    root = await Node.objects.create(tenant_id=1, id=1, name="root")
    await Node.objects.create(tenant_id=1, id=2, name="kid", parent=root)
    kid = await Node.objects.select_related("parent").get(id=2)
    assert kid.parent.pk == (1, 1)
    assert kid.parent.name == "root"
    parent = await Node.objects.select_related("children").get(id=1)
    assert [child.pk for child in parent.children] == [(1, 2)]
    parent = await Node.objects.prefetch_related("children").get(id=1)
    assert [child.pk for child in parent.children] == [(1, 2)]
    with pytest.raises(ModelPersistenceError, match="tenant_id"):
        await Node.objects.create(tenant_id=2, id=3, name="x", parent=root)
    kid.parent = None
    await kid.update()
    kid = await Node.objects.get(id=2)
    assert kid.parent is None
    assert kid.tenant_id == 1


def test_alias_translation_builds_its_cache():
    Seat.reset_column_caches()
    assert Seat.translate_columns_to_aliases(new_kwargs={"tenant": 1}) == {
        "tenant_id": 1
    }
