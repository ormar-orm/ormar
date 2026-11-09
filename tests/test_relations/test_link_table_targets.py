from typing import ForwardRef, Optional

import pydantic
import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import ModelDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Member(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ltt_members")

    tenant_id: int = ormar.Integer(primary_key=True)
    login: str = ormar.String(max_length=20, primary_key=True)


class Team(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ltt_teams")

    tenant_id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=10, primary_key=True)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ltt_tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=10)


class Membership(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ltt_memberships")

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
    tags = ormar.ManyToMany(Tag, related_name="memberships")


class Shift(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ltt_shifts")

    id: int = ormar.Integer(primary_key=True)
    membership: Optional[Membership] = ormar.ForeignKey(
        Membership, related_name="shifts"
    )
    hours: int = ormar.Integer(default=0)


class Review(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ltt_reviews")

    id: int = ormar.Integer(primary_key=True)
    membership: Membership = ormar.ForeignKey(  # type: ignore[assignment]
        ForwardRef("Membership"),
        columns={
            "tenant_id": "m_tenant",
            "member_login": "m_member",
            "team_code": "m_team",
        },
        related_name="reviews",
    )
    score: int = ormar.Integer(default=0)


Review.update_forward_refs()


class Seniority(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ltt_seniorities")

    membership: Membership = ormar.OneToOne(
        Membership, primary_key=True, related_name="seniority"
    )
    years: int = ormar.Integer(default=0)


class Project(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ltt_projects")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    memberships = ormar.ManyToMany(Membership, related_name="projects")


class Profile(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ltt_profiles")

    member: Member = ormar.ForeignKey(Member, primary_key=True, related_name="profiles")
    bio: Optional[str] = ormar.String(max_length=20, nullable=True)


class Mention(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ltt_mentions")

    id: int = ormar.Integer(primary_key=True)
    profile: Optional[Profile] = ormar.ForeignKey(Profile, related_name="mentions")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def data():
    async with base_ormar_config.database:
        for model in (
            Mention,
            Profile,
            Project.ormar_config.model_fields["memberships"].through,
            Project,
            Seniority,
            Review,
            Shift,
            Membership.ormar_config.model_fields["tags"].through,
            Membership,
            Tag,
            Team,
            Member,
        ):
            await model.objects.delete(each=True)
        alice = await Member.objects.create(tenant_id=1, login="alice")
        bob = await Member.objects.create(tenant_id=1, login="bob")
        core = await Team.objects.create(tenant_id=1, code="core")
        first = await Membership.objects.create(member=alice, team=core, role="dev")
        second = await Membership.objects.create(member=bob, team=core, role="ops")
        yield first, second


def foreign_keys(model: type[ormar.Model]) -> set[tuple[tuple[str, ...], tuple]]:
    return {
        (
            tuple(column.name for column in constraint.columns),
            tuple(element.target_fullname for element in constraint.elements),
        )
        for constraint in model.ormar_config.table.foreign_key_constraints
    }


def test_foreign_key_stores_every_key_column_once():
    table = Shift.ormar_config.table
    assert list(table.c.keys()) == [
        "id",
        "membership_tenant_id",
        "membership_member_login",
        "membership_team_code",
        "hours",
    ]
    member_login = Member.ormar_config.table.c.login.type
    assert isinstance(table.c.membership_member_login.type, type(member_login))
    assert foreign_keys(Shift) == {
        (
            (
                "membership_tenant_id",
                "membership_member_login",
                "membership_team_code",
            ),
            (
                "ltt_memberships.tenant_id",
                "ltt_memberships.member_login",
                "ltt_memberships.team_code",
            ),
        )
    }
    assert list(Review.ormar_config.table.c.keys()) == [
        "id",
        "m_tenant",
        "m_member",
        "m_team",
        "score",
    ]
    assert Seniority.ormar_config.pk.column_aliases() == (
        "membership_tenant_id",
        "membership_member_login",
        "membership_team_code",
    )
    assert list(Mention.ormar_config.table.c.keys()) == [
        "id",
        "profile_member_tenant_id",
        "profile_member_login",
    ]


def test_columns_are_keyed_by_the_key_columns():
    with pytest.raises(ModelDefinitionError, match="member_login"):

        class ByField(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="ltt_by_field")

            id: int = ormar.Integer(primary_key=True)
            membership: Membership = ormar.ForeignKey(
                Membership,
                columns={"tenant_id": "a", "member": "b", "team": "c"},
            )

    with pytest.raises(ModelDefinitionError, match="columns="):

        class ByName(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="ltt_by_name")

            id: int = ormar.Integer(primary_key=True)
            membership: Membership = ormar.ForeignKey(Membership, name="membership")


@pytest.mark.asyncio
async def test_crud_through_a_foreign_key_to_a_link_table(data):
    first, second = data
    shift = await Shift.objects.create(membership=first, hours=3)
    await Shift.objects.create(membership=(1, "bob", "core"), hours=5)
    await Shift.objects.create(
        membership={"tenant_id": 1, "member": (1, "alice"), "team": (1, "core")}
    )

    fetched = await Shift.objects.get(id=shift.id)
    assert fetched.membership.pk == (1, "alice", "core")
    assert fetched.membership.member.pk == (1, "alice")
    assert fetched.membership.role is None
    await fetched.membership.load()
    assert fetched.membership.role == "dev"

    fetched.membership = second
    await fetched.update()
    assert (await Shift.objects.get(id=shift.id)).membership.pk == (1, "bob", "core")
    await Shift.objects.filter(id=shift.id).update(membership=first)
    assert (await Shift.objects.get(id=shift.id)).membership.pk == first.pk

    await Shift.objects.bulk_create([Shift(membership=second, hours=1)])
    rows = await Shift.objects.filter(hours=1).all()
    rows[0].membership = first
    await Shift.objects.bulk_update(rows)
    assert await Shift.objects.filter(membership=first).count() == 3

    await fetched.delete()
    assert await Shift.objects.count() == 3


@pytest.mark.asyncio
async def test_joins_and_filters_across_a_foreign_key_to_a_link_table(data):
    first, second = data
    await Shift.objects.create(membership=first, hours=3)
    await Shift.objects.create(membership=second, hours=5)

    joined = await Shift.objects.select_related("membership__member").get(hours=3)
    assert joined.membership.role == "dev"
    assert joined.membership.member.login == "alice"
    prefetched = await Shift.objects.prefetch_related("membership__team").all()
    assert {shift.membership.role for shift in prefetched} == {"dev", "ops"}
    assert {shift.membership.team.code for shift in prefetched} == {"core"}

    assert await Shift.objects.filter(membership=first).count() == 1
    assert await Shift.objects.filter(membership=(1, "bob", "core")).count() == 1
    assert (
        await Shift.objects.filter(
            membership__in=[(1, "alice", "core"), (1, "bob", "core")]
        ).count()
        == 2
    )
    assert await Shift.objects.filter(membership__isnull=True).count() == 0
    assert await Shift.objects.filter(membership__role="ops").count() == 1
    assert await Shift.objects.filter(membership__member__login="alice").count() == 1
    assert [
        shift.hours
        for shift in await Shift.objects.select_related("membership__member")
        .order_by("-membership__member__login")
        .all()
    ] == [5, 3]
    assert await Membership.objects.filter(shifts__hours__gt=4).count() == 1


@pytest.mark.asyncio
async def test_reverse_side_of_a_foreign_key_to_a_link_table(data):
    first, _ = data
    await first.shifts.create(hours=2)
    await Shift.objects.create(membership=first, hours=4)
    selected = await Membership.objects.select_related("shifts").get(pk=first.pk)
    assert sorted(shift.hours for shift in selected.shifts) == [2, 4]
    prefetched = (
        await Membership.objects.prefetch_related("shifts").order_by("pk").all()
    )
    assert [len(membership.shifts) for membership in prefetched] == [2, 0]
    loaded = await Membership.objects.get(pk=first.pk)
    assert await loaded.shifts.count() == 2
    extra = await Shift.objects.create(hours=9)
    await loaded.shifts.add(extra)
    assert await Shift.objects.filter(membership=first).count() == 3
    await loaded.shifts.remove(extra)
    assert (await Shift.objects.get(id=extra.id)).membership is None
    await loaded.shifts.clear()
    assert await Shift.objects.filter(membership__isnull=True).count() == 3


@pytest.mark.asyncio
async def test_foreign_key_to_a_link_table_with_named_columns(data):
    first, _ = data
    review = await Review.objects.create(membership=first, score=3)
    fetched = await Review.objects.select_related("membership").get(id=review.id)
    assert fetched.membership.role == "dev"
    assert await first.reviews.count() == 1
    assert await Review.objects.values_list(["membership"]) == [((1, "alice", "core"),)]


@pytest.mark.asyncio
async def test_one_to_one_to_a_link_table_as_the_key(data):
    first, second = data
    seniority = await Seniority.objects.create(membership=first, years=4)
    assert seniority.pk == (1, "alice", "core")
    fetched = await Seniority.objects.get(pk=(1, "alice", "core"))
    assert fetched.years == 4
    assert fetched.membership.member.pk == (1, "alice")
    membership = await Membership.objects.select_related("seniority").get(pk=first.pk)
    assert membership.seniority.years == 4
    loose = await Membership.objects.select_related("seniority").get(pk=second.pk)
    assert loose.seniority is None
    assert await Membership.objects.filter(seniority__years=4).count() == 1
    fetched.years = 5
    await fetched.update()
    await fetched.load()
    assert fetched.years == 5
    await fetched.delete()
    assert await Seniority.objects.count() == 0


@pytest.mark.asyncio
async def test_many_to_many_to_and_on_a_link_table(data):
    first, second = data
    project = await Project.objects.create(name="ormar")
    await project.memberships.add(first)
    await project.memberships.add(second)
    assert {m.role for m in await project.memberships.all()} == {"dev", "ops"}
    assert await project.memberships.filter(member__login="bob").count() == 1
    joined = await Project.objects.select_related("memberships").get()
    assert {m.pk for m in joined.memberships} == {first.pk, second.pk}
    prefetched = await Project.objects.prefetch_related("memberships__member").get()
    assert {m.member.login for m in prefetched.memberships} == {"alice", "bob"}
    back = await Membership.objects.prefetch_related("projects").get(pk=first.pk)
    assert [p.name for p in back.projects] == ["ormar"]
    await project.memberships.remove(second)
    assert await project.memberships.count() == 1

    tag = await Tag.objects.create(name="oncall")
    await first.tags.add(tag)
    tagged = await Membership.objects.select_related("tags").get(pk=first.pk)
    assert [t.name for t in tagged.tags] == ["oncall"]
    assert (await Tag.objects.select_related("memberships").get()).memberships[
        0
    ].pk == first.pk
    assert await Membership.objects.filter(tags__name="oncall").count() == 1


@pytest.mark.asyncio
async def test_serializing_a_foreign_key_to_a_link_table(data):
    first, _ = data
    await Shift.objects.create(membership=first, hours=3)
    shift = await Shift.objects.get()
    nested = {
        "tenant_id": 1,
        "member": {"tenant_id": 1, "login": "alice"},
        "team": {"tenant_id": 1, "code": "core"},
    }
    assert shift.model_dump(exclude={"id"}) == {"hours": 3, "membership": nested}
    assert shift.model_dump(flatten_fields={"membership"})["membership"] == nested
    assert await Shift.objects.values(["membership"]) == [{"membership": nested}]
    assert await Shift.objects.values_list(["membership"]) == [((1, "alice", "core"),)]

    flat = Shift.get_pydantic(flatten_fields={"membership"})
    validated = flat.model_validate({"id": 1, "hours": 3, "membership": nested})
    assert validated.membership == nested
    with pytest.raises(pydantic.ValidationError):
        flat.model_validate({"membership": {**nested, "member": {"tenant_id": "x"}}})
    assert Shift(**shift.model_dump()).membership.pk == first.pk


@pytest.mark.asyncio
async def test_foreign_key_to_a_model_keyed_by_one_foreign_key_of_several_columns(
    data,
):
    first, _ = data
    profile = await Profile.objects.create(member=first.member, bio="hi")
    mention = await Mention.objects.create(profile=profile)
    fetched = await Mention.objects.select_related("profile").get(id=mention.id)
    assert fetched.profile.bio == "hi"
    assert fetched.profile.pk == (1, "alice")
    flat = fetched.model_dump(flatten_fields={"profile"})["profile"]
    assert flat == {"tenant_id": 1, "login": "alice"}
    validated = Mention.get_pydantic(flatten_fields={"profile"}).model_validate(
        {"id": 1, "profile": flat}
    )
    assert validated.profile == flat
    assert await Mention.objects.values(["profile"]) == [{"profile": flat}]
    assert await Mention.objects.filter(profile__member__login="alice").count() == 1
    back = await Profile.objects.prefetch_related("mentions").get()
    assert len(back.mentions) == 1
