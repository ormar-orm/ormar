from typing import ForwardRef, Optional

import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import RelationshipInstanceError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cm2m_tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Document(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cm2m_documents")

    tenant_id: int = ormar.Integer(primary_key=True)
    number: int = ormar.Integer(primary_key=True)
    title: Optional[str] = ormar.String(max_length=50, nullable=True)
    tags = ormar.ManyToMany(Tag, related_name="documents")


class Project(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cm2m_projects")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    documents = ormar.ManyToMany(Document, related_name="projects")


class Member(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cm2m_members")

    tenant_id: int = ormar.Integer(primary_key=True)
    login: str = ormar.String(max_length=20, primary_key=True)
    friends = ormar.ManyToMany(ForwardRef("Member"), related_name="friend_of")


Member.update_forward_refs()


class Team(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cm2m_teams")

    tenant_id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=10, primary_key=True)
    members = ormar.ManyToMany(
        Member, through=ForwardRef("Membership"), related_name="teams"
    )


class Membership(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cm2m_memberships")

    id: int = ormar.Integer(primary_key=True)
    role: Optional[str] = ormar.String(max_length=10, nullable=True)
    member: Optional[Member] = ormar.ForeignKey(
        Member, columns={"tenant_id": "m_tenant", "login": "m_login"}
    )
    team: Optional[Team] = ormar.ForeignKey(
        Team, columns={"tenant_id": "t_tenant", "code": "t_code"}
    )


Team.update_forward_refs()

create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def data():
    async with base_ormar_config.database:
        for model in (
            Membership,
            Member.ormar_config.model_fields["friends"].through,
            Team,
            Member,
            Project.ormar_config.model_fields["documents"].through,
            Document.ormar_config.model_fields["tags"].through,
            Project,
            Document,
            Tag,
        ):
            await model.objects.delete(each=True)
        first = await Document.objects.create(tenant_id=1, number=7, title="first")
        second = await Document.objects.create(tenant_id=1, number=8, title="second")
        yield first, second


def test_through_tables_store_composite_keys_in_several_columns():
    through = Document.ormar_config.model_fields["tags"].through.ormar_config.table
    assert {"document_tenant_id", "document_number", "tag"} <= set(through.c.keys())
    fks = {
        tuple(column.name for column in constraint.columns)
        for constraint in through.foreign_key_constraints
    }
    assert ("document_tenant_id", "document_number") in fks
    projects = Project.ormar_config.model_fields["documents"].through.ormar_config.table
    assert {"project", "document_tenant_id", "document_number"} <= set(
        projects.c.keys()
    )


@pytest.mark.asyncio
async def test_many_to_many_from_a_composite_key_model(data):
    first, second = data
    red = await Tag.objects.create(name="red")
    blue = await Tag.objects.create(name="blue")
    await first.tags.add(red)
    await first.tags.add(blue)
    await second.tags.add(red)
    assert {tag.name for tag in await first.tags.all()} == {"red", "blue"}
    assert await red.documents.count() == 2
    loaded = await Document.objects.select_related("tags").get(pk=(1, 7))
    assert {tag.name for tag in loaded.tags} == {"red", "blue"}
    fetched = await Document.objects.prefetch_related("tags").order_by("number").all()
    assert [len(document.tags) for document in fetched] == [2, 1]
    tag = await Tag.objects.prefetch_related("documents").get(name="red")
    assert sorted(document.pk for document in tag.documents) == [(1, 7), (1, 8)]
    assert len(await Document.objects.filter(tags__name="blue").all()) == 1
    await first.tags.remove(blue)
    assert await first.tags.count() == 1
    await first.tags.clear()
    assert await first.tags.count() == 0
    created = await second.tags.create(name="green")
    assert {t.name for t in await second.tags.all()} == {"red", "green"}
    assert created.documents[0].pk == (1, 8)


@pytest.mark.asyncio
async def test_many_to_many_to_a_composite_key_model(data):
    first, second = data
    project = await Project.objects.create(name="p")
    await project.documents.add(first)
    await project.documents.add(second)
    assert [d.title for d in await project.documents.order_by("number").all()] == [
        "first",
        "second",
    ]
    loaded = await Project.objects.select_related("documents").get()
    assert sorted(d.pk for d in loaded.documents) == [(1, 7), (1, 8)]
    fetched = await Project.objects.prefetch_related("documents").get()
    assert sorted(d.pk for d in fetched.documents) == [(1, 7), (1, 8)]
    reverse = await Document.objects.select_related("projects").get(pk=(1, 8))
    assert [p.name for p in reverse.projects] == ["p"]
    assert len(await Project.objects.filter(documents__title="first").all()) == 1
    assert len(await Project.objects.filter(documents__pk=(1, 7)).all()) == 1
    assert len(await Project.objects.filter(documents__pk__in=[(1, 8)]).all()) == 1
    await project.documents.remove(first)
    assert [d.pk for d in await project.documents.all()] == [(1, 8)]
    dumped = loaded.model_dump(flatten_fields={"documents"})
    assert sorted(dumped["documents"], key=lambda d: d["number"]) == [
        {"tenant_id": 1, "number": 7},
        {"tenant_id": 1, "number": 8},
    ]


@pytest.mark.asyncio
async def test_composite_keys_on_both_sides_with_a_link_table(data):
    ann = await Member.objects.create(tenant_id=1, login="ann")
    bob = await Member.objects.create(tenant_id=1, login="bob")
    team = await Team.objects.create(tenant_id=1, code="core")
    await team.members.add(ann, role="lead")
    await team.members.add(bob)
    links = await Membership.objects.order_by("id").all()
    assert [(link.member.pk, link.team.pk, link.role) for link in links] == [
        ((1, "ann"), (1, "core"), "lead"),
        ((1, "bob"), (1, "core"), None),
    ]
    leads = await team.members.filter(membership__role="lead").all()
    assert [member.login for member in leads] == ["ann"]
    loaded = await Team.objects.select_related("members").get()
    assert sorted(m.login for m in loaded.members) == ["ann", "bob"]
    assert {m.membership.role for m in loaded.members} == {"lead", None}
    fetched = await Member.objects.prefetch_related("teams").get(pk=(1, "bob"))
    assert [t.pk for t in fetched.teams] == [(1, "core")]
    await team.members.remove(ann)
    assert await Membership.objects.count() == 1


@pytest.mark.asyncio
async def test_self_referencing_many_to_many_on_a_composite_key_model(data):
    ann = await Member.objects.create(tenant_id=1, login="ann")
    bob = await Member.objects.create(tenant_id=1, login="bob")
    await ann.friends.add(bob)
    assert [f.login for f in await ann.friends.all()] == ["bob"]
    assert [f.login for f in await bob.friend_of.all()] == ["ann"]
    loaded = await Member.objects.select_related("friends").get(pk=(1, "ann"))
    assert [f.pk for f in loaded.friends] == [(1, "bob")]


def test_pydantic_model_of_a_many_to_many_to_a_composite_key_model():
    flat = Project.get_pydantic(flatten_fields={"documents"})
    parsed = flat(
        id=1,
        name="p",
        documents=[{"tenant_id": 1, "number": 7, "title": "x"}, (1, 8)],
    )
    assert parsed.documents == [
        {"tenant_id": 1, "number": 7},
        {"tenant_id": 1, "number": 8},
    ]
    nested = Project.get_pydantic()
    assert nested(id=1, name="p", documents=[{"tenant_id": 1, "number": 7}])


def test_many_to_many_to_a_composite_key_view_has_no_constraint():
    class DocumentView(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="cm2m_document_view", view=True)

        tenant_id: int = ormar.Integer(primary_key=True)
        number: int = ormar.Integer(primary_key=True)

    class Reader(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="cm2m_readers")

        id: int = ormar.Integer(primary_key=True)
        documents = ormar.ManyToMany(DocumentView, related_name="readers")

    through = Reader.ormar_config.model_fields["documents"].through.ormar_config.table
    assert {"documentview_tenant_id", "documentview_number"} <= set(through.c.keys())
    referenced = {
        element.target_fullname
        for constraint in through.foreign_key_constraints
        for element in constraint.elements
    }
    assert referenced == {"cm2m_readers.id"}


def test_many_to_many_keys_given_as_json_lists():
    project = Project(name="p", documents=[[1, 7], {"tenant_id": 1, "number": 8}])
    assert [document.pk for document in project.documents] == [(1, 7), (1, 8)]
    with pytest.raises(RelationshipInstanceError):
        Project(name="p", documents=[1, 7])


def test_constraint_name_of_a_composite_through_foreign_key():
    class Shelf(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="cm2m_shelves")

        room: int = ormar.Integer(primary_key=True)
        number: int = ormar.Integer(primary_key=True)
        tags = ormar.ManyToMany(
            Tag,
            related_name="shelves",
            through_foreign_key_name="fk_shelf_tags_shelf",
            through_relation_nullable=False,
        )

    through = Shelf.ormar_config.model_fields["tags"].through.ormar_config.table
    names = {constraint.name for constraint in through.foreign_key_constraints}
    assert "fk_shelf_tags_shelf" in names
    assert through.c.shelf_room.nullable is False
