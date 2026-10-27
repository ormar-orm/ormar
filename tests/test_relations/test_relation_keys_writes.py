from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import ModelPersistenceError
from tests import cross_check
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Country(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rkw_countries")

    id: int = ormar.Integer(primary_key=True)
    iso_code: str = ormar.String(max_length=2, unique=True)
    name: str = ormar.String(max_length=50)


class Address(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rkw_addresses")

    id: int = ormar.Integer(primary_key=True)
    street: str = ormar.String(max_length=50)
    country: Optional[Country] = ormar.ForeignKey(
        Country, to_field="iso_code", name="country_iso", related_name="addresses"
    )


class Student(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rkw_students")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Course(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rkw_courses")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=50)


class Enrollment(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rkw_enrollments")

    student: Student = ormar.ForeignKey(
        Student, primary_key=True, related_name="enrollments"
    )
    course: Course = ormar.ForeignKey(
        Course, primary_key=True, related_name="enrollments"
    )
    grade: Optional[int] = ormar.Integer(nullable=True)


class Profile(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rkw_profiles")

    student: Student = ormar.ForeignKey(
        Student, primary_key=True, related_name="profiles"
    )
    bio: Optional[str] = ormar.String(max_length=50, nullable=True)


class Note(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rkw_notes")

    id: int = ormar.Integer(primary_key=True)
    text: str = ormar.String(max_length=50)
    profile: Optional[Profile] = ormar.ForeignKey(Profile, related_name="notes")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def clean():
    async with base_ormar_config.database:
        for model in (
            Club,
            Note,
            Profile,
            Enrollment,
            Address,
            Course,
            Student,
            Country,
        ):
            await model.objects.delete(each=True)
        yield


async def countries():
    pl = await Country.objects.create(id=10, iso_code="PL", name="Poland")
    de = await Country.objects.create(id=20, iso_code="DE", name="Germany")
    return pl, de


async def stored_isos() -> list:
    addresses = await Address.objects.order_by("street").all()
    return [a.country.iso_code if a.country else None for a in addresses]


@pytest.mark.asyncio
async def test_bulk_writes_store_the_to_field_value():
    pl, de = await countries()
    await Address.objects.bulk_create(
        [Address(street="a", country=pl), Address(street="b", country="DE")]
    )
    assert await stored_isos() == ["PL", "DE"]
    addresses = await Address.objects.order_by("street").all()
    for address in addresses:
        address.street += "2"
    await Address.objects.bulk_update(addresses)
    assert await stored_isos() == ["PL", "DE"]


@pytest.mark.asyncio
async def test_queryset_update_of_to_field_relation():
    pl, de = await countries()
    await Address.objects.create(street="a", country=pl)
    await Address.objects.filter(street="a").update(country=de)
    assert await stored_isos() == ["DE"]
    await Address.objects.filter(street="a").update(country={"iso_code": "PL"})
    assert await stored_isos() == ["PL"]
    await Address.objects.filter(street="a").update(country="DE")
    assert await stored_isos() == ["DE"]


@pytest.mark.asyncio
async def test_related_model_without_the_referenced_value_is_rejected():
    pl, _ = await countries()
    keyless = Country.model_construct(id=10, name="no iso")
    with pytest.raises(ModelPersistenceError, match="iso_code"):
        await Address(street="x", country=keyless).save()


@pytest.mark.asyncio
async def test_pk_setter_on_relation_key_moves_the_relation():
    ann = await Student.objects.create(name="Ann")
    math = await Course.objects.create(title="Math")
    art = await Course.objects.create(title="Art")
    enrollment = await Enrollment.objects.create(student=ann, course=math)
    enrollment.pk = (ann.id, art.id)
    assert enrollment.course.id == art.id
    await enrollment.update()
    assert (await Enrollment.objects.get()).course.id == art.id
    assert len(await Enrollment.objects.filter(pk=(ann, art)).all()) == 1


@pytest.mark.asyncio
async def test_relations_to_a_model_keyed_by_a_foreign_key():
    ann = await Student.objects.create(name="Ann")
    profile = await Profile.objects.create(student=ann, bio="hi")
    note = await Note.objects.create(text="n", profile=profile)
    assert len(await Note.objects.filter(profile=profile).all()) == 1
    loaded = await Note.objects.get(id=note.id)
    assert loaded.profile.pk == ann.id
    await loaded.profile.load()
    assert loaded.profile.bio == "hi"
    fetched = await Note.objects.prefetch_related("profile").get()
    assert fetched.profile.bio == "hi"
    joined = await Note.objects.select_related("profile").get()
    assert joined.profile.bio == "hi"


@pytest.mark.asyncio
async def test_reverse_select_related_into_models_keyed_by_a_foreign_key(monkeypatch):
    # the cross-check's detached copies drop relations, and a foreign key in the
    # primary key is required
    monkeypatch.setattr(cross_check, "ENABLED", False)
    ann = await Student.objects.create(name="Ann")
    await Profile.objects.create(student=ann, bio="hi")
    math = await Course.objects.create(title="Math")
    await Enrollment.objects.create(student=ann, course=math, grade=4)
    loaded = await Student.objects.select_related(["profiles", "enrollments"]).get()
    assert [p.bio for p in loaded.profiles] == ["hi"]
    assert [e.grade for e in loaded.enrollments] == [4]


@pytest.mark.asyncio
async def test_reverse_proxy_update_or_create_on_link_table():
    ann = await Student.objects.create(name="Ann")
    art = await Course.objects.create(title="Art")
    await ann.enrollments.update_or_create(course=art, grade=5)
    await ann.enrollments.update_or_create(course=art, grade=6)
    assert [(e.grade) for e in await Enrollment.objects.all()] == [6]


@pytest.mark.asyncio
async def test_serialization_of_to_field_relations():
    pl, _ = await countries()
    await Address.objects.create(street="a", country=pl)
    address = await Address.objects.get()
    assert address.model_dump(exclude_unset=True)["country"] == {"iso_code": "PL"}
    flat = Address.get_pydantic(flatten_fields={"country"})
    assert flat(**address.model_dump()).country == "PL"
    assert flat(street="b", country="DE").country == "DE"
    loaded = await Country.objects.select_related("addresses").get(id=pl.id)
    dumped = loaded.model_dump()
    assert dumped["addresses"][0]["street"] == "a"
    assert address.country.reference_dict() == {"iso_code": "PL"}
    assert pl.reference_dict() == {"id": pl.id}


class Club(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rkw_clubs")

    id: int = ormar.Integer(primary_key=True)
    members = ormar.ManyToMany(Profile, related_name="clubs")


@pytest.mark.asyncio
async def test_many_to_many_to_a_model_keyed_by_a_foreign_key():
    ann = await Student.objects.create(name="Ann")
    profile = await Profile.objects.create(student=ann, bio="hi")
    club = await Club.objects.create()
    await club.members.add(profile)
    loaded = await Club.objects.select_related("members").get()
    assert [m.bio for m in loaded.members] == ["hi"]
    assert [c.id for c in await profile.clubs.all()] == [club.id]


def test_invalid_relation_key_options_raise():
    with pytest.raises(ormar.ModelDefinitionError, match="nullable"):

        class BadPk(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="rkw_bad_pk")

            student: Student = ormar.ForeignKey(
                Student, primary_key=True, nullable=True, related_name="bad_pks"
            )

    with pytest.raises(ormar.ModelDefinitionError, match="foreign_key_name"):

        class BadName(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="rkw_bad_name")

            id: int = ormar.Integer(primary_key=True)
            student: Optional[Student] = ormar.ForeignKey(
                Student,
                db_constraint=False,
                foreign_key_name="fk_x",
                related_name="bad_names",
            )


@pytest.mark.asyncio
async def test_load_of_unsaved_full_model_keeps_pk_lookup():
    with pytest.raises(ormar.NoMatch):
        await Country(iso_code="FR", name="France").load()


def test_key_value_from_mapping_resolves_relation_parts():
    key = Profile.ormar_config.pk
    assert key.value_from(data={"student": Student(id=3, name="x")}) == 3
    assert key.value_from(data={"student": 4}) == 4
