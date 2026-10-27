from typing import Optional

import pytest
import pytest_asyncio

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Student(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fkpk_students")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Course(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fkpk_courses")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=50)


class Enrollment(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fkpk_student_course")

    student: Student = ormar.ForeignKey(
        Student, primary_key=True, name="student_id", related_name="enrollments"
    )
    course: Course = ormar.ForeignKey(
        Course, primary_key=True, name="course_id", related_name="enrollments"
    )
    grade: Optional[int] = ormar.Integer(nullable=True)


class Profile(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fkpk_profiles")

    student: Student = ormar.ForeignKey(
        Student, primary_key=True, related_name="profiles"
    )
    bio: Optional[str] = ormar.Text(nullable=True)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def clean():
    async with base_ormar_config.database:
        for model in (Enrollment, Profile, Student, Course):
            await model.objects.delete(each=True)
        yield


def test_ddl_primary_key_made_of_foreign_keys():
    table = Enrollment.ormar_config.table
    assert [c.name for c in table.primary_key.columns] == ["student_id", "course_id"]
    assert {fk.target_fullname for fk in table.c.student_id.foreign_keys} == {
        "fkpk_students.id"
    }
    assert Enrollment.ormar_config.pk.fields == ("student", "course")
    assert Profile.ormar_config.pk.is_composite is False


@pytest.mark.asyncio
async def test_link_table_crud_and_queries():
    ann = await Student.objects.create(name="Ann")
    bob = await Student.objects.create(name="Bob")
    math = await Course.objects.create(title="Math")
    enrollment = await Enrollment.objects.create(student=ann, course=math, grade=3)
    await Enrollment.objects.create(student=bob, course=math)
    assert enrollment.pk == (ann.id, math.id)
    fetched = await Enrollment.objects.get(pk=(ann.id, math.id))
    assert fetched.grade == 3
    assert fetched.pk == (ann.id, math.id)
    assert fetched == enrollment
    assert hash(fetched) == hash(await Enrollment.objects.get(student=ann, course=math))
    assert len(await Enrollment.objects.filter(pk__in=[(ann.id, math.id)]).all()) == 1
    await fetched.update(grade=5)
    assert (await Enrollment.objects.get(pk=(ann.id, math.id))).grade == 5
    joined = await Enrollment.objects.select_related(["student", "course"]).all()
    assert sorted(e.student.name for e in joined) == ["Ann", "Bob"]
    loaded = await Student.objects.select_related("enrollments").get(id=ann.id)
    assert [e.pk for e in loaded.enrollments] == [(ann.id, math.id)]
    prefetched = await Course.objects.prefetch_related("enrollments").get()
    assert len(prefetched.enrollments) == 2
    assert await math.enrollments.count() == 2
    await fetched.delete()
    assert await Enrollment.objects.count() == 1


@pytest.mark.asyncio
async def test_bulk_create_and_upsert_link_rows():
    ann = await Student.objects.create(name="Ann")
    math = await Course.objects.create(title="Math")
    art = await Course.objects.create(title="Art")
    await Enrollment.objects.bulk_create(
        [Enrollment(student=ann, course=math), Enrollment(student=ann, course=art)]
    )
    assert await Enrollment.objects.count() == 2
    await Enrollment(student=ann, course=math, grade=4).upsert()
    assert (await Enrollment.objects.get(student=ann, course=math)).grade == 4
    assert await Enrollment.objects.count() == 2


@pytest.mark.asyncio
async def test_shared_primary_key():
    ann = await Student.objects.create(name="Ann")
    profile = await Profile.objects.create(student=ann, bio="hi")
    assert profile.pk == ann.id
    assert (await Profile.objects.get(pk=ann.id)).bio == "hi"
    loaded = await Profile.objects.select_related("student").get()
    assert loaded.student.name == "Ann"
    loaded.bio = "hello"
    await loaded.update()
    assert (await Profile.objects.get()).bio == "hello"
