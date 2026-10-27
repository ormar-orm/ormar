from typing import ForwardRef, Optional

import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import ModelDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Student(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lt_students")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


EnrollmentRef = ForwardRef("Enrollment")


class Course(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lt_courses")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=50)
    students = ormar.ManyToMany(Student, through=EnrollmentRef, related_name="courses")


class Enrollment(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lt_student_course")

    student: Student = ormar.ForeignKey(
        Student, primary_key=True, name="student_id", related_name="enrollments"
    )
    course: Course = ormar.ForeignKey(
        Course, primary_key=True, name="course_id", related_name="enrollments"
    )
    grade: Optional[int] = ormar.Integer(nullable=True)


Course.update_forward_refs()

PersonRef = ForwardRef("Person")


class Friendship(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lt_friendships")

    person: PersonRef = ormar.ForeignKey(  # type: ignore
        PersonRef, primary_key=True, name="person_id", related_name="friendships"
    )
    friend: PersonRef = ormar.ForeignKey(  # type: ignore
        PersonRef, primary_key=True, name="friend_id", related_name="befriended"
    )


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lt_people")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    friends = ormar.ManyToMany(
        PersonRef,
        through=Friendship,
        related_name="friend_of",
        through_relation_name="person",
        through_reverse_relation_name="friend",
    )


Friendship.update_forward_refs()
Person.update_forward_refs()

create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def clean():
    async with base_ormar_config.database:
        for model in (Enrollment, Friendship, Course, Student, Person):
            await model.objects.delete(each=True)
        yield


def test_user_link_table_columns_are_reused():
    table = Enrollment.ormar_config.table
    assert sorted(column.name for column in table.c) == [
        "course_id",
        "grade",
        "student_id",
    ]
    assert [c.name for c in table.primary_key.columns] == ["student_id", "course_id"]
    field = Course.ormar_config.model_fields["students"]
    assert field.through_owner_field_name() == "course"
    assert field.through_target_field_name() == "student"


@pytest.mark.asyncio
async def test_many_to_many_through_user_link_table():
    ann = await Student.objects.create(name="Ann")
    bob = await Student.objects.create(name="Bob")
    math = await Course.objects.create(title="Math")
    await math.students.add(ann, grade=5)
    await math.students.add(bob, grade=3)
    assert (await Enrollment.objects.get(pk=(ann.id, math.id))).grade == 5
    assert sorted(s.name for s in await math.students.all()) == ["Ann", "Bob"]
    good = await math.students.filter(enrollment__grade__gte=4).all()
    assert [s.name for s in good] == ["Ann"]
    loaded = await Course.objects.select_related("students").get()
    assert sorted((s.name, s.enrollment.grade) for s in loaded.students) == [
        ("Ann", 5),
        ("Bob", 3),
    ]
    fetched = await Course.objects.prefetch_related("students").get()
    assert sorted(s.name for s in fetched.students) == ["Ann", "Bob"]
    assert [c.title for c in await ann.courses.all()] == ["Math"]
    assert len(await ann.enrollments.all()) == 1
    await math.students.all()
    await math.students.remove(bob)
    assert await Enrollment.objects.count() == 1
    await math.students.clear()
    assert await Enrollment.objects.count() == 0


@pytest.mark.asyncio
async def test_self_referencing_user_link_table():
    ann = await Person.objects.create(name="Ann")
    bob = await Person.objects.create(name="Bob")
    await ann.friends.add(bob)
    assert [f.name for f in await ann.friends.all()] == ["Bob"]
    assert [f.name for f in await bob.friend_of.all()] == ["Ann"]
    assert (await Friendship.objects.get()).pk == (ann.id, bob.id)


def test_ambiguous_self_referencing_link_table_raises():
    with pytest.raises(ModelDefinitionError, match="through_relation_name"):
        RobotRef = ForwardRef("Robot")

        class Pairing(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="lt_pairings")

            left: RobotRef = ormar.ForeignKey(  # type: ignore
                RobotRef, primary_key=True, related_name="lefts"
            )
            right: RobotRef = ormar.ForeignKey(  # type: ignore
                RobotRef, primary_key=True, related_name="rights"
            )

        class Robot(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="lt_robots")

            id: int = ormar.Integer(primary_key=True)
            pals = ormar.ManyToMany(RobotRef, through=Pairing, related_name="pal_of")

        Pairing.update_forward_refs(Robot=Robot)
        Robot.update_forward_refs(Robot=Robot)


def test_inheriting_many_to_many_with_user_link_table_raises():
    with pytest.raises(ModelDefinitionError, match="link table"):

        class Tag(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="lt_tags")

            id: int = ormar.Integer(primary_key=True)

        class TagLink(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="lt_tag_links")

            id: int = ormar.Integer(primary_key=True)
            tag: Optional[Tag] = ormar.ForeignKey(Tag, related_name="tag_links")

        class Tagged(ormar.Model):
            ormar_config = base_ormar_config.copy(abstract=True)

            id: int = ormar.Integer(primary_key=True)
            tags = ormar.ManyToMany(Tag, through=TagLink)

        class Post(Tagged):
            ormar_config = base_ormar_config.copy(tablename="lt_posts")
