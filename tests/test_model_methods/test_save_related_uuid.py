import uuid
from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Department(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: uuid.UUID = ormar.UUID(primary_key=True, default=uuid.uuid4)
    department_name: str = ormar.String(max_length=100)


class Course(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: uuid.UUID = ormar.UUID(primary_key=True, default=uuid.uuid4)
    course_name: str = ormar.String(max_length=100)
    completed: bool = ormar.Boolean()
    department: Optional[Department] = ormar.ForeignKey(Department)


class Student(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: uuid.UUID = ormar.UUID(primary_key=True, default=uuid.uuid4)
    name: str = ormar.String(max_length=100)
    courses = ormar.ManyToMany(Course)


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_uuid_pk_in_save_related():
    async with base_ormar_config.database:
        to_save = {
            "department_name": "Ormar",
            "courses": [
                {
                    "course_name": "basic1",
                    "completed": True,
                    "students": [{"name": "Abi"}, {"name": "Jack"}],
                },
                {
                    "course_name": "basic2",
                    "completed": True,
                    "students": [{"name": "Kate"}, {"name": "Miranda"}],
                },
            ],
        }
        department = Department(**to_save)
        await department.save_related(follow=True, save_all=True)
        department_check = (
            await Department.objects.select_all(follow=True, follow_reverse=2)
            .order_by(Department.courses.students.name.asc())
            .get()
        )
        to_exclude = {
            "id": ...,
            "courses": {"id": ..., "students": {"id", "studentcourse"}},
        }
        assert department_check.model_dump(exclude=to_exclude) == to_save
