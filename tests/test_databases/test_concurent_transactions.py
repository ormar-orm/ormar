import asyncio

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Department(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="departments")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Course(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="courses")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    department: Department | None = ormar.ForeignKey(Department)


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_asyncio_run():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction():
            csse = await Department(
                id=1337,
                name="Computer Science & Software Engineering",
            ).save()

            courses = [
                ("Introduction to Computer Science", 101),
                ("Computer Architecture", 255),
                ("Algorithms and Data Structures:I", 225),
                ("Algorithms and Data Structures:II", 226),
                ("Operating Systems", 360),
                ("Database Systems", 370),
                ("Concurrent Programming and Distributed Systems", 461),
                ("Analysis of Algorithms", 425),
                ("Data Analysis and Pattern Recognition", 535),
            ]

            async with asyncio.TaskGroup() as tasks:
                for name, id in courses:
                    tasks.create_task(Course(id=id, name=name, department=csse).save())

        assert len(await Course.objects.all()) == 9
