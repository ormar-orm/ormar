import uuid
from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Department(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Course(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    completed: bool = ormar.Boolean(default=False)
    department: Optional[Department] = ormar.ForeignKey(Department)


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Pet(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: uuid.UUID = ormar.UUID(primary_key=True, default=uuid.uuid4)
    name: str = ormar.String(max_length=100)
    owner: Optional[Owner] = ormar.ForeignKey(Owner)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True, autoincrement=False)
    name: str = ormar.String(max_length=100)
    owner: Optional[Owner] = ormar.ForeignKey(Owner)


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_adding_relation_to_reverse_saves_the_child():
    async with base_ormar_config.database:
        department = await Department(name="Science").save()
        course = Course(name="Math", completed=False)

        await department.courses.add(course)
        assert course.pk is not None
        assert course.department == department
        assert department.courses[0] == course


@pytest.mark.asyncio
async def test_adding_child_with_default_pk_inserts_it():
    async with base_ormar_config.database:
        owner = await Owner(name="Ann").save()
        pet = Pet(name="Rex")

        await owner.pets.add(pet)
        assert await Pet.objects.filter(id=pet.id).count() == 1
        assert (await Pet.objects.get(id=pet.id)).owner.pk == owner.pk


@pytest.mark.asyncio
async def test_adding_child_with_explicit_pk_inserts_it():
    async with base_ormar_config.database:
        owner = await Owner(name="Ann").save()

        await owner.tags.add(Tag(id=5, name="red"))
        assert await Tag.objects.filter(id=5).count() == 1
        assert (await Tag.objects.get(id=5)).owner.pk == owner.pk


@pytest.mark.asyncio
async def test_adding_existing_child_updates_it():
    async with base_ormar_config.database:
        owner = await Owner(name="Ann").save()
        pet = await Pet(name="Rex").save()

        await owner.pets.add(pet)
        assert await Pet.objects.filter(id=pet.id).count() == 1
        assert (await Pet.objects.get(id=pet.id)).owner.pk == owner.pk
