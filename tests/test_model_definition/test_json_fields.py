from typing import Optional

import pydantic
import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="authors")
    id: int = ormar.Integer(primary_key=True)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="books")
    id: int = ormar.Integer(primary_key=True)
    author: Author = ormar.ForeignKey(Author, name="author_id")
    my_data: dict | None = ormar.JSON(nullable=True)


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="json_owners")
    id: int = ormar.Integer(primary_key=True)
    data: Optional[dict] = ormar.JSON(nullable=True)


class Pet(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="json_pets")
    id: int = ormar.Integer(primary_key=True)
    owner: Optional[Owner] = ormar.ForeignKey(Owner)


class Vet(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="json_vets")
    id: int = ormar.Integer(primary_key=True)
    owner: Optional[Owner] = ormar.ForeignKey(Owner, related_name="vets")
    name: str = ormar.String(max_length=10, default="vet")

    @pydantic.field_validator("name")
    @classmethod
    def strip_name(cls, value: str) -> str:
        return value.strip()


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_construct_with_empty_relation():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            author = await Author.objects.create()
            await Book.objects.create(author=author, my_data={"aa": 1})
            # this should not error out
            await Author.objects.select_related(Author.books).all()
            # this should also not error out
            await Author.objects.select_all().all()


@pytest.mark.asyncio
@pytest.mark.parametrize("model", [Pet, Vet])
async def test_forward_select_related_keeps_json_decoded(model):
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            owner = await Owner.objects.create(data={"a": 1})
            await model.objects.create(owner=owner)
            assert owner.data == {"a": 1}

            loaded = await model.objects.select_related("owner").get()

            assert loaded.owner.data == {"a": 1}
            assert loaded.model_dump()["owner"]["data"] == {"a": 1}


def test_related_instance_keeps_json_decoded():
    owner = Owner(id=1, data={"b": 2})
    pet = Pet(id=1, owner=owner)

    assert pet.owner is owner
    assert owner.data == {"b": 2}


def test_related_instance_keeps_json_decoded_when_validation_fails():
    owner = Owner(id=1, data={"b": 2})

    with pytest.raises(pydantic.ValidationError):
        Pet(id="not an id", owner=owner)

    assert owner.data == {"b": 2}
