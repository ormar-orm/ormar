from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Director(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="directors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, nullable=False, name="first_name")
    last_name: str = ormar.String(max_length=100, nullable=False, name="last_name")


class Movie(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="movies")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, nullable=False, name="title")
    year: int = ormar.Integer()
    profit: float = ormar.Float()
    director: Optional[Director] = ormar.ForeignKey(Director)


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_updating_selected_columns():
    async with base_ormar_config.database:
        director1 = await Director(name="Peter", last_name="Jackson").save()

        await Movie(
            id=1, name="Lord of The Rings", year=2003, director=director1, profit=1.212
        ).upsert()

        assert (await Movie.objects.get()).id == 1

        await Movie(
            id=1, name="Lord of The Rings", year=2003, director=director1, profit=1.212
        ).upsert(__force_save__=True)
        lotr = await Movie.objects.get()
        assert lotr.year == 2003
        assert lotr.name == "Lord of The Rings"
