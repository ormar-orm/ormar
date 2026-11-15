from enum import Enum
from typing import Optional

import pytest
import sqlalchemy

import ormar
from ormar import ModelDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Country(int, Enum):
    UK = 1
    US = 2


class ArtistSize(str, Enum):
    SMALL = "s"
    BIG = "b"


class Artist(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="store_values_artists")

    id: int = ormar.Integer(primary_key=True)
    country: Country = ormar.Enum(enum_class=Country, store_values=True)
    previous: Optional[Country] = ormar.Enum(
        enum_class=Country, store_values=True, nullable=True
    )
    size: ArtistSize = ormar.Enum(
        enum_class=ArtistSize, store_values=True, default=ArtistSize.SMALL
    )


create_test_database = init_tests(base_ormar_config)


async def raw_rows(query: str) -> list:
    async with base_ormar_config.database.connection() as connection:
        result = await connection.execute(sqlalchemy.text(query))
        return [tuple(row) for row in result.fetchall()]


def test_int_enum_gets_integer_column_with_check():
    table = Artist.ormar_config.table
    assert isinstance(table.c.country.type.impl, sqlalchemy.Integer)
    checks = [
        str(constraint.sqltext.compile(compile_kwargs={"literal_binds": True}))
        for constraint in table.constraints
        if isinstance(constraint, sqlalchemy.CheckConstraint)
    ]
    assert "store_values_artists.country IN (1, 2)" in checks
    assert "store_values_artists.previous IN (1, 2)" in checks


def test_other_enum_stores_values_as_strings():
    column_type = Artist.ormar_config.table.c.size.type
    assert isinstance(column_type, sqlalchemy.Enum)
    assert column_type.enums == ["s", "b"]


def test_store_values_and_values_callable_are_exclusive():
    with pytest.raises(ModelDefinitionError):
        ormar.Enum(
            enum_class=Country,
            store_values=True,
            values_callable=lambda enum: [str(member.value) for member in enum],
        )


@pytest.mark.asyncio
async def test_store_values_stores_and_loads_values():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Artist.objects.create(country=Country.UK, size=ArtistSize.BIG)
            await Artist(country=2, previous=Country.UK).save()

            assert await raw_rows(
                "SELECT country, previous, size FROM store_values_artists ORDER BY id"
            ) == [(1, None, "b"), (2, 1, "s")]

            loaded = await Artist.objects.order_by("id").all()
            assert [
                (artist.country, artist.previous, artist.size) for artist in loaded
            ] == [
                (Country.UK, None, ArtistSize.BIG),
                (Country.US, Country.UK, ArtistSize.SMALL),
            ]

            assert await Artist.objects.filter(country=Country.UK).count() == 1
            assert await Artist.objects.filter(country=1).count() == 1
            assert await Artist.objects.filter(country__gt=Country.UK).count() == 1
            assert await Artist.objects.filter(previous__isnull=True).count() == 1
            assert await Artist.objects.filter(size=ArtistSize.BIG).count() == 1
            assert (
                await Artist.objects.filter(
                    country__in=[Country.UK, Country.US]
                ).count()
                == 2
            )

            await Artist.objects.filter(country=Country.UK).update(country=Country.US)
            assert await raw_rows(
                "SELECT country FROM store_values_artists ORDER BY id"
            ) == [(2,), (2,)]


@pytest.mark.asyncio
async def test_store_values_in_bulk_operations():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Artist.objects.bulk_create(
                [Artist(country=Country.UK), Artist(country=Country.US)]
            )
            artists = await Artist.objects.order_by("id").all()
            for artist in artists:
                artist.previous = artist.country
            await Artist.objects.bulk_update(artists, columns=["previous"])

            assert await raw_rows(
                "SELECT country, previous FROM store_values_artists ORDER BY id"
            ) == [(1, 1), (2, 2)]


@pytest.mark.asyncio
async def test_database_rejects_value_outside_int_enum():
    async with base_ormar_config.database:
        with pytest.raises(sqlalchemy.exc.DBAPIError):
            async with base_ormar_config.database.connection() as connection:
                await connection.execute(
                    sqlalchemy.text(
                        "INSERT INTO store_values_artists (country, size) "
                        "VALUES (7, 's')"
                    )
                )
