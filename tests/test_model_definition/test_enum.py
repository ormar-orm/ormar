from enum import Enum

import pytest
import sqlalchemy

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class IntCountry(int, Enum):
    UK = 1
    US = 2


class IntCountryModel(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="int_country")

    id: int = ormar.Integer(primary_key=True)
    country: IntCountry = ormar.Enum(
        enum_class=IntCountry,
        values_callable=lambda enum_class: [str(member.value) for member in enum_class],
        length=1,
    )


create_test_database = init_tests(base_ormar_config)


def test_enum_values_callable_uses_enum_values():
    field = IntCountryModel.ormar_config.model_fields["country"]
    assert field.column_type.enums == ["1", "2"]


def test_enum_name_sets_column_name_not_enum_type_name():
    class Named(ormar.Model):
        ormar_config = base_ormar_config.copy(
            metadata=sqlalchemy.MetaData(), tablename="int_country_named"
        )

        id: int = ormar.Integer(primary_key=True)
        country: IntCountry = ormar.Enum(enum_class=IntCountry, name="country_col")

    column = Named.ormar_config.table.c.country_col
    assert column.type.name == "intcountry"


@pytest.mark.asyncio
async def test_enum_values_callable_stores_and_loads_values():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await IntCountryModel.objects.create(country=IntCountry.UK)
            await IntCountryModel(country=IntCountry.US).save()

            async with base_ormar_config.database.connection() as connection:
                result = await connection.execute(
                    sqlalchemy.text("SELECT country FROM int_country ORDER BY id")
                )
                assert [row[0] for row in result.fetchall()] == ["1", "2"]

            loaded = await IntCountryModel.objects.order_by("id").all()
            assert [row.country for row in loaded] == [IntCountry.UK, IntCountry.US]

            found = await IntCountryModel.objects.get(country=IntCountry.US)
            assert found.country == IntCountry.US
            assert (
                await IntCountryModel.objects.filter(
                    country__in=[IntCountry.UK, IntCountry.US]
                ).count()
                == 2
            )

            await IntCountryModel.objects.filter(country=IntCountry.UK).update(
                country=IntCountry.US
            )
            assert (
                await IntCountryModel.objects.filter(country=IntCountry.US).count() == 2
            )
