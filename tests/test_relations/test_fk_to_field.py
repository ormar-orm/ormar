from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import ModelDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Country(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tf_countries")

    id: int = ormar.Integer(primary_key=True)
    iso_code: str = ormar.String(max_length=2, unique=True)
    name: str = ormar.String(max_length=50)


class Address(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tf_addresses")

    id: int = ormar.Integer(primary_key=True)
    street: str = ormar.String(max_length=50)
    country: Optional[Country] = ormar.ForeignKey(
        Country, to_field="iso_code", name="country_iso", related_name="addresses"
    )
    legacy: Optional[Country] = ormar.ForeignKey(
        Country, db_constraint=False, related_name="legacy_addresses"
    )


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def clean():
    async with base_ormar_config.database:
        await Address.objects.delete(each=True)
        await Country.objects.delete(each=True)
        yield


def foreign_keys(column) -> set:
    return {fk.target_fullname for fk in column.foreign_keys}


def test_to_field_and_db_constraint_ddl():
    table = Address.ormar_config.table
    assert foreign_keys(table.c.country_iso) == {"tf_countries.iso_code"}
    assert isinstance(
        table.c.country_iso.type, type(Country.ormar_config.table.c.iso_code.type)
    )
    assert foreign_keys(table.c.legacy) == set()


def test_to_field_must_be_unique():
    with pytest.raises(ModelDefinitionError, match="unique"):

        class Bad(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="tf_bad")

            id: int = ormar.Integer(primary_key=True)
            country: Optional[Country] = ormar.ForeignKey(
                Country, to_field="name", related_name="bad"
            )


def test_referential_actions_need_a_constraint():
    with pytest.raises(ModelDefinitionError, match="db_constraint"):

        class Bad2(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="tf_bad2")

            id: int = ormar.Integer(primary_key=True)
            country: Optional[Country] = ormar.ForeignKey(
                Country, db_constraint=False, ondelete="CASCADE", related_name="bad2"
            )


@pytest.mark.asyncio
async def test_to_field_relation_round_trip():
    poland = await Country.objects.create(iso_code="PL", name="Poland")
    await Address.objects.create(street="a", country=poland)
    await Address.objects.create(street="b", country="PL")
    plain = await Address.objects.order_by("street").all()
    assert [a.country.iso_code for a in plain] == ["PL", "PL"]
    assert plain[0].country.pk is None
    await plain[0].country.load()
    assert plain[0].country.name == "Poland"
    joined = await Address.objects.select_related("country").order_by("street").all()
    assert [a.country.name for a in joined] == ["Poland", "Poland"]
    loaded = await Country.objects.select_related("addresses").get()
    assert sorted(a.street for a in loaded.addresses) == ["a", "b"]
    fetched = await Country.objects.prefetch_related("addresses").get()
    assert sorted(a.street for a in fetched.addresses) == ["a", "b"]
    prefetched = await Address.objects.prefetch_related("country").all()
    assert {a.country.name for a in prefetched} == {"Poland"}
    assert await poland.addresses.count() == 2
    assert len(await Address.objects.filter(country=poland).all()) == 2
    assert len(await Address.objects.filter(country__name="Poland").all()) == 2


@pytest.mark.asyncio
async def test_relation_without_db_constraint():
    germany = await Country.objects.create(iso_code="DE", name="Germany")
    await Address.objects.create(street="c", legacy=germany)
    address = await Address.objects.select_related("legacy").get()
    assert address.legacy.name == "Germany"
    assert await germany.legacy_addresses.count() == 1


def test_to_field_must_be_a_column_field():
    with pytest.raises(ModelDefinitionError, match="not a column field"):

        class Bad3(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="tf_bad3")

            id: int = ormar.Integer(primary_key=True)
            country: Optional[Country] = ormar.ForeignKey(
                Country, to_field="bogus", related_name="bad3"
            )


@pytest.mark.asyncio
async def test_filter_plain_field_by_model_binds_its_pk():
    spain = await Country.objects.create(iso_code="ES", name="Spain")
    await Address.objects.create(street="d", country=spain)
    found = await Country.objects.filter(id=spain).all()
    assert [c.name for c in found] == ["Spain"]
