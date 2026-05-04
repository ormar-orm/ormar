import asyncio

import sqlalchemy
from examples import create_drop_database

import ormar
from ormar import DatabaseConnection

DATABASE_URL = "sqlite+aiosqlite:///select_columns_docs002_flatten.db"

database = DatabaseConnection(DATABASE_URL)
metadata = sqlalchemy.MetaData()

base_ormar_config = ormar.OrmarConfig(
    database=database,
    metadata=metadata,
)


class Company(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="companies")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Car(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    manufacturer = ormar.ForeignKey(Company)
    name: str = ormar.String(max_length=100)


@create_drop_database(base_config=base_ormar_config)
async def sample_data():
    toyota = await Company.objects.create(name="Toyota")
    await Car.objects.create(manufacturer=toyota, name="Corolla")

    # Queryset-level flatten: relation renders as its pk value on dump.
    # Missing relations are auto-loaded via select_related / prefetch_related.
    cars = await Car.objects.flatten_fields("manufacturer").all()
    assert cars[0].model_dump() == {
        "id": cars[0].id,
        "name": "Corolla",
        "manufacturer": toyota.id,
    }

    # Alternatively: nested dict form, or a Python FieldAccessor chain.
    cars = await Car.objects.flatten_fields({"manufacturer": ...}).all()
    cars = await Car.objects.flatten_fields(Car.manufacturer).all()

    # Ad-hoc flattening directly on model_dump(); accepts the same forms.
    cars = await Car.objects.select_related("manufacturer").all()
    cars[0].model_dump(flatten_fields="manufacturer")
    cars[0].model_dump(flatten_all=True)


asyncio.run(sample_data())
