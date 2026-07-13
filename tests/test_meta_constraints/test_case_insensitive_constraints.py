import pytest
import sqlalchemy

import ormar.fields.constraints
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="persons",
        constraints=[
            ormar.fields.constraints.IndexColumns(
                "first_name", "last_name", unique=True, case_insensitive=True
            )
        ],
    )

    id: int = ormar.Integer(primary_key=True)
    first_name: str = ormar.String(max_length=100)
    last_name: str = ormar.String(max_length=100)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="tags",
        constraints=[
            ormar.fields.constraints.IndexColumns(
                sqlalchemy.func.lower(sqlalchemy.column("name")), unique=True
            )
        ],
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


create_test_database = init_tests(base_ormar_config)


def test_case_insensitive_unique_index_structure():
    indexes = list(Person.ormar_config.table.indexes)
    assert len(indexes) == 1
    index = indexes[0]
    assert index.unique is True
    assert index.name == "ix_persons_lower_first_name_lower_last_name"


def test_functional_index_expression_gets_safe_autoname():
    indexes = list(Tag.ormar_config.table.indexes)
    assert len(indexes) == 1
    index = indexes[0]
    assert index.unique is True
    assert index.name == "ix_tags_lower_name"


@pytest.mark.asyncio
async def test_case_insensitive_uniqueness_is_enforced():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Person.objects.create(first_name="John", last_name="Doe")
            await Person.objects.create(first_name="Jane", last_name="Doe")

            with pytest.raises(sqlalchemy.exc.IntegrityError):
                await Person.objects.create(first_name="JOHN", last_name="DOE")
