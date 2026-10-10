from typing import ForwardRef

import pytest

import ormar
from ormar.relations.relation_proxy import RelationProxy
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="people")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    friends: RelationProxy["Person"] = ormar.ManyToMany(
        ForwardRef("Person"), related_name="friend_of"
    )


Person.update_forward_refs()

create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_clear_self_referencing_many_to_many():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            through = Person.ormar_config.model_fields["friends"].through
            a = await Person(name="a").save()
            b = await Person(name="b").save()
            c = await Person(name="c").save()
            await a.friends.add(b)
            await c.friends.add(a)
            await b.friends.add(c)
            assert len(a.friends) == 1

            assert await a.friends.clear() == 1
            assert len(a.friends) == 0
            assert await through.objects.count() == 2


@pytest.mark.asyncio
async def test_clear_self_referencing_many_to_many_reverse():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            through = Person.ormar_config.model_fields["friends"].through
            a = await Person(name="a").save()
            b = await Person(name="b").save()
            c = await Person(name="c").save()
            await a.friends.add(b)
            await c.friends.add(b)
            await b.friends.add(c)

            assert await b.friend_of.clear() == 2
            assert len(b.friend_of) == 0
            assert await through.objects.count() == 1
            assert await b.friends.count() == 1
