from typing import ForwardRef

import pytest

import ormar
from ormar.relations.relation_proxy import RelationProxy
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Member(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="members")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    follows: RelationProxy["Member"] = ormar.ManyToMany(
        ForwardRef("Member"), related_name="followers"
    )


Member.update_forward_refs()

create_test_database = init_tests(base_ormar_config)


async def get_links() -> list:
    through = Member.ormar_config.model_fields["follows"].through
    rows = await through.objects.all()
    return sorted((row.from_member.pk, row.to_member.pk) for row in rows)


@pytest.mark.asyncio
async def test_add_on_reverse_side_links_owner_as_target():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            a = await Member(name="a").save()
            c = await Member(name="c").save()

            await c.followers.add(a)

            assert await get_links() == [(a.pk, c.pk)]
            a_db = await Member.objects.select_related("follows").get(pk=a.pk)
            assert [m.name for m in a_db.follows] == ["c"]
            c_db = await Member.objects.select_related("followers").get(pk=c.pk)
            assert [m.name for m in c_db.followers] == ["a"]


@pytest.mark.asyncio
async def test_remove_on_reverse_side_removes_correct_row():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            a = await Member(name="a").save()
            c = await Member(name="c").save()
            await a.follows.add(c)
            await c.follows.add(a)

            await c.followers.remove(a)

            assert await get_links() == [(c.pk, a.pk)]


@pytest.mark.asyncio
async def test_upsert_through_on_reverse_side_updates_existing_row():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            a = await Member(name="a").save()
            c = await Member(name="c").save()
            await c.followers.add(a)

            await c.followers.queryset_proxy.upsert_through_instance(a)

            assert await get_links() == [(a.pk, c.pk)]
