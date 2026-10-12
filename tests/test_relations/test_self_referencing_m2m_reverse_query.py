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


class Node(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nodes")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    links: RelationProxy["Node"] = ormar.ManyToMany(
        ForwardRef("Node"),
        related_name="linked_by",
        orders_by=["-name"],
        related_orders_by=["-name"],
    )


Node.update_forward_refs()

create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_querying_reverse_side_returns_only_related_rows():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            a = await Member(name="a").save()
            c = await Member(name="c").save()
            await Member(name="d").save()
            await a.follows.add(c)

            assert await c.followers.count() == 1
            assert [m.name for m in await c.followers.all()] == ["a"]
            assert await a.followers.count() == 0
            assert await a.follows.count() == 1
            assert [m.name for m in await a.follows.all()] == ["c"]


@pytest.mark.asyncio
async def test_filtering_through_both_sides_of_self_reference():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            a = await Member(name="a").save()
            c = await Member(name="c").save()
            d = await Member(name="d").save()
            await a.follows.add(c)
            await d.follows.add(a)

            followers_of_c = await Member.objects.filter(follows__name="c").all()
            assert [m.name for m in followers_of_c] == ["a"]
            followed_by_d = await Member.objects.filter(followers__name="d").all()
            assert [m.name for m in followed_by_d] == ["a"]
            assert [m.name for m in await a.followers.filter(name="d").all()] == ["d"]
            assert await a.followers.filter(name="c").count() == 0


@pytest.mark.asyncio
async def test_ordering_and_values_on_both_sides_of_self_reference():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            a = await Member(name="a").save()
            b = await Member(name="b").save()
            c = await Member(name="c").save()
            await a.follows.add(b)
            await a.follows.add(c)
            await c.follows.add(a)

            member = (
                await Member.objects.select_related("follows")
                .order_by("-follows__name")
                .get(pk=a.pk)
            )
            assert [m.name for m in member.follows] == ["c", "b"]
            member = (
                await Member.objects.select_related("followers")
                .order_by("followers__name")
                .get(pk=a.pk)
            )
            assert [m.name for m in member.followers] == ["c"]

            values = (
                await Member.objects.select_related("follows")
                .filter(pk=a.pk)
                .order_by("follows__name")
                .values(["name", "follows__name"], exclude_through=True)
            )
            assert values == [
                {"name": "a", "follows__name": "b"},
                {"name": "a", "follows__name": "c"},
            ]


@pytest.mark.asyncio
async def test_default_relation_ordering_on_both_sides_of_self_reference():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            a = await Node(name="a").save()
            b = await Node(name="b").save()
            c = await Node(name="c").save()
            await a.links.add(b)
            await a.links.add(c)
            await b.links.add(a)
            await c.links.add(a)

            node = await Node.objects.select_related("links").get(pk=a.pk)
            assert [n.name for n in node.links] == ["c", "b"]
            node = await Node.objects.select_related("linked_by").get(pk=a.pk)
            assert [n.name for n in node.linked_by] == ["c", "b"]
