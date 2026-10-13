from typing import ForwardRef, Optional

import pytest

import ormar
from ormar.exceptions import ModelPersistenceError
from ormar.relations.relation_proxy import RelationProxy
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class PostTag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="posts_x_tags")

    id: int = ormar.Integer(primary_key=True)
    sort_order: Optional[int] = ormar.Integer(nullable=True)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="posts")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=50)
    tags: RelationProxy[Tag] = ormar.ManyToMany(Tag, through=PostTag)


class Member(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="members")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    follows: RelationProxy["Member"] = ormar.ManyToMany(
        ForwardRef("Member"), related_name="followers"
    )


Member.update_forward_refs()

create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_adding_same_child_twice_creates_one_link():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            post = await Post(title="p").save()
            tag = await Tag(name="t").save()

            await post.tags.add(tag)
            await post.tags.add(tag)

            assert await PostTag.objects.count() == 1
            assert len(post.tags) == 1


@pytest.mark.asyncio
async def test_adding_same_child_with_through_fields_updates_link():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            post = await Post(title="p").save()
            tag = await Tag(name="t").save()

            await post.tags.add(tag, sort_order=1)
            await post.tags.add(tag, sort_order=2)

            links = await PostTag.objects.all()
            assert [link.sort_order for link in links] == [2]


@pytest.mark.asyncio
async def test_adding_new_child_without_pk_raises():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            post = await Post(title="p").save()

            with pytest.raises(ModelPersistenceError):
                await post.tags.add(Tag(name="t"))
            assert await PostTag.objects.count() == 0


@pytest.mark.asyncio
async def test_removing_child_with_duplicated_links_removes_all_of_them():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            post = await Post(title="p").save()
            tag = await Tag(name="t").save()
            await post.tags.add(tag)
            await PostTag.objects.create(post=post.pk, tag=tag.pk)

            await post.tags.remove(tag)

            assert await PostTag.objects.count() == 0
            assert len(post.tags) == 0


@pytest.mark.asyncio
async def test_removing_child_keeps_relation_when_delete_fails(monkeypatch):
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            post = await Post(title="p").save()
            tag = await Tag(name="t").save()
            await post.tags.add(tag)

            async def failing_delete(*args, **kwargs):
                raise RuntimeError("db down")

            monkeypatch.setattr(PostTag, "delete", failing_delete)
            with pytest.raises(RuntimeError):
                await post.tags.remove(tag)

            assert len(post.tags) == 1
            assert await PostTag.objects.count() == 1


@pytest.mark.asyncio
async def test_adding_self_reference_link_from_both_sides_creates_one_link():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            through = Member.ormar_config.model_fields["follows"].through
            a = await Member(name="a").save()
            c = await Member(name="c").save()

            await a.follows.add(c)
            await c.followers.add(a)

            assert await through.objects.count() == 1
