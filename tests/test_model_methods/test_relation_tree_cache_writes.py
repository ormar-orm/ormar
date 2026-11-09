import copy
from typing import Optional

import pytest

import ormar
import ormar.models.mixins.relation_mixin as relation_mixin
from ormar.models.relation_graph import RelationGraph
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Publisher(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tree_writes_publishers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Writer(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tree_writes_writers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    publisher: Optional[Publisher] = ormar.ForeignKey(Publisher)


class Novel(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tree_writes_novels")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    writer: Optional[Writer] = ormar.ForeignKey(Writer)


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_save_related_with_exclude_keeps_the_cached_tree():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            publisher = await Publisher.objects.create(name="Pub")
            writer = await Writer.objects.create(name="Ann", publisher=publisher)
            novel = await Novel.objects.create(title="Book", writer=writer)
            tree = copy.deepcopy(Novel._relation_tree())

            novel.writer.name = "Bob"
            novel.writer.publisher.name = "Other"
            count = await novel.save_related(follow=True, exclude={"writer"})

            assert count == 0
            assert Novel._relation_tree() == tree
            assert (await Writer.objects.get(id=writer.id)).name == "Ann"

            count = await novel.save_related(
                follow=True, exclude={"writer": {"publisher"}}
            )

            assert count == 0
            assert Novel._relation_tree() == tree

            count = await novel.save_related(follow=True)
            assert count == 2
            assert (await Publisher.objects.get(id=publisher.id)).name == "Other"


@pytest.mark.asyncio
async def test_bulk_create_builds_the_tree_once(monkeypatch):
    calls = []
    original = relation_mixin.translate_list_to_dict

    def counting(*args, **kwargs):
        calls.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(relation_mixin, "translate_list_to_dict", counting)
    RelationGraph.changed()
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            writer = await Writer.objects.create(name="Ann")
            await Novel.objects.bulk_create(
                [Novel(title=f"Book {i}", writer=writer) for i in range(30)]
            )

            assert await Novel.objects.filter(writer=writer).count() == 30
            assert len(calls) == 1
