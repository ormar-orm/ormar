# type: ignore
from typing import ForwardRef, Optional

import pytest

import ormar
from ormar.models.relation_graph import RelationGraph
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Writer(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rpf_writers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


LetterRef = ForwardRef("Letter")


class Note(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rpf_notes")

    id: int = ormar.Integer(primary_key=True)
    writer: Optional[Writer] = ormar.ForeignKey(Writer)
    letter: Optional[LetterRef] = ormar.ForeignKey(LetterRef)
    text: str = ormar.String(max_length=100)


class Pen(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rpf_pens")

    id: int = ormar.Integer(primary_key=True)
    writer: Optional[Writer] = ormar.ForeignKey(Writer)


# relation paths cached while Note.letter is still a ForwardRef
WRITER_PATHS_BEFORE = list(Writer._iterate_related_models())
PEN_DUMP_BEFORE = Pen(id=1).model_dump()
GRAPH_VERSION_BEFORE = RelationGraph.version


class Letter(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rpf_letters")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)


Note.update_forward_refs()


create_test_database = init_tests(base_ormar_config)


def test_relation_paths_skip_unresolved_forward_ref():
    assert WRITER_PATHS_BEFORE == ["notes", "pens"]
    assert PEN_DUMP_BEFORE == {"id": 1, "writer": None}


def test_relation_paths_are_rebuilt_after_update_forward_refs():
    assert RelationGraph.version > GRAPH_VERSION_BEFORE
    assert Writer._iterate_related_models() == ["notes__letter", "pens"]
    assert Letter._iterate_related_models() == ["notes__writer__pens"]
    assert Writer._relation_tree() == {"notes": {"letter": Ellipsis}, "pens": Ellipsis}


@pytest.mark.asyncio
async def test_bulk_create_and_dump_include_resolved_relation():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            writer = await Writer.objects.create(name="Ann")
            letters = [
                await Letter.objects.create(title=title) for title in ("Hi", "Bye")
            ]
            await Note.objects.bulk_create(
                objects=[
                    Note(writer=writer, letter=letter, text=letter.title.lower())
                    for letter in letters
                ]
            )

            writer = await Writer.objects.select_related(related="notes__letter").get()
            dumped = writer.model_dump(exclude={"notes__id", "notes__letter__id"})
            assert dumped == {
                "id": writer.id,
                "name": "Ann",
                "notes": [
                    {"text": "hi", "letter": {"title": "Hi"}},
                    {"text": "bye", "letter": {"title": "Bye"}},
                ],
                "pens": [],
            }
