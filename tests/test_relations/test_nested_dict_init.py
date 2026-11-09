from typing import Optional

import pytest
from pydantic import ValidationError

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Country(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nd_countries")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50, default="")


class Stamp(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nd_stamps")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50, default="")
    country: Optional[Country] = ormar.ForeignKey(Country)


class Letter(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nd_letters")

    id: int = ormar.Integer(primary_key=True)
    stamp: Optional[Stamp] = ormar.ForeignKey(Stamp)


class Color(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nd_colors")

    id: int = ormar.Integer(primary_key=True)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nd_tags")

    id: int = ormar.Integer(primary_key=True)
    color: Optional[Color] = ormar.ForeignKey(Color)


class Writer(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nd_writers")

    id: int = ormar.Integer(primary_key=True)
    tags = ormar.ManyToMany(Tag)


class Note(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nd_notes")

    id: int = ormar.Integer(primary_key=True)
    writer: Optional[Writer] = ormar.ForeignKey(Writer, related_name="notes")
    letter: Optional[Letter] = ormar.ForeignKey(Letter, related_name="notes")


class Shelf(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nd_shelves")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    color: Optional[Color] = ormar.ForeignKey(Color)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nd_books")

    id: int = ormar.Integer(primary_key=True)
    shelf: Optional[Shelf] = ormar.ForeignKey(Shelf, related_name="books")


create_test_database = init_tests(base_ormar_config)


def test_pk_only_dict_keeps_key_only_path():
    note = Note(id=1, letter={"id": 2})
    assert note.letter.id == 2
    assert note.letter.__pk_only__ is True


def test_pk_only_dict_with_empty_relations_keeps_key_only_path():
    note = Note(id=1, letter={"id": 2, "stamp": None, "notes": []})
    assert note.letter.__pk_only__ is True
    assert note.letter.stamp is None


def test_pk_only_dict_with_empty_relation_skips_required_fields():
    book = Book(id=1, shelf={"id": 2, "books": []})
    assert book.shelf.__pk_only__ is True


def test_nested_dict_requires_required_fields():
    with pytest.raises(ValidationError):
        Book(id=1, shelf={"id": 2, "color": {"id": 3}})
    book = Book(id=1, shelf={"id": 2, "name": "Top", "color": {"id": 3}})
    assert book.shelf.color.id == 3


def test_fk_to_fk_nested_dict_two_levels():
    note = Note(id=1, letter={"id": 2, "stamp": {"id": 3}})
    assert note.letter.__pk_only__ is False
    assert note.letter.stamp.id == 3
    assert note.letter.stamp.__pk_only__ is True
    assert note.model_dump()["letter"] == {"id": 2, "stamp": {"id": 3}}


def test_fk_to_fk_nested_dict_three_levels():
    note = Note(id=1, letter={"id": 2, "stamp": {"id": 3, "country": {"id": 4}}})
    assert note.letter.stamp.__pk_only__ is False
    assert note.letter.stamp.country.id == 4
    assert note.letter.stamp.country.__pk_only__ is True
    assert note.model_dump()["letter"] == {
        "id": 2,
        "stamp": {"id": 3, "name": "", "country": {"id": 4}},
    }


def test_nested_dict_with_full_inner_model():
    note = Note(id=1, letter={"id": 2, "stamp": {"id": 3, "name": "Penny Black"}})
    assert note.letter.__pk_only__ is False
    assert note.letter.stamp.__pk_only__ is False
    assert note.letter.stamp.name == "Penny Black"


def test_reverse_fk_list_of_nested_dicts():
    writer = Writer(id=1, notes=[{"id": 2, "letter": {"id": 3}}, {"id": 4}])
    first, second = writer.notes
    assert first.__pk_only__ is False
    assert first.letter.id == 3
    assert first.letter.__pk_only__ is True
    assert second.__pk_only__ is True
    assert writer.model_dump()["notes"] == [
        {"id": 2, "letter": {"id": 3}},
        {"id": 4},
    ]


def test_m2m_list_of_nested_dicts():
    writer = Writer(id=1, tags=[{"id": 5, "color": {"id": 6}}, {"id": 7}])
    first, second = writer.tags
    assert first.__pk_only__ is False
    assert first.color.id == 6
    assert second.__pk_only__ is True
    assert writer.model_dump(exclude={"notes"})["tags"] == [
        {"id": 5, "color": {"id": 6}},
        {"id": 7},
    ]


def test_back_reference_keeps_key_only_path():
    note = Note(id=2, letter={"id": 3, "notes": [{"id": 2, "letter": {"id": 3}}]})
    assert note.letter.__pk_only__ is True
    book = Book(id=1, shelf={"id": 2, "books": [{"id": 1, "shelf": {"id": 2}}]})
    assert book.shelf.__pk_only__ is True
    writer = Writer(id=1, notes=[{"id": 2, "writer": {"id": 1}}])
    assert writer.notes[0].__pk_only__ is True


def test_back_reference_with_other_nested_relation():
    note = Note(id=2, letter={"id": 3, "notes": [{"id": 2}], "stamp": {"id": 4}})
    assert note.letter.__pk_only__ is False
    assert note.letter.stamp.id == 4


def test_update_from_dict_with_nested_dict():
    note = Note(id=1)
    note.update_from_dict({"letter": {"id": 2, "stamp": {"id": 3}}})
    assert note.letter.__pk_only__ is False
    assert note.letter.stamp.id == 3


@pytest.mark.asyncio
async def test_save_related_and_create_with_nested_dicts():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Stamp.objects.create(id=3)
            await Letter.objects.create(id=2)
            writer = Writer(id=1, notes=[{"id": 4, "letter": {"id": 2}}])
            await writer.save()
            await writer.save_related(follow=True, save_all=True)
            saved_note = await Note.objects.get(id=4)
            assert saved_note.writer.id == 1
            assert saved_note.letter.id == 2

            note = await Note.objects.create(id=5, letter={"id": 2, "stamp": {"id": 3}})
            assert note.letter.stamp.id == 3
            assert (await Note.objects.get(id=5)).letter.id == 2

            notes = [Note(id=6, letter={"id": 2, "stamp": {"id": 3}})]
            await Note.objects.bulk_create(objects=notes)
            assert (await Note.objects.get(id=6)).letter.id == 2
