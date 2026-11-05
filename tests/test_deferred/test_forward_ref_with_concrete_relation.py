# type: ignore
from typing import ForwardRef, Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


BookRef = ForwardRef("Book")


class Review(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_reviews")

    id: int = ormar.Integer(primary_key=True)
    author: Optional[Author] = ormar.ForeignKey(Author)
    book: Optional[BookRef] = ormar.ForeignKey(BookRef)


class Editor(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_editors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Edition(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_editions")

    id: int = ormar.Integer(primary_key=True)
    book: Optional[BookRef] = ormar.ForeignKey(BookRef)
    editor: Optional[Editor] = ormar.ForeignKey(Editor)


class Translator(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_translators")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Translation(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_translations")

    id: int = ormar.Integer(primary_key=True)
    translators = ormar.ManyToMany(Translator)
    book: Optional[BookRef] = ormar.ForeignKey(BookRef)


class Publisher(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_publishers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


SeriesRef = ForwardRef("Series")


class Contract(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_contracts")

    id: int = ormar.Integer(primary_key=True)
    publisher: Optional[Publisher] = ormar.ForeignKey(Publisher)
    book: Optional[BookRef] = ormar.ForeignKey(BookRef)


class Catalogue(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_catalogues")

    id: int = ormar.Integer(primary_key=True)
    publisher: Optional[Publisher] = ormar.ForeignKey(Publisher)
    series: Optional[SeriesRef] = ormar.ForeignKey(SeriesRef)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_books")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)


Review.update_forward_refs()
Edition.update_forward_refs()
Translation.update_forward_refs()
Contract.update_forward_refs()
PUBLISHER_COMPLETE_WITH_CATALOGUE_PENDING = Publisher.__pydantic_complete__


class Series(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="fwc_series")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


Catalogue.update_forward_refs()


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_concrete_and_forward_ref_foreign_keys():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            author = await Author.objects.create(name="Ann")
            book = await Book.objects.create(title="Dune")
            await Review.objects.create(author=author, book=book)

            review = await Review.objects.select_related(["author", "book"]).get()
            assert review.author.name == "Ann"
            assert review.book.title == "Dune"

            author = await Author.objects.select_related("reviews__book").get()
            dumped = author.model_dump()
            assert dumped["reviews"][0]["book"]["title"] == "Dune"


@pytest.mark.asyncio
async def test_forward_ref_declared_before_concrete_foreign_key():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            editor = await Editor.objects.create(name="Bob")
            book = await Book.objects.create(title="Emma")
            await Edition.objects.create(editor=editor, book=book)

            editor = await Editor.objects.select_related("editions__book").get()
            assert editor.model_dump()["editions"][0]["book"]["title"] == "Emma"


@pytest.mark.asyncio
async def test_concrete_many_to_many_and_forward_ref_foreign_key():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            translator = await Translator.objects.create(name="Cid")
            book = await Book.objects.create(title="Ulysses")
            translation = await Translation.objects.create(book=book)
            await translation.translators.add(translator)

            translation = await Translation.objects.select_related(
                ["translators", "book"]
            ).get()
            assert translation.translators[0].name == "Cid"
            assert translation.book.title == "Ulysses"

            translator = await Translator.objects.select_related(
                "translations__book"
            ).get()
            dumped = translator.model_dump()
            assert dumped["translations"][0]["book"]["title"] == "Ulysses"


@pytest.mark.asyncio
async def test_related_model_completes_once_every_forward_ref_is_resolved():
    assert PUBLISHER_COMPLETE_WITH_CATALOGUE_PENDING is False
    assert Publisher.__pydantic_complete__ is True
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            publisher = await Publisher.objects.create(name="Orbit")
            book = await Book.objects.create(title="Hyperion")
            series = await Series.objects.create(name="Cantos")
            await Contract.objects.create(publisher=publisher, book=book)
            await Catalogue.objects.create(publisher=publisher, series=series)

            publisher = await Publisher.objects.select_related(
                ["contracts__book", "catalogues__series"]
            ).get()
            dumped = publisher.model_dump()
            assert dumped["contracts"][0]["book"]["title"] == "Hyperion"
            assert dumped["catalogues"][0]["series"]["name"] == "Cantos"
