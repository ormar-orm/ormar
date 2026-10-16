from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Genre(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_genres")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    category: Optional[Category] = ormar.ForeignKey(Category)


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_persons")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    country: Optional[str] = ormar.String(max_length=100, nullable=True)


class BookGenre(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_book_genres")

    id: int = ormar.Integer(primary_key=True)
    note: Optional[str] = ormar.String(max_length=100, nullable=True)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_books")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    author: Optional[Person] = ormar.ForeignKey(Person, related_name="books")
    editor: Optional[Person] = ormar.ForeignKey(Person, related_name="edited_books")
    genres: Optional[list[Genre]] = ormar.ManyToMany(
        Genre, through=BookGenre, related_name="books"
    )


class Chapter(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shared_chapters")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    book: Optional[Book] = ormar.ForeignKey(Book, related_name="chapters")


create_test_database = init_tests(base_ormar_config)


async def create_library() -> None:
    """
    Two books by the same author (also editor of the first one), sharing one
    genre with a different note on each link, two chapters per book.
    """
    fiction = await Category.objects.create(name="Fiction")
    poetry = await Category.objects.create(name="Poetry")
    drama = await Genre.objects.create(name="Drama", category=fiction)
    lyric = await Genre.objects.create(name="Lyric", category=poetry)
    author = await Person.objects.create(name="Author", country="PL")
    first = await Book.objects.create(title="First", author=author, editor=author)
    second = await Book.objects.create(title="Second", author=author)
    await BookGenre.objects.bulk_create(
        [
            BookGenre(book=first, genre=drama, note="first-drama"),
            BookGenre(book=first, genre=lyric, note="first-lyric"),
            BookGenre(book=second, genre=drama, note="second-drama"),
        ]
    )
    for book in (first, second):
        for number in (1, 2):
            await Chapter.objects.create(title=f"{book.title}-{number}", book=book)


@pytest.mark.asyncio
async def test_shared_fk_target_collects_all_loaded_children():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_library()
            chapters = await Chapter.objects.select_related("book__author").all()

            assert len(chapters) == 4
            first_chapters = [c for c in chapters if c.book.title == "First"]
            assert first_chapters[0].book is first_chapters[1].book
            assert first_chapters[0].book.author is chapters[-1].book.author
            assert sorted(c.title for c in first_chapters[0].book.chapters) == [
                "First-1",
                "First-2",
            ]
            assert sorted(b.title for b in first_chapters[0].book.author.books) == [
                "First",
                "Second",
            ]


@pytest.mark.asyncio
async def test_m2m_targets_stay_separate_per_parent():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_library()
            chapters = await Chapter.objects.select_related(
                "book__genres__category"
            ).all()

            books = {c.book.title: c.book for c in chapters}
            first_drama = next(g for g in books["First"].genres if g.name == "Drama")
            second_drama = next(g for g in books["Second"].genres if g.name == "Drama")

            assert len(books["First"].genres) == 2
            assert len(books["Second"].genres) == 1
            assert first_drama is not second_drama
            assert first_drama.bookgenre.note == "first-drama"
            assert second_drama.bookgenre.note == "second-drama"
            assert first_drama.category is not second_drama.category
            assert first_drama.category.name == second_drama.category.name


@pytest.mark.asyncio
async def test_same_model_on_different_paths_is_not_shared():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_library()
            book = (
                await Book.objects.select_related(["author", "editor"])
                .fields(["id", "title", "author", "editor__id", "editor__name"])
                .get(title="First")
            )

            assert book.author.pk == book.editor.pk
            assert book.author is not book.editor
            assert book.author.country == "PL"
            assert book.editor.country is None


@pytest.mark.asyncio
async def test_repeated_rows_attach_new_children_to_existing_models():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await create_library()
            authors = await Person.objects.select_related(
                ["books__genres", "books__chapters"]
            ).all()

            assert len(authors) == 1
            books = {b.title: b for b in authors[0].books}
            assert len(authors[0].books) == 2
            assert sorted(g.name for g in books["First"].genres) == ["Drama", "Lyric"]
            assert sorted(c.title for c in books["First"].chapters) == [
                "First-1",
                "First-2",
            ]
            assert [g.name for g in books["Second"].genres] == ["Drama"]
            assert len(books["Second"].chapters) == 2
