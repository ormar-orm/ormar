import asyncio
from enum import Enum
from typing import Optional

import pydantic
import pytest
import sqlalchemy.exc
from pydantic import Json

import ormar
from ormar import QuerySet
from ormar.exceptions import (
    ModelListEmptyError,
    ModelPersistenceError,
    QueryDefinitionError,
)
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config(force_rollback=True)


class MySize(Enum):
    SMALL = 0
    BIG = 1


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="books")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    author: str = ormar.String(max_length=100)
    genre: str = ormar.String(
        max_length=100,
        default="Fiction",
    )


class ToDo(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="todos")

    id: int = ormar.Integer(primary_key=True)
    text: str = ormar.String(max_length=500)
    completed: bool = ormar.Boolean(default=False)
    pairs: pydantic.Json = ormar.JSON(default=[])
    size = ormar.Enum(enum_class=MySize, default=MySize.SMALL)


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=500)


class Note(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="notes")

    id: int = ormar.Integer(primary_key=True)
    text: str = ormar.String(max_length=500)
    category: Optional[Category] = ormar.ForeignKey(Category)


class ItemConfig(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="item_config")

    id: Optional[int] = ormar.Integer(primary_key=True)
    item_id: str = ormar.String(max_length=32, index=True)
    pairs: pydantic.Json = ormar.JSON(default=["2", "3"])
    size = ormar.Enum(enum_class=MySize, default=MySize.SMALL)


class QuerySetCls(QuerySet):
    async def first_or_404(self, *args, **kwargs):
        entity = await self.get_or_none(*args, **kwargs)
        if not entity:
            # maybe HTTPException in fastapi
            raise ValueError("customer not found")
        return entity


class Customer(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="customer",
        queryset_class=QuerySetCls,
    )

    id: Optional[int] = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=32)


class JsonTestModel(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="test_model")

    id: int = ormar.Integer(primary_key=True)
    json_field: Json = ormar.JSON()


class Library(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="libraries")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class UniqueBook(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="unique_books")

    id: int = ormar.Integer(primary_key=True)
    isbn: str = ormar.String(max_length=20, unique=True)
    title: str = ormar.String(max_length=200)
    library: Optional[Library] = ormar.ForeignKey(
        Library, nullable=True, related_name="books"
    )


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_delete_and_update():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Book.objects.create(
                title="Tom Sawyer", author="Twain, Mark", genre="Adventure"
            )
            await Book.objects.create(
                title="War and Peace", author="Tolstoy, Leo", genre="Fiction"
            )
            await Book.objects.create(
                title="Anna Karenina", author="Tolstoy, Leo", genre="Fiction"
            )
            await Book.objects.create(
                title="Harry Potter", author="Rowling, J.K.", genre="Fantasy"
            )
            await Book.objects.create(
                title="Lord of the Rings", author="Tolkien, J.R.", genre="Fantasy"
            )

            all_books = await Book.objects.all()
            assert len(all_books) == 5

            await Book.objects.filter(author="Tolstoy, Leo").update(
                author="Lenin, Vladimir"
            )
            all_books = await Book.objects.filter(author="Lenin, Vladimir").all()
            assert len(all_books) == 2

            historic_books = await Book.objects.filter(genre="Historic").all()
            assert len(historic_books) == 0

            with pytest.raises(QueryDefinitionError):
                await Book.objects.update(genre="Historic")

            await Book.objects.filter(author="Lenin, Vladimir").update(genre="Historic")

            historic_books = await Book.objects.filter(genre="Historic").all()
            assert len(historic_books) == 2

            await Book.objects.delete(genre="Fantasy")
            all_books = await Book.objects.all()
            assert len(all_books) == 3

            await Book.objects.update(each=True, genre="Fiction")
            all_books = await Book.objects.filter(genre="Fiction").all()
            assert len(all_books) == 3

            with pytest.raises(QueryDefinitionError):
                await Book.objects.delete()

            await Book.objects.delete(each=True)
            all_books = await Book.objects.all()
            assert len(all_books) == 0


@pytest.mark.asyncio
async def test_get_or_create():
    async with base_ormar_config.database:
        tom, created = await Book.objects.get_or_create(
            title="Volume I", author="Anonymous", genre="Fiction"
        )
        assert await Book.objects.count() == 1
        assert created is True

        second_tom, created = await Book.objects.get_or_create(
            title="Volume I", author="Anonymous", genre="Fiction"
        )

        assert second_tom.pk == tom.pk
        assert created is False
        assert await Book.objects.count() == 1

        assert await Book.objects.create(
            title="Volume I", author="Anonymous", genre="Fiction"
        )
        with pytest.raises(ormar.exceptions.MultipleMatches):
            await Book.objects.get_or_create(
                title="Volume I", author="Anonymous", genre="Fiction"
            )


@pytest.mark.asyncio
async def test_get_or_create_with_defaults():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            book, created = await Book.objects.get_or_create(
                title="Nice book", _defaults={"author": "Mojix", "genre": "Historic"}
            )
            assert created is True
            assert book.author == "Mojix"
            assert book.title == "Nice book"
            assert book.genre == "Historic"

            book2, created = await Book.objects.get_or_create(
                author="Mojix", _defaults={"title": "Book2"}
            )
            assert created is False
            assert book2 == book
            assert book2.title == "Nice book"
            assert book2.author == "Mojix"
            assert book2.genre == "Historic"
            assert await Book.objects.count() == 1

            book, created = await Book.objects.get_or_create(
                title="doesn't exist",
                _defaults={
                    "title": "overwritten",
                    "author": "Mojix",
                    "genre": "Historic",
                },
            )
            assert created is True
            assert book.title == "overwritten"

            book2, created = await Book.objects.get_or_create(
                title="overwritten", _defaults={"title": "doesn't work"}
            )
            assert created is False
            assert book2.title == "overwritten"
            assert book2 == book


@pytest.mark.asyncio
async def test_get_or_create_recovers_from_concurrent_create():
    """Regression test for issue #1016."""
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            results = await asyncio.gather(
                UniqueBook.objects.get_or_create(
                    isbn="978-0-00-000000-1", title="Race"
                ),
                UniqueBook.objects.get_or_create(
                    isbn="978-0-00-000000-1", title="Race"
                ),
            )

            (book1, c1), (book2, c2) = results
            assert book1.pk == book2.pk
            assert book1.isbn == "978-0-00-000000-1"
            assert sorted([c1, c2]) == [False, True]
            assert await UniqueBook.objects.count() == 1


@pytest.mark.asyncio
async def test_get_or_create_propagates_unrelated_integrity_error():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await UniqueBook.objects.create(isbn="978-0-00-000000-2", title="Original")

            with pytest.raises(sqlalchemy.exc.IntegrityError):
                # ``isbn`` matches an existing row but ``title`` doesn't,
                # so the initial get returns NoMatch, the create violates
                # the unique constraint on isbn, and the retry get also
                # returns NoMatch (title='Different' doesn't match) — so
                # the integrity error is re-raised.
                await UniqueBook.objects.get_or_create(
                    isbn="978-0-00-000000-2", title="Different"
                )


@pytest.mark.asyncio
async def test_proxy_get_or_create_recovers_from_concurrent_create():
    """Same real-concurrency race as
    ``test_get_or_create_recovers_from_concurrent_create`` but via the
    m2m / reverse-fk proxy ``QuerysetProxy.get_or_create``, which has its
    own try/except around the get/create pair.
    """
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            library = await Library.objects.create(name="Main")

            results = await asyncio.gather(
                library.books.get_or_create(
                    isbn="978-0-00-000000-3", title="ProxyRace"
                ),
                library.books.get_or_create(
                    isbn="978-0-00-000000-3", title="ProxyRace"
                ),
            )

            (book1, c1), (book2, c2) = results
            assert book1.pk == book2.pk
            assert sorted([c1, c2]) == [False, True]
            assert await UniqueBook.objects.count() == 1


@pytest.mark.asyncio
async def test_proxy_get_or_create_propagates_unrelated_integrity_error():
    """``QuerysetProxy.get_or_create`` re-raises the original
    ``IntegrityError`` when the retry ``get`` still doesn't match —
    same semantics as ``QuerySet.get_or_create``.
    """
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            library = await Library.objects.create(name="Main")
            await UniqueBook.objects.create(
                isbn="978-0-00-000000-4", title="Original", library=library
            )

            with pytest.raises(sqlalchemy.exc.IntegrityError):
                await library.books.get_or_create(
                    isbn="978-0-00-000000-4", title="Different"
                )


@pytest.mark.asyncio
async def test_update_or_create():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            tom = await Book.objects.update_or_create(
                title="Volume I", author="Anonymous", genre="Fiction"
            )
            assert await Book.objects.count() == 1

            assert await Book.objects.update_or_create(id=tom.id, genre="Historic")
            assert await Book.objects.count() == 1

            assert await Book.objects.update_or_create(pk=tom.id, genre="Fantasy")
            assert await Book.objects.count() == 1

            assert await Book.objects.create(
                title="Volume I", author="Anonymous", genre="Fantasy"
            )
            with pytest.raises(ormar.exceptions.MultipleMatches):
                await Book.objects.get(
                    title="Volume I", author="Anonymous", genre="Fantasy"
                )


@pytest.mark.asyncio
async def test_update_or_create_creates_when_pk_does_not_exist():
    """``update_or_create`` with a pk that doesn't exist must create the
    row at that pk instead of raising ``NoMatch``.
    """
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            assert await Book.objects.count() == 0

            book = await Book.objects.update_or_create(
                id=42, title="X", author="Y", genre="Fiction"
            )
            assert book.id == 42
            assert await Book.objects.count() == 1

            updated = await Book.objects.update_or_create(id=42, genre="Historic")
            assert updated.id == 42
            assert updated.genre == "Historic"
            assert await Book.objects.count() == 1


@pytest.mark.asyncio
async def test_update_or_create_creates_when_pk_alias_does_not_exist():
    """Same as above but using the ``pk`` alias kwarg, which goes
    through the ``kwargs.pop("pk")`` rename branch.
    """
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            book = await Book.objects.update_or_create(
                pk=99, title="A", author="B", genre="Fiction"
            )
            assert book.id == 99
            assert await Book.objects.count() == 1


@pytest.mark.asyncio
async def test_proxy_update_or_create_creates_when_pk_does_not_exist():
    """``QuerysetProxy.update_or_create`` (m2m / reverse-fk) duplicates
    the same get/create logic and must also create when given a missing
    pk instead of raising ``NoMatch``.
    """
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            library = await Library.objects.create(name="Main")

            book = await library.books.update_or_create(
                id=123, isbn="978-0-00-000000-9", title="Created"
            )
            assert book.id == 123
            assert book.isbn == "978-0-00-000000-9"
            assert await UniqueBook.objects.count() == 1


@pytest.mark.asyncio
async def test_bulk_create():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await ToDo.objects.bulk_create(
                [
                    ToDo(text="Buy the groceries."),
                    ToDo(text="Call Mum.", completed=True),
                    ToDo(text="Send invoices.", completed=True),
                ]
            )

            todoes = await ToDo.objects.all()
            assert len(todoes) == 3
            for todo in todoes:
                assert todo.pk is not None

            completed = await ToDo.objects.filter(completed=True).all()
            assert len(completed) == 2

            with pytest.raises(ormar.exceptions.ModelListEmptyError):
                await ToDo.objects.bulk_create([])


@pytest.mark.asyncio
async def test_bulk_create_json_field():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(
            force_rollback=True
        ) as transaction:
            json_value = {"a": 1}
            test_model_1 = JsonTestModel(id=1, json_field=json_value)
            test_model_2 = JsonTestModel(id=2, json_field=json_value)

            # store one with .save() and the other with .bulk_create()
            await test_model_1.save()
            await JsonTestModel.objects.bulk_create([test_model_2])

            # refresh from the database
            await test_model_1.load()
            await test_model_2.load()

            assert test_model_1.json_field == test_model_2.json_field  # True

            # try to query the json field
            table = JsonTestModel.ormar_config.table
            query = table.select().where(table.c.json_field["a"].as_integer() == 1)
            res = [
                JsonTestModel.from_row(record, source_model=JsonTestModel)
                for record in list(
                    (await transaction._connection.execute(query)).mappings().all()
                )
            ]

            assert test_model_1 in res
            assert test_model_2 in res
            assert len(res) == 2


@pytest.mark.asyncio
async def test_bulk_create_with_relation():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            category = await Category.objects.create(name="Sample Category")

            await Note.objects.bulk_create(
                [
                    Note(text="Buy the groceries.", category=category),
                    Note(text="Call Mum.", category=category),
                ]
            )

            todoes = await Note.objects.all()
            assert len(todoes) == 2
            for todo in todoes:
                assert todo.category.pk == category.pk


@pytest.mark.asyncio
async def test_bulk_update():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await ToDo.objects.bulk_create(
                [
                    ToDo(text="Buy the groceries."),
                    ToDo(text="Call Mum.", completed=True),
                    ToDo(text="Send invoices.", completed=True),
                ]
            )
            todoes = await ToDo.objects.all()
            assert len(todoes) == 3

            for todo in todoes:
                todo.text = todo.text + "_1"
                todo.completed = False
                todo.size = MySize.BIG

            await ToDo.objects.bulk_update(todoes)

            completed = await ToDo.objects.filter(completed=False).all()
            assert len(completed) == 3

            todoes = await ToDo.objects.all()
            assert len(todoes) == 3

            for todo in todoes:
                assert todo.text[-2:] == "_1"
                assert todo.size == MySize.BIG


@pytest.mark.asyncio
async def test_bulk_update_with_only_selected_columns():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await ToDo.objects.bulk_create(
                [
                    ToDo(text="Reset the world simulation.", completed=False),
                    ToDo(text="Watch kittens.", completed=True),
                ]
            )

            todoes = await ToDo.objects.all()
            assert len(todoes) == 2

            for todo in todoes:
                todo.text = todo.text + "_1"
                todo.completed = False

            await ToDo.objects.bulk_update(todoes, columns=["completed"])

            completed = await ToDo.objects.filter(completed=False).all()
            assert len(completed) == 2

            todoes = await ToDo.objects.all()
            assert len(todoes) == 2

            for todo in todoes:
                assert todo.text[-2:] != "_1"


@pytest.mark.asyncio
async def test_bulk_update_with_relation():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            category = await Category.objects.create(name="Sample Category")
            category2 = await Category.objects.create(name="Sample II Category")

            await Note.objects.bulk_create(
                [
                    Note(text="Buy the groceries.", category=category),
                    Note(text="Call Mum.", category=category),
                    Note(text="Text skynet.", category=category),
                ]
            )

            notes = await Note.objects.all()
            assert len(notes) == 3

            for note in notes:
                note.category = category2

            await Note.objects.bulk_update(notes)

            notes_upd = await Note.objects.all()
            assert len(notes_upd) == 3

            for note in notes_upd:
                assert note.category.pk == category2.pk


@pytest.mark.asyncio
async def test_bulk_update_not_saved_objts():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            category = await Category.objects.create(name="Sample Category")
            with pytest.raises(ModelPersistenceError):
                await Note.objects.bulk_update(
                    [
                        Note(text="Buy the groceries.", category=category),
                        Note(text="Call Mum.", category=category),
                    ]
                )

            with pytest.raises(ModelListEmptyError):
                await Note.objects.bulk_update([])


@pytest.mark.asyncio
async def test_bulk_operations_with_json():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(
            force_rollback=True
        ) as transaction:
            items = [
                ItemConfig(item_id="test1"),
                ItemConfig(item_id="test2"),
                ItemConfig(item_id="test3"),
            ]
            await ItemConfig.objects.bulk_create(items)
            items = await ItemConfig.objects.all()
            assert all(x.pairs == ["2", "3"] for x in items)

            for item in items:
                item.pairs = ["1"]

            await ItemConfig.objects.bulk_update(items)
            items = await ItemConfig.objects.all()
            assert all(x.pairs == ["1"] for x in items)

            items = await ItemConfig.objects.filter(ItemConfig.id > 1).all()
            for item in items:
                item.pairs = {"b": 2}
            await ItemConfig.objects.bulk_update(items)
            items = await ItemConfig.objects.filter(ItemConfig.id > 1).all()
            assert all(x.pairs == {"b": 2} for x in items)

            table = ItemConfig.ormar_config.table
            query = table.select().where(table.c.pairs["b"].as_integer() == 2)
            res = [
                ItemConfig.from_row(record, source_model=ItemConfig)
                for record in list(
                    (await transaction._connection.execute(query)).mappings().all()
                )
            ]
            assert len(res) == 2


@pytest.mark.asyncio
async def test_custom_queryset_cls():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with pytest.raises(ValueError):
                await Customer.objects.first_or_404(id=1)

            await Customer(name="test").save()
            c = await Customer.objects.first_or_404(name="test")
            assert c.name == "test"


@pytest.mark.asyncio
async def test_filter_enum():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            it = ItemConfig(item_id="test_1")
            await it.save()

            it = await ItemConfig.objects.filter(size=MySize.SMALL).first()
            assert it
