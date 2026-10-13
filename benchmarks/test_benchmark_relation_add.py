import random
import string

import pytest

from benchmarks.conftest import Author, Book

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("num_models", [10, 20, 40])
async def test_adding_new_children_to_reverse_relation(
    aio_benchmark, num_models: int, author: Author
):
    @aio_benchmark
    async def add_new_books(author: Author, num_models: int):
        books = [
            Book(
                title="".join(random.sample(string.ascii_letters, 5)),
                year=random.randint(0, 2000),
            )
            for i in range(0, num_models)
        ]
        for book in books:
            await author.books.add(book)
        return books

    books = add_new_books(author=author, num_models=num_models)
    for book in books:
        assert book.pk is not None


@pytest.mark.parametrize("num_models", [10, 20, 40])
async def test_adding_saved_children_to_reverse_relation(
    aio_benchmark, num_models: int, author: Author
):
    books = [
        await Book(
            title="".join(random.sample(string.ascii_letters, 5)),
            year=random.randint(0, 2000),
        ).save()
        for i in range(0, num_models)
    ]

    @aio_benchmark
    async def add_saved_books(author: Author, books: list[Book]):
        for book in books:
            await author.books.add(book)
        return books

    books = add_saved_books(author=author, books=books)
    for book in books:
        assert book.author.pk == author.pk


@pytest.mark.parametrize("num_models", [10, 20, 40])
async def test_saving_related_new_children(aio_benchmark, num_models: int):
    @aio_benchmark
    async def save_related_new_books(num_models: int):
        author = await Author(name="Author", score=10).save()
        books = [
            Book(
                author=author,
                title="".join(random.sample(string.ascii_letters, 5)),
                year=random.randint(0, 2000),
            )
            for i in range(0, num_models)
        ]
        await author.save_related()
        return books

    books = save_related_new_books(num_models=num_models)
    for book in books:
        assert book.pk is not None
