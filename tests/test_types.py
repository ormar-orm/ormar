from typing import Optional

import pytest
import sqlalchemy

import ormar
from ormar.databases.connection import DatabaseConnection
from ormar.models.ormar_config import OrmarConfig
from tests.settings import ASYNC_DATABASE_URL, SYNC_DATABASE_URL

database = DatabaseConnection(ASYNC_DATABASE_URL)
metadata = sqlalchemy.MetaData()


class Publisher(ormar.Model):
    ormar_config = OrmarConfig(
        metadata=metadata,
        database=database,
        tablename="publishers",
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Author(ormar.Model):
    ormar_config = OrmarConfig(
        metadata=metadata, database=database, tablename="authors", order_by=["-name"]
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    publishers = ormar.ManyToMany(Publisher)


class Book(ormar.Model):
    ormar_config = OrmarConfig(
        metadata=metadata,
        database=database,
        tablename="books",
        order_by=["year", "-ranking"],
    )

    id: int = ormar.Integer(primary_key=True)
    author = ormar.ForeignKey(Author)
    title: str = ormar.String(max_length=100)
    year: Optional[int] = ormar.Integer(nullable=True)
    ranking: Optional[int] = ormar.Integer(nullable=True)


@pytest.fixture(autouse=True, scope="module")
def create_test_database_for_this_module():
    engine = sqlalchemy.create_engine(SYNC_DATABASE_URL)
    metadata.drop_all(engine)
    metadata.create_all(engine)
    yield
    metadata.drop_all(engine)


def assert_type(book: Book):
    _ = str(book)


@pytest.mark.asyncio
async def test_types() -> None:
    async with database:
        publisher = await Publisher(name="Test publisher").save()
        author = await Author.objects.create(name="Test Author")
        await author.publishers.add(publisher)
        await Author.objects.select_related("publishers").get()
        publisher2 = await Publisher.objects.select_related("authors").get()
        authors = publisher2.authors
        assert authors[0] == author
        for author in authors:
            pass
            # if TYPE_CHECKING:  # pragma: no cover
            #     reveal_type(author)  # iter of relation proxy
        book = await Book.objects.create(title="Test", author=author)
        book2 = await Book.objects.select_related("author").get()
        await Book.objects.select_related("author").all()
        await author.books.all()
        assert book.author.name == "Test Author"
        assert book2.author.name == "Test Author"
        # if TYPE_CHECKING:  # pragma: no cover
        #     reveal_type(publisher)  # model method
        #     reveal_type(publishers)  # many to many
        #     reveal_type(publishers[0])  # item in m2m list
        #     reveal_type(next(p for p in publishers))  # item in m2m iterator
        #     # getting relation without __getattribute__
        #     reveal_type(authors)  # reverse many to many  # TODO: wrong
        #     reveal_type(book2)  # queryset get
        #     reveal_type(books)  # queryset all
        #     reveal_type(book)  # queryset - create
        #     reveal_type(query)  # queryset itself
        #     reveal_type(book.author)  # fk
        #     reveal_type(author.books)  # reverse fk relation proxy  # TODO: wrong
        #     reveal_type(author)  # another test for queryset get different model
        #     reveal_type(book.author.name)  # field on related model
        #     reveal_type(author_books)  # querysetproxy result for fk  # TODO: wrong
        #     reveal_type(author_books[0])  # item in qs proxy for fk  # TODO: wrong
        assert_type(book)
