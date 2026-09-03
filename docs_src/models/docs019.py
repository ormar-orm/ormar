import ormar
import sqlalchemy
from ormar import DatabaseConnection

database = DatabaseConnection("sqlite+aiosqlite:///db.sqlite")
metadata = sqlalchemy.MetaData()


class Author(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="authors",
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Book(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="books",
    )

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    author: Author = ormar.ForeignKey(Author)
