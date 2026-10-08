import asyncio
import random
import string
from datetime import datetime
from decimal import Decimal
from typing import Optional

import nest_asyncio
import pytest
import pytest_asyncio
from tests.lifespan import init_tests
from tests.settings import create_config

import ormar

base_ormar_config = create_config()
nest_asyncio.apply()
pytestmark = pytest.mark.asyncio


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    score: float = ormar.Integer(minimum=0, maximum=100)


class AuthorWithManyFields(Author):
    year_born: int = ormar.Integer()
    year_died: int = ormar.Integer(nullable=True)
    birthplace: str = ormar.String(max_length=255)


class Publisher(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="publishers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    prestige: int = ormar.Integer(minimum=0, maximum=10)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="books")

    id: int = ormar.Integer(primary_key=True)
    author: Author = ormar.ForeignKey(Author, index=True)
    publisher: Publisher = ormar.ForeignKey(Publisher, index=True)
    title: str = ormar.String(max_length=100)
    year: int = ormar.Integer(nullable=True)


DEFAULT_TEXT = "Moo,Foo,Baa,Waa,Moo,Foo,Baa,Waa,Moo,Foo,Baa,Waa"
DEFAULT_JSON = {"a": 1, "b": "b", "c": [2], "d": {"e": 3}, "f": True}


class WideRecord(ormar.Model):
    """Model with 36 columns of mixed types, half with defaults, half nullable."""

    ormar_config = base_ormar_config.copy(tablename="wide_records")

    id: int = ormar.Integer(primary_key=True)
    timestamp: datetime = ormar.DateTime(default=datetime.now)
    level: int = ormar.SmallInteger(index=True)
    text: str = ormar.String(max_length=255, index=True)

    col_float1: float = ormar.Float(default=2.2)
    col_smallint1: int = ormar.SmallInteger(default=2)
    col_int1: int = ormar.Integer(default=2000000)
    col_bigint1: int = ormar.BigInteger(default=99999999)
    col_char1: str = ormar.String(max_length=255, default="value1")
    col_text1: str = ormar.Text(default=DEFAULT_TEXT)
    col_decimal1: Decimal = ormar.Decimal(
        max_digits=12, decimal_places=8, default=Decimal("2.2")
    )
    col_json1: dict = ormar.JSON(default=DEFAULT_JSON)

    col_float2: Optional[float] = ormar.Float(nullable=True)
    col_smallint2: Optional[int] = ormar.SmallInteger(nullable=True)
    col_int2: Optional[int] = ormar.Integer(nullable=True)
    col_bigint2: Optional[int] = ormar.BigInteger(nullable=True)
    col_char2: Optional[str] = ormar.String(max_length=255, nullable=True)
    col_text2: Optional[str] = ormar.Text(nullable=True)
    col_decimal2: Optional[Decimal] = ormar.Decimal(
        max_digits=12, decimal_places=8, nullable=True
    )
    col_json2: Optional[dict] = ormar.JSON(nullable=True)

    col_float3: float = ormar.Float(default=2.2)
    col_smallint3: int = ormar.SmallInteger(default=2)
    col_int3: int = ormar.Integer(default=2000000)
    col_bigint3: int = ormar.BigInteger(default=99999999)
    col_char3: str = ormar.String(max_length=255, default="value1")
    col_text3: str = ormar.Text(default=DEFAULT_TEXT)
    col_decimal3: Decimal = ormar.Decimal(
        max_digits=12, decimal_places=8, default=Decimal("2.2")
    )
    col_json3: dict = ormar.JSON(default=DEFAULT_JSON)

    col_float4: Optional[float] = ormar.Float(nullable=True)
    col_smallint4: Optional[int] = ormar.SmallInteger(nullable=True)
    col_int4: Optional[int] = ormar.Integer(nullable=True)
    col_bigint4: Optional[int] = ormar.BigInteger(nullable=True)
    col_char4: Optional[str] = ormar.String(max_length=255, nullable=True)
    col_text4: Optional[str] = ormar.Text(nullable=True)
    col_decimal4: Optional[Decimal] = ormar.Decimal(
        max_digits=12, decimal_places=8, nullable=True
    )
    col_json4: Optional[dict] = ormar.JSON(nullable=True)


class Document(ormar.Model):
    """Model holding a JSON document of a few kilobytes."""

    ormar_config = base_ormar_config.copy(tablename="documents")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    payload: dict = ormar.JSON()


def make_document_payload() -> dict:
    """
    Builds a JSON document of roughly 5KB.

    :return: nested dictionary with a list of 100 items
    :rtype: dict
    """
    return {
        "items": [
            {"id": i, "name": f"item {i}", "tags": ["a", "b", "c"], "score": i * 1.5}
            for i in range(100)
        ]
    }


create_test_database = init_tests(base_ormar_config, scope="function")


@pytest_asyncio.fixture(autouse=True, scope="function")
async def connect_database(create_test_database):
    if not base_ormar_config.database.is_connected:
        await base_ormar_config.database.connect()

    yield

    if base_ormar_config.database.is_connected:
        await base_ormar_config.database.disconnect()


@pytest_asyncio.fixture
async def author():
    author = await Author(name="Author", score=10).save()
    return author


@pytest_asyncio.fixture
async def publisher():
    publisher = await Publisher(name="Publisher", prestige=random.randint(0, 10)).save()
    return publisher


@pytest_asyncio.fixture
async def authors_in_db(num_models: int):
    authors = [
        Author(
            name="".join(random.sample(string.ascii_letters, 5)),
            score=int(random.random() * 100),
        )
        for i in range(0, num_models)
    ]
    await Author.objects.bulk_create(authors)
    return await Author.objects.all()


@pytest_asyncio.fixture
async def wide_records_in_db(num_models: int):
    records = [
        WideRecord(level=random.choice([10, 20, 30]), text=f"record {i}")
        for i in range(0, num_models)
    ]
    await WideRecord.objects.bulk_create(records)
    return await WideRecord.objects.all()


@pytest_asyncio.fixture
async def documents_in_db(num_models: int):
    documents = [
        Document(title=f"document {i}", payload=make_document_payload())
        for i in range(0, num_models)
    ]
    await Document.objects.bulk_create(documents)
    return await Document.objects.all()


@pytest_asyncio.fixture
async def aio_benchmark(benchmark):
    def _fixture_wrapper(func):
        def _func_wrapper(*args, **kwargs):
            if asyncio.iscoroutinefunction(func):
                # Get the running event loop instead of requesting it as a fixture
                loop = asyncio.get_running_loop()

                @benchmark
                def benchmarked_func():
                    a = loop.run_until_complete(func(*args, **kwargs))
                    return a

                return benchmarked_func
            else:
                return benchmark(func, *args, **kwargs)

        return _func_wrapper

    return _fixture_wrapper
