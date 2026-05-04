import asyncio
import random
import string

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
