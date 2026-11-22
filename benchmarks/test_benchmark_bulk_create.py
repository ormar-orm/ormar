import random
import string

import pytest

from benchmarks.conftest import Author

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("num_models", [10, 20, 40])
async def test_making_and_inserting_models_in_bulk(aio_benchmark, num_models: int):
    @aio_benchmark
    async def make_and_insert(num_models: int):
        authors = [
            Author(
                name="".join(random.sample(string.ascii_letters, 5)),
                score=int(random.random() * 100),
            )
            for i in range(0, num_models)
        ]
        assert len(authors) == num_models

        await Author.objects.bulk_create(authors)

    make_and_insert(num_models)


@pytest.mark.parametrize("num_models", [10, 20, 40])
async def test_inserting_dicts_in_bulk(aio_benchmark, num_models: int):
    @aio_benchmark
    async def insert_dicts(num_models: int):
        rows = [
            {
                "name": "".join(random.sample(string.ascii_letters, 5)),
                "score": int(random.random() * 100),
            }
            for index in range(0, num_models)
        ]
        await Author.objects.bulk_create(rows)

    insert_dicts(num_models)


@pytest.mark.parametrize("source", ["models", "dicts"])
async def test_upserting_in_bulk(aio_benchmark, source: str):
    @aio_benchmark
    async def upsert(source: str):
        rows = [
            {"id": index + 1, "name": f"author {index}", "score": index % 100}
            for index in range(120)
        ]
        objects = rows if source == "dicts" else [Author(**row) for row in rows]
        await Author.objects.bulk_create(
            objects, on_conflict="update", update_values={"score": 1}
        )

    upsert(source)
