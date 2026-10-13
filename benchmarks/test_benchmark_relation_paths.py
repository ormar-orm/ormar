import pytest
import pytest_asyncio

from benchmarks.conftest import Author, Book, Publisher, Transfer

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture()
async def transfers(author: Author, publisher: Publisher, num_models: int):
    books = [
        Book(author=author, publisher=publisher, title=f"t{i}", year=i)
        for i in range(num_models)
    ]
    await Book.objects.bulk_create(books)
    books = await Book.objects.all()
    await Transfer.objects.bulk_create(
        [Transfer(from_book=books[i], to_book=books[-i - 1]) for i in range(num_models)]
    )


@pytest.mark.parametrize("num_models", [50, 100, 200])
async def test_duplicate_relation_paths(aio_benchmark, num_models: int, transfers):
    @aio_benchmark
    async def load_transfers():
        return (
            await Transfer.objects.select_related(
                ["from_book__author", "to_book__author"]
            )
            .filter(from_book__author__name__isnull=False)
            .exclude(to_book__author__score__lt=0)
            .order_by(["from_book__author__name", "-to_book__author__name"])
            .all()
        )

    loaded = load_transfers()
    assert len(loaded) == num_models
