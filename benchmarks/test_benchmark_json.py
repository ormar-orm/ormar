import pytest

from benchmarks.conftest import Document, make_document_payload

pytestmark = pytest.mark.asyncio


async def get_all_documents() -> list[Document]:
    """
    Loads all documents with their JSON payloads.

    :return: loaded documents
    :rtype: list[Document]
    """
    return await Document.objects.all()


async def get_documents_page(limit: int) -> list[Document]:
    """
    Loads a page of documents with their JSON payloads.

    :param limit: number of documents to load
    :type limit: int
    :return: loaded documents
    :rtype: list[Document]
    """
    return await Document.objects.limit(limit).all()


@pytest.mark.parametrize("num_models", [250, 500, 1000])
async def test_get_all_documents_with_json(
    aio_benchmark, num_models: int, documents_in_db: list[Document]
):
    documents = aio_benchmark(get_all_documents)()
    assert len(documents) == num_models
    assert documents[0].payload == make_document_payload()


@pytest.mark.parametrize("num_models", [250])
async def test_get_documents_page_with_json(
    aio_benchmark, num_models: int, documents_in_db: list[Document]
):
    documents = aio_benchmark(get_documents_page)(20)
    assert len(documents) == 20
    assert documents[0].payload == make_document_payload()
