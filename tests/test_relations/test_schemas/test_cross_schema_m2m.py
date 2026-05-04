"""Many-to-many across schemas, including auto-generated through models."""

from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

from .dialect import requires_schemas

pytestmark = requires_schemas

base_ormar_config = create_config()


class M2MAuthor(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="authors", schema="people")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class M2MBook(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="books", schema="library")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    authors: Optional[list[M2MAuthor]] = ormar.ManyToMany(M2MAuthor)


create_test_database = init_tests(base_ormar_config)


def test_auto_through_model_inherits_owner_schema():  # pragma: no cover
    book_field = M2MBook.ormar_config.model_fields["authors"]
    through = book_field.through
    assert through.ormar_config.schema == "library"
    assert through.ormar_config.table.schema == "library"


@pytest.mark.asyncio
async def test_m2m_save_and_prefetch_across_schemas():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await M2MAuthor.objects.create(name="Alice")
        bob = await M2MAuthor.objects.create(name="Bob")
        book = await M2MBook.objects.create(title="Coauthored")

        await book.authors.add(alice)
        await book.authors.add(bob)

        prefetched = await M2MBook.objects.prefetch_related("authors").get(id=book.id)
        names = sorted(a.name for a in prefetched.authors)
        assert names == ["Alice", "Bob"]


@pytest.mark.asyncio
async def test_m2m_select_related_across_schemas():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await M2MAuthor.objects.create(name="Alice")
        bob = await M2MAuthor.objects.create(name="Bob")
        book = await M2MBook.objects.create(title="Coauthored")
        await book.authors.add(alice)
        await book.authors.add(bob)

        loaded = await M2MBook.objects.select_related("authors").get(id=book.id)
        names = sorted(a.name for a in loaded.authors)
        assert names == ["Alice", "Bob"]


@pytest.mark.asyncio
async def test_m2m_filter_through_implicit_join():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await M2MAuthor.objects.create(name="Alice")
        bob = await M2MAuthor.objects.create(name="Bob")
        b1 = await M2MBook.objects.create(title="Solo Alice")
        b2 = await M2MBook.objects.create(title="Solo Bob")
        await b1.authors.add(alice)
        await b2.authors.add(bob)

        # Implicit join from M2MBook through the m2m relation to M2MAuthor —
        # exercises the cross-schema through table.
        alice_books = await M2MBook.objects.filter(authors__name="Alice").all()
        assert [b.title for b in alice_books] == ["Solo Alice"]


@pytest.mark.asyncio
async def test_m2m_filter_via_reverse_relation():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await M2MAuthor.objects.create(name="Alice")
        bob = await M2MAuthor.objects.create(name="Bob")
        b1 = await M2MBook.objects.create(title="Hit")
        b2 = await M2MBook.objects.create(title="Miss")
        await b1.authors.add(alice)
        await b2.authors.add(bob)

        # Reverse side: filter authors by the books they wrote.
        hit_authors = await M2MAuthor.objects.filter(m2mbooks__title="Hit").all()
        assert [a.name for a in hit_authors] == ["Alice"]


@pytest.mark.asyncio
async def test_m2m_reverse_prefetch_across_schemas():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        alice = await M2MAuthor.objects.create(name="Alice")
        bob = await M2MAuthor.objects.create(name="Bob")
        b1 = await M2MBook.objects.create(title="A1")
        b2 = await M2MBook.objects.create(title="A2")
        b3 = await M2MBook.objects.create(title="B1")
        await b1.authors.add(alice)
        await b2.authors.add(alice)
        await b3.authors.add(bob)

        authors = (
            await M2MAuthor.objects.prefetch_related("m2mbooks").order_by("name").all()
        )
        assert {b.title for b in authors[0].m2mbooks} == {"A1", "A2"}
        assert {b.title for b in authors[1].m2mbooks} == {"B1"}
