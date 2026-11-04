import pickle
import uuid
from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import ModelPersistenceError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pk_items")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Token(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pk_tokens")

    id: uuid.UUID = ormar.UUID(primary_key=True, default=uuid.uuid4)
    name: str = ormar.String(max_length=50)


class Country(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pk_countries")

    iso_code: str = ormar.String(primary_key=True, max_length=2)
    name: str = ormar.String(max_length=50)


class Order(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pk_orders")

    tenant_id: int = ormar.Integer(primary_key=True)
    number: int = ormar.Integer(primary_key=True)
    note: Optional[str] = ormar.String(max_length=50, nullable=True)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def clean():
    async with base_ormar_config.database:
        for model in (Book, Author, Item, Token, Country, Order):
            await model.objects.delete(each=True)
        yield


@pytest.mark.asyncio
async def test_update_after_key_change_moves_the_loaded_row():
    item = await Item.objects.create(id=1, name="a")
    item.id = 5
    await item.update()
    assert [i.id for i in await Item.objects.all()] == [5]
    fetched = await Item.objects.get(id=5)
    fetched.id = 7
    await fetched.update(name="b")
    assert [(i.id, i.name) for i in await Item.objects.all()] == [(7, "b")]


@pytest.mark.asyncio
async def test_load_after_key_change_reads_the_persisted_row():
    item = await Item.objects.create(id=2, name="a")
    item.id = 9
    await item.load()
    assert (item.id, item.name) == (2, "a")


@pytest.mark.asyncio
async def test_delete_after_key_change_deletes_the_persisted_row():
    item = await Item.objects.create(id=3, name="a")
    item.id = 4
    await item.delete()
    assert await Item.objects.count() == 0


@pytest.mark.asyncio
async def test_upsert_inserts_never_saved_instance_with_preset_key():
    token = await Token(name="t").upsert()
    assert await Token.objects.get(id=token.id)
    country = await Country(iso_code="PL", name="Poland").upsert()
    assert country.saved
    assert (await Country.objects.get(iso_code="PL")).name == "Poland"


@pytest.mark.asyncio
async def test_upsert_updates_never_saved_instance_with_existing_key():
    await Country.objects.create(iso_code="DE", name="old")
    await Country(iso_code="DE", name="Germany").upsert()
    assert [(c.iso_code, c.name) for c in await Country.objects.all()] == [
        ("DE", "Germany")
    ]
    await Country(iso_code="DE", name="Deutschland").upsert(__force_save__=True)
    assert (await Country.objects.get(iso_code="DE")).name == "Deutschland"


@pytest.mark.asyncio
async def test_upsert_of_persisted_instance_updates_without_lookup():
    item = await Item.objects.create(id=11, name="a")
    item.name = "b"
    await item.upsert()
    assert (await Item.objects.get(id=11)).name == "b"


@pytest.mark.asyncio
async def test_delete_then_save_reinserts():
    country = await Country.objects.create(iso_code="FR", name="France")
    await country.delete()
    await country.save()
    assert await Country.objects.count() == 1


@pytest.mark.asyncio
async def test_composite_key_change_and_missing_part():
    order = await Order.objects.create(tenant_id=1, number=7)
    order.number = 8
    await order.update(note="moved")
    assert [(o.tenant_id, o.number, o.note) for o in await Order.objects.all()] == [
        (1, 8, "moved")
    ]
    with pytest.raises(ModelPersistenceError, match="number"):
        await Order(tenant_id=1).save()


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pk_authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pk_books")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=50)
    author: Optional[Author] = ormar.ForeignKey(Author, related_name="books")


@pytest.mark.asyncio
async def test_bulk_created_rows_without_returned_keys_are_not_persisted():
    items = [Item(name="x"), Item(name="y")]
    await Item.objects.bulk_create(items)
    assert all(item.persisted is (item.pk is not None) for item in items)
    with pytest.raises(ModelPersistenceError, match="number"):
        await Order.objects.bulk_create([Order(tenant_id=1)])


@pytest.mark.asyncio
async def test_bulk_update_moves_changed_keys():
    await Order.objects.bulk_create(
        [Order(tenant_id=1, number=1), Order(tenant_id=1, number=2)]
    )
    orders = await Order.objects.order_by("pk").all()
    orders[0].number = 50
    orders[1].note = "kept"
    await Order.objects.bulk_update(orders)
    rows = [(o.number, o.note) for o in await Order.objects.order_by("pk").all()]
    assert rows == [(2, "kept"), (50, None)]
    orders[0].note = "after"
    await orders[0].update()
    assert (await Order.objects.get(pk=(1, 50))).note == "after"


@pytest.mark.asyncio
async def test_prefetched_and_unpickled_rows_track_their_key():
    async with base_ormar_config.database:
        author = await Author.objects.create(id=1, name="a")
        await Book.objects.create(id=10, title="t", author=author)
        loaded = await Author.objects.prefetch_related("books").get()
        book = loaded.books[0]
        book.id = 11
        await book.update(title="moved")
        assert [(b.id, b.title) for b in await Book.objects.all()] == [(11, "moved")]
        await Item.objects.create(id=31, name="p")
        restored = pickle.loads(pickle.dumps(await Item.objects.get(id=31)))
        restored.id = 32
        await restored.update()
        assert [i.id for i in await Item.objects.all()] == [32]


@pytest.mark.asyncio
async def test_load_all_after_key_change_reads_the_persisted_row():
    item = await Item.objects.create(id=21, name="a")
    item.id = 22
    await item.load_all()
    assert (item.id, item.name) == (21, "a")
