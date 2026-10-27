from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import QueryDefinitionError
from ormar.models.descriptors import PkDescriptor
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Order(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cq_orders")

    tenant_id: int = ormar.Integer(primary_key=True)
    number: int = ormar.Integer(primary_key=True, name="order_no")
    note: Optional[str] = ormar.String(max_length=50, nullable=True)


class Shop(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cq_shops")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Stock(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cq_stock")

    shop_code: str = ormar.String(max_length=10, primary_key=True)
    sku: str = ormar.String(max_length=10, primary_key=True)
    shop: Optional[Shop] = ormar.ForeignKey(Shop, related_name="stock")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def clean():
    async with base_ormar_config.database:
        for model in (Stock, Shop, Order):
            await model.objects.delete(each=True)
        yield


async def seed() -> None:
    await Order.objects.bulk_create(
        [
            Order(tenant_id=1, number=7, note="a"),
            Order(tenant_id=1, number=8, note="b"),
            Order(tenant_id=2, number=7, note="c"),
        ]
    )


@pytest.mark.asyncio
async def test_get_and_filter_by_composite_pk():
    await seed()
    assert (await Order.objects.get(pk=(1, 8))).note == "b"
    assert (await Order.objects.get(pk={"tenant_id": 2, "number": 7})).note == "c"
    found = await Order.objects.filter(pk__in=[(1, 7), (2, 7)]).order_by("note").all()
    assert [o.note for o in found] == ["a", "c"]
    assert [o.note for o in await Order.objects.exclude(pk=(1, 7)).all()] == [
        "b",
        "c",
    ]
    assert await Order.objects.filter(pk=(9, 9)).exists() is False
    assert await Order.objects.filter(pk__in=[]).all() == []


@pytest.mark.asyncio
async def test_order_count_first_last_iterate():
    await seed()
    assert [o.pk for o in await Order.objects.order_by("-pk").all()] == [
        (2, 7),
        (1, 8),
        (1, 7),
    ]
    assert await Order.objects.count() == 3
    assert (await Order.objects.first()).pk == (1, 7)
    assert (await Order.objects.last()).pk == (2, 7)
    assert [o.pk async for o in Order.objects.iterate()] == [(1, 7), (1, 8), (2, 7)]


@pytest.mark.asyncio
async def test_bulk_update_and_create_helpers():
    await seed()
    orders = await Order.objects.all()
    for order in orders:
        order.note = order.note.upper()
    await Order.objects.bulk_update(orders)
    assert [o.note for o in await Order.objects.all()] == ["A", "B", "C"]
    created, was_created = await Order.objects.get_or_create(tenant_id=3, number=1)
    assert was_created and created.pk == (3, 1)
    updated = await Order.objects.update_or_create(pk=(3, 1), note="z")
    assert (await Order.objects.get(pk=(3, 1))).note == updated.note == "z"
    await Order.objects.filter(pk=(3, 1)).update(note="y")
    assert (await Order.objects.get(pk=(3, 1))).note == "y"
    await Order.objects.delete(pk=(3, 1))
    assert await Order.objects.count() == 3


@pytest.mark.asyncio
async def test_values_return_key_fields():
    await seed()
    assert await Order.objects.order_by("pk").values_list(["tenant_id", "note"]) == [
        (1, "a"),
        (1, "b"),
        (2, "c"),
    ]
    first = (await Order.objects.order_by("pk").values())[0]
    assert (first["tenant_id"], first["note"]) == (1, "a")


@pytest.mark.asyncio
async def test_composite_key_model_with_fk_to_single_key_model():
    shop = await Shop.objects.create(name="s")
    await Stock.objects.create(shop_code="A", sku="1", shop=shop)
    await Stock.objects.create(shop_code="A", sku="2", shop=shop)
    stock = await Stock.objects.select_related("shop").get(pk=("A", "2"))
    assert stock.shop.name == "s"
    loaded = await Shop.objects.select_related("stock").get()
    assert sorted(s.pk for s in loaded.stock) == [("A", "1"), ("A", "2")]
    fetched = await Shop.objects.prefetch_related("stock").get()
    assert sorted(s.pk for s in fetched.stock) == [("A", "1"), ("A", "2")]
    assert len(await shop.stock.all()) == 2


@pytest.mark.asyncio
async def test_iterate_and_all_dedupe_by_composite_key():
    shop = await Shop.objects.create(name="s")
    await Stock.objects.create(shop_code="A", sku="1", shop=shop)
    await Stock.objects.create(shop_code="B", sku="1", shop=shop)
    shops = await Shop.objects.select_related("stock").all()
    assert len(shops) == 1
    assert len(shops[0].stock) == 2


@pytest.mark.asyncio
async def test_invalid_composite_pk_filters_raise():
    with pytest.raises(QueryDefinitionError, match="primary key has fields"):
        await Order.objects.get(pk=(1,))
    with pytest.raises(QueryDefinitionError, match="primary key has fields"):
        await Order.objects.filter(pk__in=[(1, 7, 3)]).all()
    with pytest.raises(QueryDefinitionError):
        await Order.objects.filter(pk__gt=(1, 7)).all()
    assert isinstance(Order.pk, PkDescriptor)


@pytest.mark.asyncio
async def test_single_key_pk_lookups_and_order():
    for name in "abc":
        await Shop.objects.create(name=name)
    ids = [s.id for s in await Shop.objects.all()]
    assert [s.name for s in await Shop.objects.filter(pk__in=ids[:2]).all()] == [
        "a",
        "b",
    ]
    assert [s.name for s in await Shop.objects.filter(pk__gt=ids[0]).all()] == [
        "b",
        "c",
    ]
    assert [s.name for s in await Shop.objects.order_by("-pk").all()] == [
        "c",
        "b",
        "a",
    ]
