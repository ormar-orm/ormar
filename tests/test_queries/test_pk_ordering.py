from typing import Optional

import pytest
import pytest_asyncio

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Shop(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="po_shops", order_by=["-pk"])

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Stock(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="po_stock", order_by=["-pk"])

    code: str = ormar.String(max_length=10, primary_key=True)
    sku: str = ormar.String(max_length=10, primary_key=True)
    shop: Optional[Shop] = ormar.ForeignKey(
        Shop, related_name="stock", related_orders_by=["-pk"]
    )


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="po_tags")

    id: int = ormar.Integer(primary_key=True)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="po_posts")

    id: int = ormar.Integer(primary_key=True)
    tags: Optional[list[Tag]] = ormar.ManyToMany(Tag, orders_by=["-pk"])


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def seed():
    async with base_ormar_config.database:
        for model in (Post, Tag, Stock, Shop):
            await model.objects.delete(each=True)
        first = await Shop.objects.create(id=1, name="first")
        second = await Shop.objects.create(id=2, name="second")
        await Stock.objects.create(code="A", sku="1", shop=first)
        await Stock.objects.create(code="A", sku="2", shop=first)
        await Stock.objects.create(code="B", sku="1", shop=second)
        post = await Post.objects.create(id=1)
        for tag_id in (1, 2, 3):
            await post.tags.add(await Tag.objects.create(id=tag_id))
        yield


def stock_keys(shop: Shop) -> list[tuple[str, str]]:
    """
    Keys of the stock loaded on a shop, in loaded order.

    :param shop: shop with stock loaded
    :type shop: Shop
    :return: stock keys
    :rtype: list[tuple[str, str]]
    """
    return [item.pk for item in shop.stock]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "order, shop_names, first_shop_keys",
    [
        ("stock__pk", ["first", "second"], [("A", "1"), ("A", "2")]),
        ("-stock__pk", ["second", "first"], [("A", "2"), ("A", "1")]),
    ],
)
async def test_order_by_composite_pk_through_reverse_relation(
    order, shop_names, first_shop_keys
):
    shops = await Shop.objects.select_related("stock").order_by(order).all()
    assert [shop.name for shop in shops] == shop_names
    assert stock_keys(shops[shop_names.index("first")]) == first_shop_keys


@pytest.mark.asyncio
async def test_order_by_composite_pk_accessor_through_reverse_relation():
    shops = (
        await Shop.objects.select_related("stock").order_by(Shop.stock.pk.desc()).all()
    )
    assert [shop.name for shop in shops] == ["second", "first"]
    assert stock_keys(shops[1]) == [("A", "2"), ("A", "1")]


@pytest.mark.asyncio
async def test_order_by_single_pk_through_relation():
    stock = await Stock.objects.select_related("shop").order_by("shop__pk").all()
    assert [item.shop.id for item in stock] == [1, 1, 2]
    stock = (
        await Stock.objects.select_related("shop").order_by(Stock.shop.pk.desc()).all()
    )
    assert [item.shop.id for item in stock] == [2, 1, 1]


@pytest.mark.asyncio
async def test_order_by_pk_through_many_to_many():
    post = await Post.objects.select_related("tags").order_by("tags__pk").get()
    assert [tag.id for tag in post.tags] == [1, 2, 3]


@pytest.mark.asyncio
async def test_config_order_by_pk():
    assert [shop.id for shop in await Shop.objects.all()] == [2, 1]
    assert [item.pk for item in await Stock.objects.all()] == [
        ("B", "1"),
        ("A", "2"),
        ("A", "1"),
    ]
    assert (await Stock.objects.first()).pk == ("A", "1")
    assert (await Stock.objects.last()).pk == ("B", "1")


@pytest.mark.asyncio
async def test_relation_orders_by_pk():
    shop = await Shop.objects.select_related("stock").get(id=1)
    assert stock_keys(shop) == [("A", "2"), ("A", "1")]
    post = await Post.objects.select_related("tags").get()
    assert [tag.id for tag in post.tags] == [3, 2, 1]
    shop = await Shop.objects.get(id=1)
    assert [item.pk for item in await shop.stock.all()] == [("A", "2"), ("A", "1")]


@pytest.mark.asyncio
async def test_pk_ordering_on_queried_model_and_other_fields_unchanged():
    assert [item.pk for item in await Stock.objects.order_by("pk").all()] == [
        ("A", "1"),
        ("A", "2"),
        ("B", "1"),
    ]
    assert [
        item.pk for item in await Stock.objects.order_by(Stock.pk.desc()).all()
    ] == [
        ("B", "1"),
        ("A", "2"),
        ("A", "1"),
    ]
    shop = await Shop.objects.select_related("stock").order_by("stock__sku").get(id=1)
    assert stock_keys(shop) == [("A", "1"), ("A", "2")]
