from typing import Optional

import pytest
import pytest_asyncio
import sqlalchemy

import ormar
from ormar.exceptions import ModelDefinitionError, QueryDefinitionError
from tests import cross_check
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Order(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_orders")

    tenant_id: int = ormar.Integer(primary_key=True)
    number: int = ormar.Integer(primary_key=True, name="order_no")
    note: Optional[str] = ormar.String(max_length=50, nullable=True)


class OrderLine(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_lines")

    id: int = ormar.Integer(primary_key=True)
    qty: int = ormar.Integer(default=1)
    order: Optional[Order] = ormar.ForeignKey(
        Order,
        columns={"tenant_id": "ord_tenant", "order_no": "ord_no"},
        related_name="lines",
    )


class Shipment(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_shipments")

    id: int = ormar.Integer(primary_key=True)
    order: Optional[Order] = ormar.ForeignKey(Order, related_name="shipments")


class Product(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="cfk_products",
        constraints=[ormar.UniqueColumns("tenant_id", "sku")],
    )

    id: int = ormar.Integer(primary_key=True)
    tenant_id: int = ormar.Integer()
    sku: str = ormar.String(max_length=10)


class Stock(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_stock")

    id: int = ormar.Integer(primary_key=True)
    amount: int = ormar.Integer(default=0)
    product: Optional[Product] = ormar.ForeignKey(
        Product,
        to_field=["tenant_id", "sku"],
        columns={"tenant_id": "prod_tenant", "sku": "prod_sku"},
        related_name="stock",
    )


class Region(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_regions")

    id: int = ormar.Integer(primary_key=True)
    code: str = ormar.String(max_length=5, unique=True)


class City(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_cities")

    id: int = ormar.Integer(primary_key=True)
    region: Optional[Region] = ormar.ForeignKey(Region, to_field="code")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def data():
    async with base_ormar_config.database:
        for model in (OrderLine, Shipment, Stock, City, Order, Product, Region):
            await model.objects.delete(each=True)
        first = await Order.objects.create(tenant_id=1, number=7, note="first")
        second = await Order.objects.create(tenant_id=1, number=8, note="second")
        yield first, second


def composite_fks(table) -> set:
    return {
        (
            tuple(c.name for c in fk.columns),
            tuple(e.target_fullname for e in fk.elements),
        )
        for fk in table.foreign_key_constraints
    }


def test_composite_foreign_key_ddl():
    lines = OrderLine.ormar_config.table
    assert {"ord_tenant", "ord_no"} <= set(lines.c.keys())
    assert "order" not in lines.c
    assert composite_fks(lines) == {
        (("ord_tenant", "ord_no"), ("cfk_orders.tenant_id", "cfk_orders.order_no"))
    }
    shipments = Shipment.ormar_config.table
    assert {"order_tenant_id", "order_order_no"} <= set(shipments.c.keys())
    assert composite_fks(Stock.ormar_config.table) == {
        (("prod_tenant", "prod_sku"), ("cfk_products.tenant_id", "cfk_products.sku"))
    }
    assert lines.c.ord_tenant.type._type_affinity is sqlalchemy.Integer


def test_composite_foreign_key_definition_errors():
    with pytest.raises(ModelDefinitionError, match="columns"):

        class BadColumns(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="cfk_bad_columns")

            id: int = ormar.Integer(primary_key=True)
            order: Optional[Order] = ormar.ForeignKey(
                Order, columns={"tenant_id": "x"}, related_name="bad_columns"
            )

    with pytest.raises(ModelDefinitionError, match="columns="):

        class BadName(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="cfk_bad_name")

            id: int = ormar.Integer(primary_key=True)
            order: Optional[Order] = ormar.ForeignKey(
                Order, name="x", related_name="bad_name"
            )

    with pytest.raises(ModelDefinitionError, match="not both"):

        class BadBoth(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="cfk_bad_both")

            id: int = ormar.Integer(primary_key=True)
            order: Optional[Order] = ormar.ForeignKey(
                Order,
                name="x",
                columns={"tenant_id": "a", "order_no": "b"},
                related_name="bad_both",
            )

    with pytest.raises(ModelDefinitionError, match="primary key"):

        class BadKey(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="cfk_bad_key")

            order: Order = ormar.ForeignKey(
                Order, primary_key=True, related_name="bad_key"
            )

    with pytest.raises(ModelDefinitionError, match="unique"):

        class BadToField(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="cfk_bad_to_field")

            id: int = ormar.Integer(primary_key=True)
            product: Optional[Product] = ormar.ForeignKey(
                Product, to_field=["sku", "id"], related_name="bad_to_field"
            )


@pytest.mark.asyncio
async def test_composite_foreign_key_writes_and_reads(data, monkeypatch):
    monkeypatch.setattr(cross_check, "ENABLED", False)
    first, second = data
    await OrderLine.objects.create(order=first, qty=1)
    await OrderLine.objects.create(order={"tenant_id": 1, "number": 7}, qty=2)
    await OrderLine.objects.create(order=(1, 8), qty=3)
    lines = await OrderLine.objects.order_by("qty").all()
    assert [line.order.pk for line in lines] == [(1, 7), (1, 7), (1, 8)]
    await lines[0].order.load()
    assert lines[0].order.note == "first"
    joined = await OrderLine.objects.select_related("order").order_by("qty").all()
    assert [line.order.note for line in joined] == ["first", "first", "second"]
    loaded = await Order.objects.select_related("lines").get(pk=(1, 7))
    assert sorted(line.qty for line in loaded.lines) == [1, 2]
    fetched = await Order.objects.prefetch_related("lines").get(pk=(1, 8))
    assert [line.qty for line in fetched.lines] == [3]
    prefetched = await OrderLine.objects.prefetch_related("order").order_by("qty").all()
    assert [line.order.note for line in prefetched] == ["first", "first", "second"]
    assert await first.lines.count() == 2
    assert len(await OrderLine.objects.filter(order=first).all()) == 2
    assert len(await OrderLine.objects.filter(order=(1, 8)).all()) == 1
    assert len(await OrderLine.objects.filter(order__note="second").all()) == 1
    line = lines[2]
    line.order = first
    await line.update()
    assert await first.lines.count() == 3
    await OrderLine.objects.filter(qty=3).update(order=second)
    assert await second.lines.count() == 1
    await second.lines.create(qty=9)
    assert await second.lines.count() == 2


@pytest.mark.asyncio
async def test_composite_foreign_key_bulk_and_nullable(data):
    first, second = data
    await OrderLine.objects.bulk_create(
        [OrderLine(order=first, qty=4), OrderLine(order=second, qty=5)]
    )
    lines = await OrderLine.objects.order_by("qty").all()
    lines[0].order = second
    await OrderLine.objects.bulk_update(lines)
    assert [
        line.order.pk for line in await OrderLine.objects.order_by("qty").all()
    ] == [
        (1, 8),
        (1, 8),
    ]
    shipment = await Shipment.objects.create()
    assert (await Shipment.objects.get(id=shipment.id)).order is None
    assert await Shipment.objects.filter(order__isnull=True).count() == 1
    assert await Shipment.objects.filter(order=None).count() == 1
    assert (await Shipment.objects.values_list(["id", "order"])) == [
        (shipment.id, None)
    ]
    with pytest.raises(QueryDefinitionError, match="several columns"):
        await Shipment.objects.filter(order__gt=(1, 7)).all()
    await Shipment.objects.create(order=first)
    assert (
        await Shipment.objects.select_related("order").filter(order__isnull=False).get()
    ).order.note == "first"


@pytest.mark.asyncio
async def test_values_and_serialization_of_composite_foreign_keys(data):
    first, _ = data
    await OrderLine.objects.create(order=first, qty=1)
    assert (await OrderLine.objects.values(["qty", "order"]))[0]["order"] == {
        "tenant_id": 1,
        "number": 7,
    }
    assert (await OrderLine.objects.values_list(["qty", "order"]))[0] == (1, (1, 7))
    line = await OrderLine.objects.select_related("order").get()
    assert line.model_dump()["order"]["note"] == "first"
    flat = OrderLine.get_pydantic(flatten_fields={"order"})
    assert flat(**line.model_dump()).order == {"tenant_id": 1, "number": 7}
    assert flat(qty=1, order={"tenant_id": 1, "number": 8}).order == {
        "tenant_id": 1,
        "number": 8,
    }
    assert flat(qty=1, order=first).order == {"tenant_id": 1, "number": 7}
    assert flat(qty=1, order=(1, 8)).order == {"tenant_id": 1, "number": 8}
    assert OrderLine.get_column_aliases(field_name="order") == ("ord_tenant", "ord_no")
    assert OrderLine.get_column_aliases(field_name="ord_no") == ("ord_no",)
    dumped = line.model_dump(flatten_fields={"order"})
    assert dumped["order"] == {"tenant_id": 1, "number": 7}
    assert line.model_dump_json(flatten_fields={"order"}).count('"number":7') == 1
    loaded = await Order.objects.select_related("lines").get(pk=(1, 7))
    assert loaded.model_dump(flatten_fields={"lines"})["lines"] == [line.id]


@pytest.mark.asyncio
async def test_flattened_to_field_relation_is_the_referenced_value(data):
    region = await Region.objects.create(code="EU")
    await City.objects.create(region=region)
    city = await City.objects.select_related("region").get()
    assert city.model_dump(flatten_fields={"region"})["region"] == "EU"
    flat = City.get_pydantic(flatten_fields={"region"})
    assert flat(**city.model_dump()).region == "EU"


@pytest.mark.asyncio
async def test_foreign_key_to_a_multi_column_unique_set(data, monkeypatch):
    monkeypatch.setattr(cross_check, "ENABLED", False)
    product = await Product.objects.create(tenant_id=3, sku="abc")
    await Stock.objects.create(product=product, amount=5)
    await Stock.objects.create(product={"tenant_id": 3, "sku": "abc"}, amount=6)
    stock = await Stock.objects.select_related("product").order_by("amount").all()
    assert [s.product.id for s in stock] == [product.id, product.id]
    plain = await Stock.objects.order_by("amount").first()
    assert (plain.product.tenant_id, plain.product.sku) == (3, "abc")
    loaded = await Product.objects.select_related("stock").get()
    assert sorted(s.amount for s in loaded.stock) == [5, 6]
    assert len(await Stock.objects.filter(product=product).all()) == 2
