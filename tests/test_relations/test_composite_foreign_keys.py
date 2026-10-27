from typing import ForwardRef, Optional

import pydantic
import pytest
import pytest_asyncio
import sqlalchemy

import ormar
from ormar.exceptions import ModelDefinitionError, QueryDefinitionError
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


class Receipt(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_receipts")

    id: int = ormar.Integer(primary_key=True)
    status: Optional[str] = ormar.String(
        max_length=10, server_default="new", nullable=True
    )
    order: Order = ormar.ForeignKey(Order, related_name="receipts")


class Node(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_nodes")

    tree: int = ormar.Integer(primary_key=True)
    number: int = ormar.Integer(primary_key=True)
    parent = ormar.ForeignKey(ForwardRef("Node"), related_name="children")


Node.update_forward_refs()


class Invoice(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_invoices")

    id: int = ormar.Integer(primary_key=True)
    order: Optional[Order] = ormar.OneToOne(Order, related_name="invoice")


class Tenant(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_tenants")

    id: int = ormar.Integer(primary_key=True)


class Ledger(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_ledgers")

    tenant: Tenant = ormar.ForeignKey(Tenant, primary_key=True, related_name="ledgers")
    number: int = ormar.Integer(primary_key=True)


class Entry(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cfk_entries")

    id: int = ormar.Integer(primary_key=True)
    ledger: Optional[Ledger] = ormar.ForeignKey(
        Ledger, related_name="entries", index=True
    )


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def data():
    async with base_ormar_config.database:
        for model in (
            OrderLine,
            Shipment,
            Stock,
            City,
            Receipt,
            Invoice,
            Entry,
            Ledger,
            Tenant,
            Order,
            Product,
            Region,
        ):
            await model.objects.delete(each=True)
        await Node.objects.update(each=True, parent=None)
        await Node.objects.delete(each=True)
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
async def test_composite_foreign_key_writes_and_reads(data):
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
async def test_foreign_key_to_a_multi_column_unique_set(data):
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


@pytest.mark.asyncio
async def test_load_and_server_defaults_with_composite_foreign_keys(data):
    first, second = data
    line = await OrderLine.objects.create(order=first, qty=1)
    await line.load()
    assert line.order.pk == (1, 7)
    receipt = await Receipt.objects.create(order=second)
    assert receipt.status == "new" and receipt.order.pk == (1, 8)
    await receipt.load()
    assert receipt.order.pk == (1, 8)
    product = await Product.objects.create(tenant_id=3, sku="x")
    stock = await Stock.objects.create(product=product)
    await stock.load()
    assert (stock.product.tenant_id, stock.product.sku) == (3, "x")


@pytest.mark.asyncio
async def test_composite_foreign_key_set_to_none_in_bulk_and_partial_updates(data):
    first, second = data
    await OrderLine.objects.bulk_create(
        [OrderLine(qty=1), OrderLine(order=first, qty=2)]
    )
    lines = await OrderLine.objects.order_by("qty").all()
    assert [line.order for line in lines][0] is None
    lines[1].order = None
    lines[0].order = second
    await OrderLine.objects.bulk_update(lines, columns=["order"])
    reloaded = await OrderLine.objects.order_by("qty").all()
    assert [None if line.order is None else line.order.pk for line in reloaded] == [
        (1, 8),
        None,
    ]
    line = reloaded[0]
    line.order = first
    line.qty = 10
    await line.update(_columns=["order"])
    stored = await OrderLine.objects.get(id=line.id)
    assert (stored.order.pk, stored.qty) == ((1, 7), 1)


@pytest.mark.asyncio
async def test_ordering_by_a_composite_foreign_key(data):
    first, second = data
    for order, qty in ((second, 1), (first, 2), (second, 3)):
        await OrderLine.objects.create(order=order, qty=qty)
    ascending = await OrderLine.objects.order_by(["order", "qty"]).all()
    assert [line.qty for line in ascending] == [2, 1, 3]
    descending = await OrderLine.objects.order_by(["-order", "qty"]).all()
    assert [line.qty for line in descending] == [1, 3, 2]


@pytest.mark.asyncio
async def test_self_referencing_composite_foreign_key(data):
    root = await Node.objects.create(tree=1, number=1)
    await Node.objects.create(tree=1, number=2, parent=root)
    child = await Node.objects.select_related("parent").get(pk=(1, 2))
    assert child.parent.pk == (1, 1)
    loaded = await Node.objects.select_related("children").get(pk=(1, 1))
    assert [node.pk for node in loaded.children] == [(1, 2)]
    assert {"parent_tree", "parent_number"} <= set(Node.ormar_config.table.c.keys())


def test_one_to_one_to_a_composite_key_is_unique():
    unique_sets = {
        tuple(column.name for column in constraint.columns)
        for constraint in Invoice.ormar_config.table.constraints
        if isinstance(constraint, sqlalchemy.UniqueConstraint)
    }
    assert ("order_tenant_id", "order_order_no") in unique_sets


def test_columns_need_a_foreign_key_to_several_columns():
    with pytest.raises(ModelDefinitionError, match="name="):

        class SingleColumns(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="cfk_single_columns")

            id: int = ormar.Integer(primary_key=True)
            region: Optional[Region] = ormar.ForeignKey(
                Region, columns={"id": "rid"}, related_name="single_columns"
            )

    with pytest.raises(ModelDefinitionError, match="order_tenant_id"):

        class Clash(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="cfk_clash")

            id: int = ormar.Integer(primary_key=True)
            order_tenant_id: int = ormar.Integer()
            order: Optional[Order] = ormar.ForeignKey(Order, related_name="clashes")


@pytest.mark.asyncio
async def test_partially_null_composite_foreign_key_reads_as_none(data):
    first, _ = data
    insert = OrderLine.ormar_config.table.insert().values(
        ord_tenant=1, ord_no=None, qty=50
    )
    async with base_ormar_config.database.get_query_executor() as executor:
        await executor.execute(insert)
    line = await OrderLine.objects.get(qty=50)
    assert line.order is None
    assert (await OrderLine.objects.select_related("order").get(qty=50)).order is None
    assert (await OrderLine.objects.values(["order"]))[0]["order"] is None
    assert await OrderLine.objects.filter(order__isnull=True).count() == 1
    assert await OrderLine.objects.filter(order__isnull=False).count() == 0
    with pytest.raises(QueryDefinitionError):
        await OrderLine.objects.filter(qty=50).update(order={"tenant_id": 2})
    with pytest.raises(QueryDefinitionError):
        await OrderLine.objects.filter(order={"tenant_id": 1}).all()
    await OrderLine.objects.create(order=[1, 7], qty=51)
    assert (await OrderLine.objects.get(qty=51)).order.pk == (1, 7)


def test_flattened_composite_key_request_validation():
    flat = OrderLine.get_pydantic(flatten_fields={"order"})
    assert flat(qty=1, order=[1, 7]).order == {"tenant_id": 1, "number": 7}
    for wrong in (5, "ab", {"foo": 1}, [1], [1, 2, 3], {"tenant_id": "x", "number": 1}):
        with pytest.raises(pydantic.ValidationError):
            flat(qty=1, order=wrong)
    schema = flat.model_json_schema()
    key_schema = schema["$defs"][next(iter(schema["$defs"]))]
    assert set(key_schema["properties"]) == {"tenant_id", "number"}


@pytest.mark.asyncio
async def test_composite_target_with_a_foreign_key_in_its_key(data):
    tenant = await Tenant.objects.create(id=4)
    ledger = await Ledger.objects.create(tenant=tenant, number=1)
    await Entry.objects.create(ledger=ledger)
    await Entry.objects.create(ledger=(4, 1))
    fetched = await Ledger.objects.prefetch_related("entries").get()
    assert len(fetched.entries) == 2
    joined = await Ledger.objects.select_related("entries").get()
    assert len(joined.entries) == 2
    entries = await Entry.objects.prefetch_related("ledger").all()
    assert [entry.ledger.pk for entry in entries] == [(4, 1), (4, 1)]
    indexes = {
        tuple(column.name for column in index.columns)
        for index in Entry.ormar_config.table.indexes
    }
    assert ("ledger_tenant", "ledger_number") in indexes


@pytest.mark.asyncio
async def test_bulk_create_with_a_missing_reverse_one_to_one(data):
    await Order.objects.bulk_create([Order(tenant_id=2, number=1)])
    assert await Order.objects.filter(tenant_id=2).count() == 1
    OrderLine.reset_column_caches()
    assert OrderLine.get_field_name_of_column(column="ord_no") == "order"
    Order.reset_column_caches()
    assert Order.get_column_name_from_alias(alias="order_no") == "number"
    assert OrderLine.get_column_name_from_alias(alias="ord_no") == "ord_no"
