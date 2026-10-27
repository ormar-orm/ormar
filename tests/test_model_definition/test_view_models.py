from typing import Optional

import pytest
import pytest_asyncio
import sqlalchemy

import ormar
from ormar.exceptions import ModelDefinitionError, ReadOnlyModelError
from tests import cross_check
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Customer(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="vw_customers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Purchase(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="vw_purchases")

    id: int = ormar.Integer(primary_key=True)
    customer: Optional[Customer] = ormar.ForeignKey(Customer, related_name="purchases")
    amount: float = ormar.Float()


class CustomerTotal(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="vw_customer_totals", view=True)

    customer: Customer = ormar.ForeignKey(
        Customer, primary_key=True, name="customer_id", related_name="totals"
    )
    total: float = ormar.Float()


class Flag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="vw_flags")

    id: int = ormar.Integer(primary_key=True)
    summary: Optional[CustomerTotal] = ormar.ForeignKey(
        CustomerTotal, db_constraint=False, related_name="flags"
    )


create_test_database = init_tests(base_ormar_config)

VIEW_SQL = (
    "CREATE VIEW vw_customer_totals AS SELECT customer AS customer_id, "
    "SUM(amount) AS total FROM vw_purchases GROUP BY customer"
)


@pytest_asyncio.fixture(scope="module", autouse=True)
async def view(create_test_database):
    async with base_ormar_config.engine.begin() as conn:
        await conn.execute(sqlalchemy.text("DROP VIEW IF EXISTS vw_customer_totals"))
        await conn.execute(sqlalchemy.text(VIEW_SQL))
    yield
    async with base_ormar_config.engine.begin() as conn:
        await conn.execute(sqlalchemy.text("DROP VIEW IF EXISTS vw_customer_totals"))


@pytest_asyncio.fixture()
async def data():
    async with base_ormar_config.database:
        await Flag.objects.delete(each=True)
        await Purchase.objects.delete(each=True)
        await Customer.objects.delete(each=True)
        ann = await Customer.objects.create(name="Ann")
        bob = await Customer.objects.create(name="Bob")
        await Purchase.objects.bulk_create(
            [
                Purchase(customer=ann, amount=1.5),
                Purchase(customer=ann, amount=2.5),
                Purchase(customer=bob, amount=10),
            ]
        )
        yield ann, bob


def test_view_is_not_part_of_the_metadata():
    assert "vw_customer_totals" not in base_ormar_config.metadata.tables
    table = CustomerTotal.ormar_config.table
    assert all(not column.foreign_keys for column in table.c)
    assert all(not column.foreign_keys for column in Flag.ormar_config.table.c)


def test_foreign_key_to_a_view_needs_db_constraint_false():
    with pytest.raises(ModelDefinitionError, match="view"):

        class Bad(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="vw_bad")

            id: int = ormar.Integer(primary_key=True)
            summary: Optional[CustomerTotal] = ormar.ForeignKey(
                CustomerTotal, related_name="bads"
            )


@pytest.mark.asyncio
async def test_reading_a_view(data, monkeypatch):
    # the cross-check's detached copies drop relations, and a foreign key in the
    # primary key is required
    monkeypatch.setattr(cross_check, "ENABLED", False)
    ann, bob = data
    totals = await CustomerTotal.objects.order_by("total").all()
    assert [(t.pk, t.total) for t in totals] == [(ann.id, 4.0), (bob.id, 10.0)]
    joined = await CustomerTotal.objects.select_related("customer").get(pk=bob.id)
    assert joined.customer.name == "Bob"
    loaded = await Customer.objects.select_related("totals").get(id=ann.id)
    assert [t.total for t in loaded.totals] == [4.0]
    fetched = await Customer.objects.prefetch_related("totals").get(id=bob.id)
    assert [t.total for t in fetched.totals] == [10.0]
    assert await ann.totals.count() == 1
    flag = await Flag.objects.create(summary=ann.id)
    assert (await Flag.objects.select_related("summary").get()).summary.total == 4.0
    assert flag.summary.pk == ann.id


@pytest.mark.asyncio
async def test_writing_a_view_raises(data):
    ann, _ = data
    total = await CustomerTotal.objects.get(pk=ann.id)
    await total.load_all()
    for write in (total.save, total.update, total.upsert, total.delete):
        with pytest.raises(ReadOnlyModelError):
            await write()
    with pytest.raises(ReadOnlyModelError):
        await CustomerTotal.objects.create(customer=ann, total=1)
    with pytest.raises(ReadOnlyModelError):
        await CustomerTotal.objects.bulk_create([CustomerTotal(customer=ann, total=1)])
    with pytest.raises(ReadOnlyModelError):
        await CustomerTotal.objects.bulk_update([total])
    with pytest.raises(ReadOnlyModelError):
        await CustomerTotal.objects.filter(pk=ann.id).update(total=1)
    with pytest.raises(ReadOnlyModelError):
        await CustomerTotal.objects.delete(each=True)
    with pytest.raises(ReadOnlyModelError):
        await ann.totals.create(total=3)
