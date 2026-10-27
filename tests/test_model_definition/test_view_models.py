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
        await Badge.objects.delete(each=True)
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


class CustomerTotalProxy(CustomerTotal):
    ormar_config = base_ormar_config.copy(proxy=True)


class Badge(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="vw_badges")

    id: int = ormar.Integer(primary_key=True)
    totals = ormar.ManyToMany(CustomerTotal, related_name="badges")


class ReportBase(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True, view=True)

    customer_id: int = ormar.Integer(primary_key=True)


class ReportChild(ReportBase):
    ormar_config = base_ormar_config.copy(tablename="vw_report_child")

    total: float = ormar.Float()


def test_proxies_and_children_of_views_are_views():
    assert CustomerTotalProxy.ormar_config.view is True
    assert ReportChild.ormar_config.view is True
    assert "vw_report_child" not in base_ormar_config.metadata.tables


def test_view_table_keeps_metadata_schema_and_naming():
    metadata = sqlalchemy.MetaData(
        schema="reporting", naming_convention={"ix": "ix_%(column_0_label)s"}
    )
    config = ormar.OrmarConfig(database=base_ormar_config.database, metadata=metadata)

    class SchemaView(ormar.Model):
        ormar_config = config.copy(tablename="schema_view", view=True)

        id: int = ormar.Integer(primary_key=True)

    table = SchemaView.ormar_config.table
    assert table.schema == "reporting"
    assert table.metadata.naming_convention["ix"] == "ix_%(column_0_label)s"


def test_many_to_many_to_a_view_creates_no_constraint_to_it():
    through = Badge.ormar_config.model_fields["totals"].through
    targets = {
        fk.target_fullname
        for column in through.ormar_config.table.c
        for fk in column.foreign_keys
    }
    assert targets == {"vw_badges.id"}


@pytest.mark.asyncio
async def test_relation_writes_through_views_are_refused_before_changing_anything(
    data, monkeypatch
):
    monkeypatch.setattr(cross_check, "ENABLED", False)
    ann, bob = data
    total = await CustomerTotal.objects.get(pk=ann.id)
    with pytest.raises(ReadOnlyModelError):
        await CustomerTotalProxy(customer=ann, total=1).save()
    with pytest.raises(ReadOnlyModelError):
        await CustomerTotalProxy.objects.delete(each=True)
    loaded = await Customer.objects.select_related("totals").get(id=bob.id)
    with pytest.raises(ReadOnlyModelError):
        await loaded.totals.add(total)
    assert [t.pk for t in loaded.totals] == [bob.id]
    with pytest.raises(ReadOnlyModelError):
        await loaded.totals.remove(loaded.totals[0])
    assert [t.pk for t in loaded.totals] == [bob.id]
    with pytest.raises(ReadOnlyModelError):
        await total.save_related()
    badge = await Badge.objects.create()
    await badge.totals.add(total)
    assert [t.total for t in await badge.totals.all()] == [4.0]


@pytest.mark.asyncio
async def test_save_related_of_a_table_skips_loaded_view_relations(data, monkeypatch):
    monkeypatch.setattr(cross_check, "ENABLED", False)
    ann, _ = data
    customer = await Customer.objects.prefetch_related("totals").get(id=ann.id)
    customer.name = "Anna"
    await customer.save_related(save_all=True)
    assert (await Customer.objects.get(id=ann.id)).name == "Anna"
    loaded = await Customer.objects.select_related("totals").get(id=ann.id)
    await loaded.save_related(save_all=True)
