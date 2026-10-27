from typing import Optional

import pytest

import ormar
from ormar.exceptions import ModelDefinitionError
from tests.settings import create_config

base_ormar_config = create_config()


class Order(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cpd_orders")

    tenant_id: int = ormar.Integer(primary_key=True)
    number: int = ormar.Integer(primary_key=True, name="order_no")
    note: Optional[str] = ormar.String(max_length=100, nullable=True)


class Single(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cpd_singles")

    id: int = ormar.Integer(primary_key=True)


class TenantScoped(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True)

    tenant_id: int = ormar.Integer(primary_key=True)


class Invoice(TenantScoped):
    ormar_config = base_ormar_config.copy(tablename="cpd_invoices")

    code: str = ormar.String(max_length=20, primary_key=True)


def test_composite_key_ddl_and_fields():
    key = Order.ormar_config.pk
    assert key.fields == ("tenant_id", "number")
    assert key.column_aliases() == ("tenant_id", "order_no")
    table_pk = Order.ormar_config.table.primary_key
    assert [column.name for column in table_pk.columns] == ["tenant_id", "order_no"]


def test_composite_parts_do_not_autoincrement():
    assert Order.ormar_config.table.c.tenant_id.autoincrement is False
    assert Order.ormar_config.table.c.order_no.autoincrement is False
    assert Order.ormar_config.pk.autoincrement is False


def test_composite_pk_value_on_instance():
    order = Order(tenant_id=1, number=7)
    assert order.pk == (1, 7)
    order.pk = (2, 8)
    assert (order.tenant_id, order.number) == (2, 8)
    assert Order(tenant_id=1).pk is None


def test_composite_key_across_abstract_parent():
    assert Invoice.ormar_config.pk.fields == ("tenant_id", "code")
    assert Invoice(tenant_id=3, code="x").pk == (3, "x")


def test_explicit_autoincrement_on_composite_part_raises():
    with pytest.raises(ModelDefinitionError, match="cannot autoincrement"):

        class Bad(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="cpd_bad")

            a: int = ormar.Integer(primary_key=True, autoincrement=True)
            b: int = ormar.Integer(primary_key=True)


def test_many_to_many_to_composite_key_model_raises_for_now():
    with pytest.raises(ModelDefinitionError, match="composite primary key"):

        class Tag(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="cpd_tags")

            id: int = ormar.Integer(primary_key=True)
            orders = ormar.ManyToMany(Order)


def test_many_to_many_on_composite_key_model_raises_for_now():
    with pytest.raises(ModelDefinitionError, match="Many to many"):

        class Bundle(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="cpd_bundles")

            tenant_id: int = ormar.Integer(primary_key=True)
            code: str = ormar.String(max_length=10, primary_key=True)
            singles = ormar.ManyToMany(Single)
