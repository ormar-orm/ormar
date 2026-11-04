import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class TenantBase(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True)

    tenant_id: int = ormar.Integer(primary_key=True)


class Invoice(TenantBase):
    ormar_config = base_ormar_config.copy(tablename="cps_invoices")

    serial: str = ormar.String(max_length=10, primary_key=True)


class Single(TenantBase):
    ormar_config = base_ormar_config.copy(tablename="cps_singles")

    name: str = ormar.String(max_length=10)


create_test_database = init_tests(base_ormar_config)


def test_composite_child_does_not_change_sibling_key():
    assert Single.ormar_config.pk.is_composite is False
    assert Single.ormar_config.pk.autoincrement is True
    assert Single.ormar_config.table.c.tenant_id.autoincrement is True
    assert Invoice.ormar_config.pk.autoincrement is False


@pytest.mark.asyncio
async def test_sibling_of_composite_child_still_autoincrements():
    async with base_ormar_config.database:
        single = await Single.objects.create(name="a")
        assert single.pk is not None
        await Invoice.objects.create(tenant_id=1, serial="x")
        assert await Invoice.objects.count() == 1
