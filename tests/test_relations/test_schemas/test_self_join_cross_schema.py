"""Self-referential foreign key on a model that lives in a non-default schema."""

from typing import ForwardRef

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

from .dialect import requires_schemas

pytestmark = requires_schemas

base_ormar_config = create_config()


class OrgNode(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="org_nodes", schema="org_tree")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    parent = ormar.ForeignKey(ForwardRef("OrgNode"), related_name="children")


OrgNode.update_forward_refs()

create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_self_reference_across_same_schema():  # pragma: no cover
    async with (
        base_ormar_config.database,
        base_ormar_config.database.transaction(force_rollback=True),
    ):
        ceo = await OrgNode.objects.create(name="CEO")
        cto = await OrgNode.objects.create(name="CTO", parent=ceo)
        await OrgNode.objects.create(name="Eng Lead", parent=cto)

        chain = (
            await OrgNode.objects.select_related("parent__parent")
            .filter(name="Eng Lead")
            .get()
        )
        assert chain.parent.name == "CTO"
        assert chain.parent.parent.name == "CEO"
