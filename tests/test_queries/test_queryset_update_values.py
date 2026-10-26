import base64
from typing import ForwardRef, Optional

import pydantic
import pytest

import ormar
from ormar.exceptions import ModelPersistenceError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="update_owners")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, default="owner")


NodeRef = ForwardRef("Node")


class Node(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="update_nodes")

    id: int = ormar.Integer(primary_key=True)
    owner: Optional[Owner] = ormar.ForeignKey(Owner, nullable=True)
    parent: Optional[NodeRef] = ormar.ForeignKey(  # type: ignore[valid-type]
        NodeRef, nullable=True, related_name="children"
    )
    data: Optional[dict] = ormar.JSON(nullable=True)
    raw: bytes = ormar.LargeBinary(max_length=100, default=b"")
    encoded: str = ormar.LargeBinary(
        max_length=100, represent_as_base64_str=True, default=b""
    )


Node.update_forward_refs()


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
@pytest.mark.parametrize("value_kind", ["pk", "model", "dict"])
async def test_update_sets_foreign_key(value_kind):
    async with base_ormar_config.database:
        old_owner = await Owner.objects.create()
        new_owner = await Owner.objects.create()
        node = await Node.objects.create(owner=old_owner)
        value = {
            "pk": new_owner.id,
            "model": new_owner,
            "dict": {"id": new_owner.id},
        }[value_kind]

        await Node.objects.filter(id=node.id).update(owner=value)

        loaded = await Node.objects.get(id=node.id)
        assert loaded.owner.id == new_owner.id


@pytest.mark.asyncio
@pytest.mark.parametrize("value_kind", ["pk", "model", "dict"])
async def test_update_sets_self_referencing_foreign_key(value_kind):
    async with base_ormar_config.database:
        root = await Node.objects.create()
        node = await Node.objects.create()
        value = {"pk": root.id, "model": root, "dict": {"id": root.id}}[value_kind]

        await Node.objects.filter(id=node.id).update(parent=value)

        loaded = await Node.objects.get(id=node.id)
        assert loaded.parent.id == root.id


@pytest.mark.asyncio
async def test_update_clears_foreign_key():
    async with base_ormar_config.database:
        owner = await Owner.objects.create()
        node = await Node.objects.create(owner=owner)

        await Node.objects.filter(id=node.id).update(owner=None)

        loaded = await Node.objects.get(id=node.id)
        assert loaded.owner is None


@pytest.mark.asyncio
async def test_update_with_unsaved_related_model_raises():
    async with base_ormar_config.database:
        node = await Node.objects.create()

        with pytest.raises(ModelPersistenceError):
            await Node.objects.filter(id=node.id).update(owner=Owner())


@pytest.mark.asyncio
async def test_update_validates_foreign_key_pk():
    async with base_ormar_config.database:
        node = await Node.objects.create()

        with pytest.raises(pydantic.ValidationError):
            await Node.objects.filter(id=node.id).update(owner="not a pk")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value, expected",
    [
        ({"d": 2}, {"d": 2}),
        ([1, 2], [1, 2]),
        ('{"d": 2}', {"d": 2}),
        (None, None),
    ],
)
async def test_update_sets_json_field(value, expected):
    async with base_ormar_config.database:
        node = await Node.objects.create(data={"a": 1})

        await Node.objects.filter(id=node.id).update(data=value)

        loaded = await Node.objects.get(id=node.id)
        assert loaded.data == expected


@pytest.mark.asyncio
async def test_update_sets_bytes_fields_from_strings():
    async with base_ormar_config.database:
        node = await Node.objects.create()

        await Node.objects.filter(id=node.id).update(
            raw="hi", encoded=base64.b64encode(b"hi").decode()
        )

        loaded = await Node.objects.get(id=node.id)
        assert loaded.raw == b"hi"
        assert loaded.encoded == base64.b64encode(b"hi").decode()
