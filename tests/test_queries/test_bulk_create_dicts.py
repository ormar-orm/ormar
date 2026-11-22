import base64
import datetime
import enum
import uuid
from typing import Any, Optional

import pydantic
import pytest

import ormar
from ormar import F, Inserted
from ormar.exceptions import (
    ModelListEmptyError,
    ModelPersistenceError,
    QueryDefinitionError,
)
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bd_owners")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Label(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bd_labels")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bd_items")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20, unique=True)
    score: int = ormar.Integer(default=0)
    token: uuid.UUID = ormar.UUID(default=uuid.uuid4)
    released: Optional[datetime.date] = ormar.Date(nullable=True)
    extra: Optional[dict] = ormar.JSON(nullable=True)
    owner: Optional[Owner] = ormar.ForeignKey(Owner, related_name="items")
    labels: Optional[list[Label]] = ormar.ManyToMany(Label)
    is_dirty: bool = ormar.Boolean(default=False, on_update=True)


class Pair(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bd_pairs")

    left_id: int = ormar.Integer(primary_key=True, autoincrement=False)
    right_id: int = ormar.Integer(primary_key=True, autoincrement=False)
    value: int = ormar.Integer(default=0)


class Booking(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bd_bookings")

    id: int = ormar.Integer(primary_key=True)
    pair: Optional[Pair] = ormar.ForeignKey(
        Pair,
        columns={"left_id": "pair_left", "right_id": "pair_right"},
        related_name="bookings",
    )
    slot: int = ormar.Integer(name="slot_number")


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Blob(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bd_blobs")

    id: int = ormar.Integer(primary_key=True)
    raw: bytes = ormar.LargeBinary(max_length=100)
    encoded: str = ormar.LargeBinary(max_length=100, represent_as_base64_str=True)
    color: Color = ormar.Enum(enum_class=Color)

    @pydantic.computed_field  # type: ignore[prop-decorator]
    @property
    def raw_size(self) -> int:
        return len(self.raw)


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_dicts_get_defaults_conversion_and_foreign_keys() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            owner = await Owner.objects.create(name="o")
            await Item.objects.bulk_create(
                [
                    {"name": "pk", "owner": owner.pk, "released": "2024-01-02"},
                    {"name": "model", "owner": owner, "score": "7"},
                    {"name": "dict", "owner": {"id": owner.pk}, "extra": '{"a": 1}'},
                    {"name": "none", "owner": None, "extra": {"b": [1, 2]}},
                ]
            )
            items = {
                item.name: item
                for item in await Item.objects.select_related("owner").all()
            }
            assert len(items) == 4
            assert items["pk"].released == datetime.date(2024, 1, 2)
            assert items["model"].score == 7
            assert items["dict"].extra == {"a": 1}
            assert items["none"].extra == {"b": [1, 2]}
            assert items["none"].owner is None
            for name in ("pk", "model", "dict"):
                assert items[name].owner == owner
            assert len({item.token for item in items.values()}) == 4
            assert all(not item.is_dirty for item in items.values())


@pytest.mark.asyncio
async def test_dict_rows_are_validated() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with pytest.raises(pydantic.ValidationError):
                await Item.objects.bulk_create([{"name": "x" * 21}])
            with pytest.raises(pydantic.ValidationError):
                await Item.objects.bulk_create([{"name": "a", "score": "many"}])
            with pytest.raises(QueryDefinitionError, match="unknown"):
                await Item.objects.bulk_create([{"name": "a", "unknown": 1}])
            with pytest.raises(QueryDefinitionError, match="labels"):
                await Item.objects.bulk_create([{"name": "a", "labels": []}])
            with pytest.raises(QueryDefinitionError, match="itemlabel"):
                await Item.objects.bulk_create([{"name": "a", "itemlabel": None}])
            with pytest.raises(ModelPersistenceError, match="name"):
                await Item.objects.bulk_create([{"score": 1}])
            with pytest.raises(ModelPersistenceError, match="without pk"):
                await Item.objects.bulk_create(
                    [{"name": "a", "owner": Owner(name="o")}]
                )
            with pytest.raises(ModelPersistenceError, match="right_id"):
                await Pair.objects.bulk_create([{"left_id": 1}])
            assert await Item.objects.count() == 0


@pytest.mark.asyncio
async def test_models_and_dicts_cannot_be_mixed() -> None:
    async with base_ormar_config.database:
        with pytest.raises(QueryDefinitionError, match="not both"):
            await Item.objects.bulk_create(
                [{"name": "a"}, Item(name="b")]  # type: ignore[arg-type]
            )
        with pytest.raises(QueryDefinitionError, match="not both"):
            await Item.objects.bulk_create(
                [Item(name="b"), {"name": "a"}]  # type: ignore[arg-type]
            )
        with pytest.raises(ModelListEmptyError):
            await Item.objects.bulk_create([])
        with pytest.raises(QueryDefinitionError, match="on_conflict='update'"):
            await Item.objects.bulk_create(
                [{"name": "a"}], on_conflict="ignore", update_fields=["score"]
            )


@pytest.mark.asyncio
async def test_dict_upsert_ignore_and_update() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Item.objects.bulk_create([{"name": "a", "score": 1}])
            await Item.objects.bulk_create(
                [{"name": "a", "score": 5}, {"name": "b", "score": 2}],
                on_conflict="ignore",
            )
            scores = {item.name: item.score for item in await Item.objects.all()}
            assert scores == {"a": 1, "b": 2}

            await Item.objects.bulk_create(
                [{"name": "a", "score": 10}, {"name": "c", "score": 3}],
                on_conflict="update",
                conflict_target=["name"],
                update_values={"score": F("score") + Inserted("score")},
            )
            items = {item.name: item for item in await Item.objects.all()}
            assert {name: item.score for name, item in items.items()} == {
                "a": 11,
                "b": 2,
                "c": 3,
            }
            assert items["a"].is_dirty and not items["c"].is_dirty


@pytest.mark.asyncio
async def test_dicts_with_multi_column_foreign_key_and_aliases() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            pair = await Pair.objects.create(left_id=1, right_id=2)
            await Booking.objects.bulk_create(
                [
                    {"pair": pair, "slot": 1},
                    {"pair": {"left_id": 1, "right_id": 2}, "slot": 2},
                ]
            )
            bookings = await Booking.objects.select_related("pair").all()
            assert sorted(booking.slot for booking in bookings) == [1, 2]
            assert {booking.pair for booking in bookings} == {pair}


@pytest.mark.asyncio
async def test_model_input_keeps_its_behaviour() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            items = [Item(name="a"), Item(name="b")]
            await Item.objects.bulk_create(items)
            assert all(item.saved for item in items)
            assert await Item.objects.count() == 2


@pytest.mark.asyncio
async def test_dicts_with_bytes_base64_and_enums() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            encoded = base64.b64encode(b"xyz").decode()
            rows: list[dict[str, Any]] = [
                {"raw": b"abc", "encoded": encoded, "color": "red"},
                {"raw": "text", "encoded": encoded, "color": Color.BLUE},
            ]
            await Blob.objects.bulk_create(rows)
            blobs = await Blob.objects.all()
            assert all(blob.encoded == encoded for blob in blobs)
            assert {blob.__dict__["encoded"] for blob in blobs} == {b"xyz"}
            assert {blob.raw for blob in blobs} == {b"abc", b"text"}
            assert {blob.color for blob in blobs} == {Color.RED, Color.BLUE}
            with pytest.raises(pydantic.ValidationError):
                await Blob.objects.bulk_create(
                    [{"raw": b"a", "encoded": encoded, "color": "green"}]
                )
            # pydantic-only fields are rejected, so a model_dump() needs filtering
            dumped = Blob(raw=b"a", encoded=encoded, color=Color.RED).model_dump()
            with pytest.raises(QueryDefinitionError, match="raw_size"):
                await Blob.objects.bulk_create([dumped])
