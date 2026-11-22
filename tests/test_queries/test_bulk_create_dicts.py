import datetime
import uuid
from typing import Any, Optional, assert_type

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

    a: int = ormar.Integer(primary_key=True, autoincrement=False)
    b: int = ormar.Integer(primary_key=True, autoincrement=False)
    value: int = ormar.Integer(default=0)


class Booking(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bd_bookings")

    id: int = ormar.Integer(primary_key=True)
    pair: Optional[Pair] = ormar.ForeignKey(
        Pair, columns={"a": "pair_a", "b": "pair_b"}, related_name="bookings"
    )
    slot: int = ormar.Integer(name="slot_number")


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
            with pytest.raises(ModelPersistenceError, match="'b'"):
                await Pair.objects.bulk_create([{"a": 1}])
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
        with pytest.raises(QueryDefinitionError, match="not models"):
            await Item.objects.bulk_create(  # type: ignore[call-overload]
                [Item(name="b")], returning=True
            )
        with pytest.raises(ModelListEmptyError):
            await Item.objects.bulk_create([], returning=True)
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
async def test_returning_inserted_rows() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            owner = await Owner.objects.create(name="o")
            rows = await Item.objects.bulk_create(
                [
                    {"name": "a", "owner": owner, "released": "2024-01-02"},
                    {"name": "b"},
                    {"id": 1000, "name": "c", "score": 3},
                ],
                returning=True,
            )
            assert_type(rows, list[dict[str, Any]])
            by_name = {row["name"]: row for row in rows}
            assert set(by_name) == {"a", "b", "c"}
            stored = {item.name: item for item in await Item.objects.all()}
            for name, row in by_name.items():
                assert row["id"] == stored[name].id
                assert row["token"] == stored[name].token
            assert by_name["c"]["id"] == 1000
            assert by_name["a"]["owner"] == owner.pk
            assert by_name["a"]["released"] == datetime.date(2024, 1, 2)
            assert by_name["b"]["owner"] is None
            assert by_name["b"]["score"] == 0
            assert set(by_name["a"]) == {
                column.name for column in Item.ormar_config.table.columns
            }


@pytest.mark.asyncio
async def test_returning_leaves_out_ignored_rows() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Item.objects.bulk_create([{"name": "a", "score": 1}])
            rows = await Item.objects.bulk_create(
                [{"name": "a", "score": 5}, {"name": "b", "score": 2}],
                on_conflict="ignore",
                returning=True,
            )
            assert [(row["name"], row["score"]) for row in rows] == [("b", 2)]
            # the transaction stays usable after a skipped row
            assert await Item.objects.count() == 2
            skipped = await Item.objects.bulk_create(
                [{"name": "a"}], on_conflict="ignore", returning=True
            )
            assert skipped == []
            repeated = await Item.objects.bulk_create(
                [{"name": "c", "score": 1}, {"name": "c", "score": 2}],
                on_conflict="ignore",
                returning=True,
            )
            assert [(row["name"], row["score"]) for row in repeated] == [("c", 1)]


@pytest.mark.asyncio
async def test_returning_upserted_rows() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Item.objects.bulk_create([{"name": "a", "score": 1}])
            rows = await Item.objects.bulk_create(
                [{"name": "a", "score": 10}, {"name": "b", "score": 2}],
                on_conflict="update",
                conflict_target=["name"],
                update_values={"score": F("score") + 100},
                returning=True,
            )
            by_name = {row["name"]: row for row in rows}
            assert by_name["a"]["score"] == 101 and by_name["a"]["is_dirty"]
            assert by_name["b"]["score"] == 2 and not by_name["b"]["is_dirty"]

            rows = await Pair.objects.bulk_create(
                [{"a": 1, "b": 1, "value": 1}, {"a": 1, "b": 2}],
                on_conflict="update",
                returning=True,
            )
            assert sorted((row["a"], row["b"], row["value"]) for row in rows) == [
                (1, 1, 1),
                (1, 2, 0),
            ]

            with pytest.raises(QueryDefinitionError, match="conflict target"):
                await Item.objects.bulk_create(
                    [{"score": 1, "name": "c"}], on_conflict="update", returning=True
                )


@pytest.mark.asyncio
async def test_returning_multi_column_foreign_key_and_aliases() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            pair = await Pair.objects.create(a=1, b=2)
            rows = await Booking.objects.bulk_create(
                [{"pair": pair, "slot": 1}, {"pair": {"a": 1, "b": 2}, "slot": 2}],
                returning=True,
            )
            assert sorted((row["pair"], row["slot"]) for row in rows) == [
                ((1, 2), 1),
                ((1, 2), 2),
            ]
            bookings = await Booking.objects.select_related("pair").all()
            assert {booking.pair for booking in bookings} == {pair}


@pytest.mark.asyncio
async def test_model_input_keeps_its_behaviour() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            items = [Item(name="a"), Item(name="b")]
            await Item.objects.bulk_create(items)
            assert all(item.saved for item in items)
            assert await Item.objects.count() == 2
