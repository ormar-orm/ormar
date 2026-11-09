import warnings
from datetime import datetime
from typing import Any, Optional

import pytest

import ormar
import ormar.models.model
from ormar import pre_update
from ormar.queryset.templates import update_template
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()
partial_config = base_ormar_config.copy(partial_updates=True)


class Instructor(ormar.Model):
    ormar_config = partial_config.copy(tablename="partial_instructors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Training(ormar.Model):
    ormar_config = partial_config.copy(tablename="partial_trainings")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, name="training_name")
    status: str = ormar.String(max_length=20, default="open")
    seats_taken: int = ormar.Integer(default=0)
    note: Optional[str] = ormar.String(max_length=100, nullable=True)
    settings: Optional[dict] = ormar.JSON(nullable=True)
    instructor: Optional[Instructor] = ormar.ForeignKey(Instructor)


class Booking(ormar.Model):
    ormar_config = partial_config.copy(tablename="partial_bookings")

    id: int = ormar.Integer(primary_key=True)
    status: str = ormar.String(max_length=20, default="new")
    paid: bool = ormar.Boolean(default=False)
    updated_at: datetime = ormar.DateTime(
        default=datetime(2000, 1, 1), on_update=datetime.now
    )


class Seat(ormar.Model):
    ormar_config = partial_config.copy(tablename="partial_seats")

    code: str = ormar.String(max_length=10, primary_key=True)
    row: int = ormar.Integer()
    taken: bool = ormar.Boolean(default=False)


class LegacyTraining(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="partial_legacy_trainings")

    id: int = ormar.Integer(primary_key=True)
    status: str = ormar.String(max_length=20, default="open")
    seats_taken: int = ormar.Integer(default=0)
    settings: Optional[dict] = ormar.JSON(nullable=True)


class FullTraining(ormar.Model):
    ormar_config = partial_config.copy(
        tablename="partial_full_trainings", partial_updates=False
    )

    id: int = ormar.Integer(primary_key=True)
    status: str = ormar.String(max_length=20, default="open")
    seats_taken: int = ormar.Integer(default=0)


@pre_update(Booking)
async def mark_paid(sender: Any, instance: Booking, **kwargs: Any) -> None:
    if instance.status == "confirmed":
        instance.paid = True


create_test_database = init_tests(base_ormar_config)


@pytest.fixture
def written_columns(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Records the column names of every ``Model.update()`` statement."""
    calls: list[list[str]] = []

    def spy(model_cls: Any, values: Any, pk: Any) -> Any:
        calls.append(sorted(values))
        return update_template(model_cls=model_cls, values=values, pk=pk)

    monkeypatch.setattr(ormar.models.model, "update_template", spy)
    return calls


async def clean_tables() -> None:
    for model in (Training, Instructor, Booking, Seat, LegacyTraining, FullTraining):
        await model.objects.delete(each=True)


def test_config_copy_keeps_or_overrides_partial_updates():
    assert base_ormar_config.partial_updates is None
    assert partial_config.partial_updates is True
    assert partial_config.copy().partial_updates is True
    assert partial_config.copy(partial_updates=None).partial_updates is None
    assert FullTraining.ormar_config.partial_updates is False


@pytest.mark.asyncio
async def test_stale_instance_does_not_revert_other_columns():
    async with base_ormar_config.database:
        await clean_tables()
        await Training(name="Pistol").save()
        stale = await Training.objects.get()
        fresh = await Training.objects.get()

        await fresh.update(seats_taken=5)
        await stale.update(status="closed")

        row = await Training.objects.get()
        assert row.status == "closed"
        assert row.seats_taken == 5


@pytest.mark.asyncio
async def test_setattr_and_kwargs_are_both_written(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        training = await Training(name="Pistol").save()
        concurrent = await Training.objects.get()
        await concurrent.update(seats_taken=3)

        training.note = "bring ear protection"
        await training.update(name="Rifle")

        assert written_columns[-1] == ["note", "training_name"]
        row = await Training.objects.get()
        assert row.name == "Rifle"
        assert row.note == "bring ear protection"
        assert row.seats_taken == 3


@pytest.mark.asyncio
async def test_nothing_changed_writes_all_columns(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        training = await Training(name="Pistol").save()
        await training.update()
        assert written_columns[-1] == [
            "instructor",
            "note",
            "seats_taken",
            "settings",
            "status",
            "training_name",
        ]


@pytest.mark.asyncio
async def test_partial_flag_overrides_config(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        training = await Training(name="Pistol").save()
        await training.update(_partial=False, status="closed")
        assert len(written_columns[-1]) == 6

        legacy = await LegacyTraining().save()
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            await legacy.update(_partial=True, status="closed")
        assert written_columns[-1] == ["status"]


@pytest.mark.asyncio
async def test_columns_keep_their_meaning(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        training = await Training(name="Pistol").save()
        training.note = "not written"
        await training.update(_columns=["status"], status="closed")
        assert written_columns[-1] == ["status"]
        row = await Training.objects.get()
        assert row.status == "closed"
        assert row.note is None


@pytest.mark.asyncio
async def test_field_set_in_pre_update_signal_is_written(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        booking = await Booking().save()
        await booking.update(status="confirmed")

        assert written_columns[-1] == ["paid", "status", "updated_at"]
        row = await Booking.objects.get()
        assert row.paid is True
        assert row.status == "confirmed"


@pytest.mark.asyncio
async def test_onupdate_field_is_written(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        booking = await Booking().save()
        await booking.update(status="cancelled")

        assert written_columns[-1] == ["status", "updated_at"]
        row = await Booking.objects.get()
        assert row.updated_at > datetime(2000, 1, 1)

        await booking.update()
        assert written_columns[-1] == ["paid", "status", "updated_at"]


@pytest.mark.asyncio
async def test_json_mutated_in_place_is_written(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        await Training(name="Pistol", settings={"lanes": 4}).save()
        training = await Training.objects.get()

        training.settings["lanes"] = 6
        await training.update(status="closed")
        assert written_columns[-1] == ["settings", "status"]
        assert (await Training.objects.get()).settings == {"lanes": 6}

        # the snapshot is taken again after the update
        await training.update(seats_taken=2)
        assert written_columns[-1] == ["seats_taken"]

        training.settings = {"lanes": 8}
        await training.update(status="open")
        assert written_columns[-1] == ["settings", "status"]
        assert (await Training.objects.get()).settings == {"lanes": 8}


@pytest.mark.asyncio
async def test_json_mutation_after_save_and_load_is_written(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        training = await Training(name="Pistol", settings={"lanes": 4}).save()
        training.settings["lanes"] = 5
        await training.update(status="closed")
        assert written_columns[-1] == ["settings", "status"]

        other = await Training.objects.get()
        await other.update(settings={"lanes": 7})
        await training.load()
        training.settings["lanes"] += 1
        await training.update(seats_taken=1)
        assert written_columns[-1] == ["seats_taken", "settings"]
        assert (await Training.objects.get()).settings == {"lanes": 8}


@pytest.mark.asyncio
async def test_foreign_key_assignment_is_written(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        instructor = await Instructor(name="Anna").save()
        training = await Training(name="Pistol").save()
        concurrent = await Training.objects.get()
        await concurrent.update(seats_taken=4)

        training.instructor = instructor
        await training.update()

        assert written_columns[-1] == ["instructor"]
        row = await Training.objects.select_related("instructor").get()
        assert row.instructor.name == "Anna"
        assert row.seats_taken == 4


@pytest.mark.asyncio
async def test_reverse_relation_add_and_remove_write_only_the_key(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        instructor = await Instructor(name="Anna").save()
        training = await Training(name="Pistol").save()
        concurrent = await Training.objects.get()
        await concurrent.update(seats_taken=4)

        await instructor.trainings.add(training)
        assert written_columns[-1] == ["instructor"]
        await instructor.trainings.remove(training)
        assert written_columns[-1] == ["instructor"]

        row = await Training.objects.get()
        assert row.instructor is None
        assert row.seats_taken == 4


@pytest.mark.asyncio
async def test_primary_key_change_moves_the_row(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        seat = await Seat(code="A1", row=1).save()
        seat.code = "B1"
        await seat.update(taken=True)

        assert written_columns[-1] == ["code", "taken"]
        row = await Seat.objects.get()
        assert row.code == "B1"
        assert row.taken is True
        assert row.row == 1


@pytest.mark.asyncio
async def test_tracking_is_cleared_after_save_update_and_load(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        training = Training(name="Pistol")
        training.note = "first"
        await training.save()
        await training.update(status="closed")
        assert written_columns[-1] == ["status"]

        training.note = "second"
        await training.load()
        await training.update(seats_taken=1)
        assert written_columns[-1] == ["seats_taken"]

        training.note = "third"
        await training.load_all()
        await training.update(seats_taken=2)
        assert written_columns[-1] == ["seats_taken"]

        training.note = "fourth"
        await Training.objects.bulk_update([training], columns=["note"])
        await training.update(seats_taken=3)
        assert written_columns[-1] == ["seats_taken"]


@pytest.mark.asyncio
async def test_upsert_and_update_or_create(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        training = await Training(name="Pistol").save()
        await training.upsert(status="closed")
        assert written_columns[-1] == ["status"]

        # a model never loaded from the database writes all of its values
        detached = Training(id=training.id, name="Rifle", note="detached")
        await detached.upsert(seats_taken=7)
        assert len(written_columns[-1]) == 6

        await Training.objects.update_or_create(id=training.id, note="again")
        assert written_columns[-1] == ["note"]


@pytest.mark.asyncio
async def test_save_related_writes_changed_fields(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        instructor = await Instructor(name="Anna").save()
        training = await Training(name="Pistol", instructor=instructor).save()
        instructor.name = "Anna Maria"
        await training.save_related()
        assert written_columns[-1] == ["name"]
        assert (await Instructor.objects.get()).name == "Anna Maria"


@pytest.mark.asyncio
async def test_default_config_warns_when_other_columns_are_written(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        training = await LegacyTraining().save()
        with pytest.warns(DeprecationWarning, match="partial_updates=True"):
            await training.update(status="closed")
        assert written_columns[-1] == ["seats_taken", "settings", "status"]

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            await training.update(_columns=["status"], status="open")
            training.status = "closed"
            await training.update()
            await training.update(status="open", seats_taken=1, settings={})
            await training.update(_partial=False, status="closed")


@pytest.mark.asyncio
async def test_no_warning_with_partial_updates_set(written_columns):
    async with base_ormar_config.database:
        await clean_tables()
        training = await Training(name="Pistol").save()
        full = await FullTraining().save()
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            await training.update(status="closed")
            await full.update(status="closed")
        assert written_columns[-1] == ["seats_taken", "status"]


@pytest.mark.asyncio
async def test_default_config_ignores_json_snapshot():
    async with base_ormar_config.database:
        await clean_tables()
        await LegacyTraining(settings={"lanes": 1}).save()
        training = await LegacyTraining.objects.get()
        assert training.__json_snapshot__ is None
