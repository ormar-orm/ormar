import datetime
import decimal
import enum
import pickle
import uuid
from typing import Optional
from unittest import mock

import pydantic
import pytest
import pytest_asyncio

import ormar
from ormar.queryset import queryset, templates
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class Plain(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_plain")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    amount: decimal.Decimal = ormar.Decimal(max_digits=10, decimal_places=2)
    ratio: float = ormar.Float(default=1.5)
    flag: bool = ormar.Boolean(default=True)
    day: datetime.date = ormar.Date(default=datetime.date(2020, 1, 1))
    stamp: datetime.datetime = ormar.DateTime(default=datetime.datetime(2020, 1, 1))
    payload: dict = ormar.JSON(default={})
    uid: uuid.UUID = ormar.UUID(default=uuid.uuid4)
    color: Color = ormar.Enum(enum_class=Color, default=Color.RED)
    blob: bytes = ormar.LargeBinary(max_length=50, default=b"x")
    note: Optional[str] = ormar.String(max_length=100, nullable=True)


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_owners")

    id: int = ormar.Integer(primary_key=True)


class Pet(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_pets")

    id: int = ormar.Integer(primary_key=True)
    owner: Optional[Owner] = ormar.ForeignKey(Owner)


class Validated(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_validated")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)

    @pydantic.field_validator("name")
    @classmethod
    def upper(cls, value: str) -> str:
        return value.upper()


class Encoded(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_encoded")

    id: int = ormar.Integer(primary_key=True)
    data: str = ormar.LargeBinary(max_length=50, represent_as_base64_str=True)


class Secret(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_secret")

    id: int = ormar.Integer(primary_key=True)
    secret: str = ormar.String(
        max_length=200,
        encrypt_secret="s3cr3t",
        encrypt_backend=ormar.EncryptBackends.FERNET,
    )


class EnumValues(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_enum_values")
    model_config = pydantic.ConfigDict(use_enum_values=True)

    id: int = ormar.Integer(primary_key=True)
    color: Color = ormar.Enum(enum_class=Color, default=Color.RED)


class Stripped(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_stripped")
    model_config = pydantic.ConfigDict(str_strip_whitespace=True)

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Overwritten(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_overwritten")

    id: int = ormar.Integer(primary_key=True)
    data: str = ormar.String(max_length=100, overwrite_pydantic_type=pydantic.EmailStr)


class CustomInit(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_custom_init")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class Early(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_early")

    id: int = ormar.Integer(primary_key=True)


EARLY_TRUSTED_BEFORE_RELATION = Early.trusted_row_construction()


class Late(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="trusted_late")

    id: int = ormar.Integer(primary_key=True)
    early: Optional[Early] = ormar.ForeignKey(Early)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def connected():
    async with base_ormar_config.database:
        yield


class ConstructCounter:
    def __init__(self, original):
        self.original = original
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        return self.original(*args, **kwargs)


def test_eligibility():
    assert Plain.trusted_row_construction() is True
    assert Pet.trusted_row_construction() and Owner.trusted_row_construction()
    assert Plain.flat_row_model() is True
    assert Pet.flat_row_model() is False and Owner.flat_row_model() is False
    untrusted = (
        Validated,
        Encoded,
        Secret,
        EnumValues,
        Stripped,
        Overwritten,
        CustomInit,
    )
    for model in untrusted:
        assert model.trusted_row_construction() is False, model.__name__
    assert CustomInit(id=1, name="c").name == "c"
    assert Validated(id=1, name="lower").name == "LOWER"


def test_trusted_build_equals_init_build():
    row = {
        "id": 1,
        "name": "n",
        "amount": decimal.Decimal("1.50"),
        "ratio": 2.5,
        "flag": False,
        "day": datetime.date(2021, 2, 3),
        "stamp": datetime.datetime(2021, 2, 3, 4, 5, 6),
        "payload": {"a": [1, 2]},
        "uid": uuid.UUID(int=5),
        "color": Color.BLUE,
        "blob": b"bytes",
        "note": None,
    }
    for excluded in (frozenset(), frozenset({"note", "ratio"})):
        values = {k: v for k, v in row.items() if k not in excluded}
        trusted = Plain._construct_from_row(excluded, values)
        built = Plain._construct_with_excluded(excluded, **values)
        assert trusted.__dict__ == built.__dict__
        assert trusted.__pydantic_fields_set__ == built.__pydantic_fields_set__
        assert trusted.model_dump() == built.model_dump()


@pytest.mark.asyncio
async def test_loaded_rows_skip_init_for_eligible_models(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", False)
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Plain.objects.create(name="a", amount=decimal.Decimal("1"))
        counter = ConstructCounter(Plain._construct_with_excluded)
        monkeypatch.setattr(Plain, "_construct_with_excluded", counter)
        loaded = await Plain.objects.all()
    assert counter.calls == 0
    assert loaded[0].name == "a" and loaded[0].saved
    Plain._construct_with_excluded(frozenset(), id=9, name="x", amount=1)
    assert counter.calls == 1


@pytest.mark.asyncio
async def test_trusted_instances_behave_like_built_ones(connected):
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Plain.objects.create(name="a", amount=decimal.Decimal("1"))
        item = await Plain.objects.get(name="a")
        same = await Plain.objects.get(name="a")
        assert item == same and hash(item) == hash(same)
        item.name = "b"
        assert not item.saved
        await item.update()
        assert (await Plain.objects.get(id=item.id)).name == "b"
        copy = item.model_copy()
        assert copy.name == "b" and copy.id == item.id
        restored = pickle.loads(pickle.dumps(item))
        assert restored.model_dump() == item.model_dump()
        partial = await Plain.objects.exclude_fields(["note", "ratio"]).get(id=item.id)
        assert partial.note is None and partial.ratio is None


class WrongConstruct:
    def __init__(self, original):
        self.original = original

    def __call__(self, excluded, values):
        instance = self.original(excluded, values)
        instance.__dict__["name"] = "tampered"
        return instance


@pytest.mark.asyncio
async def test_cross_check_detects_trusted_mismatch(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", True)
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Plain.objects.create(name="a", amount=decimal.Decimal("1"))
        monkeypatch.setattr(
            Plain, "_construct_from_row", WrongConstruct(Plain._construct_from_row)
        )
        with pytest.raises(AssertionError, match="Trusted row construction"):
            await Plain.objects.all()


def test_reverse_relation_resets_eligibility():
    assert EARLY_TRUSTED_BEFORE_RELATION is True
    assert Early.flat_row_model() is False


@pytest.mark.asyncio
async def test_excluding_required_field_still_raises(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", False)
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Plain.objects.create(name="a", amount=decimal.Decimal("1"))
        with pytest.raises(pydantic.ValidationError):
            await Plain.objects.exclude_fields(["name"]).all()


class RetypedConstruct:
    def __init__(self, original):
        self.original = original

    def __call__(self, excluded, values):
        instance = self.original(excluded, values)
        instance.__dict__["flag"] = int(instance.__dict__["flag"])
        return instance


@pytest.mark.asyncio
async def test_cross_check_detects_equal_values_of_other_type(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", True)
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Plain.objects.create(name="a", amount=decimal.Decimal("1"))
        monkeypatch.setattr(
            Plain, "_construct_from_row", RetypedConstruct(Plain._construct_from_row)
        )
        with pytest.raises(AssertionError, match="Trusted row construction"):
            await Plain.objects.all()


@pytest.mark.asyncio
async def test_flat_queries_skip_from_row_and_yield_between_chunks(
    connected, monkeypatch
):
    monkeypatch.setattr(queryset, "FLAT_ROWS_CHUNK", 2)
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Plain.objects.bulk_create(
            [Plain(name=f"n{i}", amount=decimal.Decimal(i)) for i in range(5)]
        )
        sleep = mock.AsyncMock()
        monkeypatch.setattr(queryset.asyncio, "sleep", sleep)
        counter = ConstructCounter(Plain.from_row)
        monkeypatch.setattr(Plain, "from_row", counter)
        loaded = await Plain.objects.order_by("id").all()
        assert counter.calls == 0
        assert sleep.await_count == 2
        assert [item.name for item in loaded] == [f"n{i}" for i in range(5)]
        assert all(item.saved for item in loaded)
        assert loaded[0].model_dump(exclude={"uid"})["name"] == "n0"
        pets = await Pet.objects.select_related("owner").all()
        assert pets == []


@pytest.mark.asyncio
async def test_rows_without_related_values_skip_init(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", False)
    async with base_ormar_config.database.transaction(force_rollback=True):
        owner = await Owner.objects.create()
        await Pet.objects.create(owner=None)
        await Pet.objects.create(owner=owner)
        counter = ConstructCounter(Pet._construct_with_excluded)
        monkeypatch.setattr(Pet, "_construct_with_excluded", counter)
        pets = await Pet.objects.order_by("id").all()
        assert counter.calls == 1
        assert pets[0].owner is None
        assert pets[1].owner.pk == owner.pk
        assert pets[1] in (await Owner.objects.select_related("pets").get()).pets
