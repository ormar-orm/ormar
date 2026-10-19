import datetime
import decimal
import enum
import pickle
import uuid
from typing import Optional

import pydantic
import pytest
import pytest_asyncio

import ormar
from ormar.queryset import templates
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
    for model in (Pet, Owner, Validated, Encoded, Secret):
        assert model.trusted_row_construction() is False


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
