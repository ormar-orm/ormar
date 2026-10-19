# type: ignore
import base64
import datetime
import decimal
import enum
import hashlib
import uuid
from typing import Any, Optional

import pytest

import ormar
from ormar import ModelDefinitionError, NoMatch
from ormar.fields.sqlalchemy_encrypted import EncryptedString
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()
default_fernet = dict(
    encrypt_secret="asd123", encrypt_backend=ormar.EncryptBackends.FERNET
)


class Color(enum.Enum):
    RED = 1
    BLUE = "blue"


class DummyBackend(ormar.fields.EncryptBackend):
    def _initialize_backend(self, secret_key: bytes) -> None:
        pass

    def encrypt(self, value: Any) -> str:
        return value

    def decrypt(self, value: Any) -> str:
        return value


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, **default_fernet)
    uuid_test = ormar.UUID(default=uuid.uuid4, uuid_format="string")
    uuid_test2 = ormar.UUID(nullable=True, uuid_format="string")
    password: str = ormar.String(
        max_length=128,
        encrypt_secret="udxc32",
        encrypt_backend=ormar.EncryptBackends.HASH,
    )
    birth_year: int = ormar.Integer(
        nullable=True,
        encrypt_secret="secure89key%^&psdijfipew",
        encrypt_backend=ormar.EncryptBackends.FERNET,
    )
    test_text: str = ormar.Text(default="", **default_fernet)
    test_bool: bool = ormar.Boolean(nullable=False, **default_fernet)
    test_float: float = ormar.Float(**default_fernet)
    test_float2: Optional[float] = ormar.Float(nullable=True, **default_fernet)
    test_datetime = ormar.DateTime(default=datetime.datetime.now, **default_fernet)
    test_date = ormar.Date(default=datetime.date.today, **default_fernet)
    test_time = ormar.Time(default=datetime.time, **default_fernet)
    test_json = ormar.JSON(default={}, **default_fernet)
    test_bigint: int = ormar.BigInteger(default=0, **default_fernet)
    test_smallint: int = ormar.SmallInteger(default=0, **default_fernet)
    test_decimal = ormar.Decimal(scale=2, precision=10, **default_fernet)
    test_decimal2 = ormar.Decimal(max_digits=10, decimal_places=2, **default_fernet)
    test_bytes = ormar.LargeBinary(max_length=100, **default_fernet)
    test_b64bytes = ormar.LargeBinary(
        max_length=100, represent_as_base64_str=True, **default_fernet
    )
    custom_backend: str = ormar.String(
        max_length=200,
        encrypt_secret="asda8",
        encrypt_backend=ormar.EncryptBackends.CUSTOM,
        encrypt_custom_backend=DummyBackend,
    )


class Hash(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="hashes")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(
        max_length=128,
        encrypt_secret="udxc32",
        encrypt_backend=ormar.EncryptBackends.HASH,
    )


class Filter(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="filters")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, **default_fernet)
    hash = ormar.ForeignKey(Hash)


class Paint(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="paints")

    id: int = ormar.Integer(primary_key=True)
    color = ormar.Enum(enum_class=Color, nullable=True, **default_fernet)


class Report(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="reports")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    filters = ormar.ManyToMany(Filter)


create_test_database = init_tests(base_ormar_config)


def test_error_on_encrypted_pk():
    with pytest.raises(ModelDefinitionError):

        class Wrong(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="wrongs")

            id: int = ormar.Integer(
                primary_key=True,
                encrypt_secret="asd123",
                encrypt_backend=ormar.EncryptBackends.FERNET,
            )


def test_error_on_encrypted_relation():
    with pytest.raises(ModelDefinitionError):

        class Wrong2(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="wrongs2")

            id: int = ormar.Integer(primary_key=True)
            author = ormar.ForeignKey(
                Author,
                encrypt_secret="asd123",
                encrypt_backend=ormar.EncryptBackends.FERNET,
            )


def test_error_on_encrypted_m2m_relation():
    with pytest.raises(ModelDefinitionError):

        class Wrong3(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="wrongs3")

            id: int = ormar.Integer(primary_key=True)
            author = ormar.ManyToMany(
                Author,
                encrypt_secret="asd123",
                encrypt_backend=ormar.EncryptBackends.FERNET,
            )


def test_wrong_backend():
    with pytest.raises(ModelDefinitionError):

        class Wrong3(ormar.Model):
            ormar_config = base_ormar_config.copy(tablename="wrongs3")

            id: int = ormar.Integer(primary_key=True)
            author = ormar.Integer(
                encrypt_secret="asd123",
                encrypt_backend=ormar.EncryptBackends.CUSTOM,
                encrypt_custom_backend="aa",
            )


def test_db_structure():
    assert Author.ormar_config.table.c.get("name").type.__class__ == EncryptedString


@pytest.mark.asyncio
async def test_save_and_retrieve():
    async with base_ormar_config.database:
        test_uuid = uuid.uuid4()
        await Author(
            name="Test",
            birth_year=1988,
            password="test123",
            uuid_test=test_uuid,
            test_float=1.2,
            test_bool=True,
            test_decimal=3.57,
            test_decimal2=decimal.Decimal(5.5),
            test_json=dict(aa=12),
            custom_backend="test12",
            test_bytes=b"test",
            test_b64bytes=b"test2",
        ).save()
        author = await Author.objects.get()

        assert author.name == "Test"
        assert author.birth_year == 1988
        password = (
            "03e4a4d513e99cb3fe4ee3db282c053daa3f3572b849c3868939a306944ad5c08"
            "22b50d4886e10f4cd418c3f2df3ceb02e2e7ac6e920ae0c90f2dedfc8fa16e2"
        )
        assert author.password == password
        assert author.uuid_test == test_uuid
        assert author.uuid_test2 is None
        assert author.test_datetime.date() == datetime.date.today()
        assert author.test_date == datetime.date.today()
        assert author.test_text == ""
        assert author.test_float == 1.2
        assert author.test_float2 is None
        assert author.test_bigint == 0
        assert author.test_json == {"aa": 12}
        assert float(author.test_decimal) == 3.57
        assert author.test_decimal2 == 5.5
        assert author.custom_backend == "test12"
        assert author.test_bytes == "test".encode("utf-8")
        assert author.test_b64bytes == "dGVzdDI="
        assert base64.b64decode(author.test_b64bytes) == b"test2"


@pytest.mark.asyncio
async def test_fernet_filters_nomatch():
    async with base_ormar_config.database:
        await Filter(name="test1").save()
        await Filter(name="test1").save()

        filters = await Filter.objects.all()
        assert filters[0].name == filters[1].name == "test1"

        with pytest.raises(NoMatch):
            await Filter.objects.get(name="test1")

        assert await Filter.objects.get_or_none(name="test1") is None


@pytest.mark.asyncio
async def test_hash_filters_works():
    async with base_ormar_config.database:
        await Hash(name="test1").save()
        await Hash(name="test2").save()

        secret = hashlib.sha256("udxc32".encode()).digest()
        secret = base64.urlsafe_b64encode(secret)
        hashed_test1 = hashlib.sha512(secret + "test1".encode()).hexdigest()

        hash1 = await Hash.objects.get(name="test1")
        assert hash1.name == hashed_test1

        with pytest.raises(NoMatch):
            await Filter.objects.get(name__icontains="test")


@pytest.mark.asyncio
async def test_related_model_fields_properly_decrypted():
    async with base_ormar_config.database:
        hash1 = await Hash(name="test1").save()
        report = await Report.objects.create(name="Report1")
        await report.filters.create(name="test1", hash=hash1)
        await report.filters.create(name="test2")

        report2 = await Report.objects.select_related("filters").get()
        assert report2.filters[0].name == "test1"
        assert report2.filters[1].name == "test2"

        secret = hashlib.sha256("udxc32".encode()).digest()
        secret = base64.urlsafe_b64encode(secret)
        hashed_test1 = hashlib.sha512(secret + "test1".encode()).hexdigest()

        report2 = await Report.objects.select_related("filters__hash").get()
        assert report2.filters[0].name == "test1"
        assert report2.filters[0].hash.name == hashed_test1


@pytest.mark.asyncio
async def test_encrypted_enum_round_trip():
    async with base_ormar_config.database:
        red = await Paint.objects.create(color=Color.RED)
        empty = await Paint.objects.create(color=None)
        assert (await Paint.objects.get(id=red.id)).color == Color.RED
        assert (await Paint.objects.get(id=empty.id)).color is None

        red.color = Color.BLUE
        await red.update()
        assert (await Paint.objects.get(id=red.id)).color == Color.BLUE

        await Paint.objects.filter(id=red.id).update(color=Color.RED)
        assert (await Paint.objects.get(id=red.id)).color == Color.RED
