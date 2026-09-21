import asyncio
import base64
import datetime
import os
import uuid
from enum import Enum
from typing import Literal, Optional, get_args

import pydantic
import pytest
import sqlalchemy

import ormar
from ormar.exceptions import ModelError, NoMatch, QueryDefinitionError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class JsonSample(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="jsons")

    id: int = ormar.Integer(primary_key=True)
    test_json = ormar.JSON(nullable=True)


blob = b"test"
blob2 = b"test2icac89uc98"


class LargeBinarySample(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="my_bolbs")

    id: int = ormar.Integer(primary_key=True)
    test_binary: bytes = ormar.LargeBinary(max_length=100000)


blob3 = os.urandom(64)
blob4 = os.urandom(100)


class LargeBinaryStr(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="my_str_bolbs")

    id: int = ormar.Integer(primary_key=True)
    test_binary: str = ormar.LargeBinary(
        max_length=100000, represent_as_base64_str=True
    )


class LargeBinaryNullableStr(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="my_str_bolbs2")

    id: int = ormar.Integer(primary_key=True)
    test_binary: str = ormar.LargeBinary(
        max_length=100000,
        represent_as_base64_str=True,
        nullable=True,
    )


class UUIDSample(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="uuids")

    id: uuid.UUID = ormar.UUID(primary_key=True, default=uuid.uuid4)
    test_text: str = ormar.Text()


class UUIDSample2(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="uuids2")

    id: uuid.UUID = ormar.UUID(
        primary_key=True, default=uuid.uuid4, uuid_format="string"
    )
    test_text: str = ormar.Text()


class User(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, default="")


class User2(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users2")

    id: str = ormar.String(primary_key=True, max_length=100)
    name: str = ormar.String(max_length=100, default="")


class Product(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="product")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    rating: int = ormar.Integer(minimum=1, maximum=5)
    in_stock: bool = ormar.Boolean(default=False)
    last_delivery: datetime.date = ormar.Date(default=datetime.date.today)


class CountryNameEnum(Enum):
    CANADA = "Canada"
    ALGERIA = "Algeria"
    USA = "United States"
    BELIZE = "Belize"


class CountryCodeEnum(int, Enum):
    MINUS_TEN = -10
    ONE = 1
    TWO_HUNDRED_THIRTEEN = 213
    THOUSAND_TWO_HUNDRED = 1200


class Country(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="country")

    id: int = ormar.Integer(primary_key=True)
    name: CountryNameEnum = ormar.Enum(enum_class=CountryNameEnum, default="Canada")
    taxed: bool = ormar.Boolean(default=True)
    country_code: int = ormar.Enum(enum_class=CountryCodeEnum, default=1)


class NullableCountry(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="country2")

    id: int = ormar.Integer(primary_key=True)
    name: Optional[CountryNameEnum] = ormar.Enum(
        enum_class=CountryNameEnum, nullable=True
    )


class NotNullableCountry(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="country3")

    id: int = ormar.Integer(primary_key=True)
    name: CountryNameEnum = ormar.Enum(enum_class=CountryNameEnum, nullable=False)


class UserRole(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="user_roles")

    id: int = ormar.Integer(primary_key=True)
    mode: Literal["user", "manager", "admin"] = ormar.String(
        max_length=32, choices=["user", "manager", "admin"]
    )
    optional_mode: Optional[Literal["user", "manager", "admin"]] = ormar.String(
        max_length=32,
        choices=["user", "manager", "admin"],
        nullable=True,
    )


create_test_database = init_tests(base_ormar_config)


def test_model_class():
    assert list(User.ormar_config.model_fields.keys()) == ["id", "name"]
    assert issubclass(
        User.ormar_config.model_fields["id"].__class__, pydantic.fields.FieldInfo
    )
    assert User.ormar_config.model_fields["id"].primary_key is True
    assert isinstance(User.ormar_config.model_fields["name"], pydantic.fields.FieldInfo)
    assert User.ormar_config.model_fields["name"].max_length == 100
    assert isinstance(User.ormar_config.table, sqlalchemy.Table)


def test_wrong_field_name():
    with pytest.raises(ModelError):
        User(non_existing_pk=1)


def test_string_choices_field_definition():
    field = UserRole.ormar_config.model_fields["mode"]

    assert isinstance(field.column_type, sqlalchemy.String)
    assert field.column_type.length == 32
    assert field.choices == ["user", "manager", "admin"]
    assert get_args(field.__pydantic_type__) == ("user", "manager", "admin")


def test_string_field_rejects_non_string_choices():
    with pytest.raises(ormar.ModelDefinitionError):
        ormar.String(max_length=8, choices=["user", 1])  # type: ignore[list-item]


def test_model_pk():
    user = User(pk=1)
    assert user.pk == 1
    assert user.id == 1


@pytest.mark.asyncio
async def test_json_column():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await JsonSample.objects.create(test_json=dict(aa=12))
            await JsonSample.objects.create(test_json='{"aa": 12}')

            items = await JsonSample.objects.all()
            assert len(items) == 2
            assert items[0].test_json == dict(aa=12)
            assert items[1].test_json == dict(aa=12)

            items[0].test_json = "[1, 2, 3]"
            assert items[0].test_json == [1, 2, 3]


@pytest.mark.asyncio
async def test_binary_column():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await LargeBinarySample.objects.create(test_binary=blob)
            await LargeBinarySample.objects.create(test_binary=blob2)

            items = await LargeBinarySample.objects.all()
            assert len(items) == 2
            assert items[0].test_binary == blob
            assert items[1].test_binary == blob2

            items[0].test_binary = "test2icac89uc98"
            assert items[0].test_binary == b"test2icac89uc98"


@pytest.mark.asyncio
async def test_binary_str_column():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await LargeBinaryStr(test_binary=blob3).save()
            await LargeBinaryStr.objects.create(test_binary=blob4)

            items = await LargeBinaryStr.objects.all()
            assert len(items) == 2
            assert items[0].test_binary == base64.b64encode(blob3).decode()
            items[0].test_binary = base64.b64encode(blob4).decode()
            assert items[0].test_binary == base64.b64encode(blob4).decode()
            assert items[1].test_binary == base64.b64encode(blob4).decode()
            assert items[1].__dict__["test_binary"] == blob4


@pytest.mark.asyncio
async def test_binary_nullable_str_column():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await LargeBinaryNullableStr().save()
            await LargeBinaryNullableStr.objects.create()
            items = await LargeBinaryNullableStr.objects.all()
            assert len(items) == 2

            items[0].test_binary = blob3
            items[1].test_binary = blob4

            await LargeBinaryNullableStr.objects.bulk_update(items)
            items = await LargeBinaryNullableStr.objects.all()
            assert len(items) == 2
            assert items[0].test_binary == base64.b64encode(blob3).decode()
            items[0].test_binary = base64.b64encode(blob4).decode()
            assert items[0].test_binary == base64.b64encode(blob4).decode()
            assert items[1].test_binary == base64.b64encode(blob4).decode()
            assert items[1].__dict__["test_binary"] == blob4

            await LargeBinaryNullableStr.objects.bulk_create(
                [LargeBinaryNullableStr(), LargeBinaryNullableStr(test_binary=blob3)]
            )
            items = await LargeBinaryNullableStr.objects.all()
            assert len(items) == 4
            await items[0].update(test_binary=blob4)
            check_item = await LargeBinaryNullableStr.objects.get(id=items[0].id)
            assert check_item.test_binary == base64.b64encode(blob4).decode()


@pytest.mark.asyncio
async def test_uuid_column():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            u1 = await UUIDSample.objects.create(test_text="aa")
            u2 = await UUIDSample.objects.create(test_text="bb")

            items = await UUIDSample.objects.all()
            assert len(items) == 2

            assert isinstance(items[0].id, uuid.UUID)
            assert isinstance(items[1].id, uuid.UUID)

            assert items[0].id in (u1.id, u2.id)
            assert items[1].id in (u1.id, u2.id)

            assert items[0].id != items[1].id

            item = await UUIDSample.objects.filter(id=u1.id).get()
            assert item.id == u1.id

            item2 = await UUIDSample.objects.first()
            item3 = await UUIDSample.objects.get(pk=item2.id)
            assert item2.id == item3.id
            assert isinstance(item3.id, uuid.UUID)

            u3 = await UUIDSample2(**u1.model_dump()).save()

            u1_2 = await UUIDSample.objects.get(pk=u3.id)
            assert u1_2 == u1

            u4 = await UUIDSample2.objects.get(pk=u3.id)
            assert u3 == u4


@pytest.mark.asyncio
async def test_model_crud():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            users = await User.objects.all()
            assert users == []

            user = await User.objects.create(name="Tom")
            users = await User.objects.all()
            assert user.name == "Tom"
            assert user.pk is not None
            assert users == [user]

            lookup = await User.objects.get()
            assert lookup == user

            await user.update(name="Jane")
            users = await User.objects.all()
            assert user.name == "Jane"
            assert user.pk is not None
            assert users == [user]

            await user.delete()
            users = await User.objects.all()
            assert users == []


@pytest.mark.asyncio
async def test_model_get():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with pytest.raises(ormar.NoMatch):
                await User.objects.get()

            assert await User.objects.get_or_none() is None

            user = await User.objects.create(name="Tom")
            lookup = await User.objects.get()
            assert lookup == user

            user2 = await User.objects.create(name="Jane")
            await User.objects.create(name="Jane")
            with pytest.raises(ormar.MultipleMatches):
                await User.objects.get(name="Jane")

            same_user = await User.objects.get(pk=user2.id)
            assert same_user.id == user2.id
            assert same_user.pk == user2.pk

            assert await User.objects.order_by("-name").get() == user


@pytest.mark.asyncio
async def test_model_filter():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await User.objects.create(name="Tom")
            await User.objects.create(name="Jane")
            await User.objects.create(name="Lucy")

            user = await User.objects.get(name="Lucy")
            assert user.name == "Lucy"

            with pytest.raises(ormar.NoMatch):
                await User.objects.get(name="Jim")

            await Product.objects.create(name="T-Shirt", rating=5, in_stock=True)
            await Product.objects.create(name="Dress", rating=4)
            await Product.objects.create(name="Coat", rating=3, in_stock=True)

            product = await Product.objects.get(name__iexact="t-shirt", rating=5)
            assert product.pk is not None
            assert product.name == "T-Shirt"
            assert product.rating == 5
            assert product.last_delivery == datetime.datetime.now().date()

            products = await Product.objects.all(rating__gte=2, in_stock=True)
            assert len(products) == 2

            products = await Product.objects.all(name__icontains="T")
            assert len(products) == 2

            products = await Product.objects.exclude(rating__gte=4).all()
            assert len(products) == 1

            products = await Product.objects.exclude(rating__gte=4, in_stock=True).all()
            assert len(products) == 2

            products = await Product.objects.exclude(in_stock=True).all()
            assert len(products) == 1

            products = await Product.objects.exclude(name__icontains="T").all()
            assert len(products) == 1

            # Test escaping % character from icontains, contains, and iexact
            await Product.objects.create(name="100%-Cotton", rating=3)
            await Product.objects.create(name="Cotton-100%-Egyptian", rating=3)
            await Product.objects.create(name="Cotton-100%", rating=3)
            products = Product.objects.filter(name__iexact="100%-cotton")
            assert await products.count() == 1

            products = Product.objects.filter(name__contains="%")
            assert await products.count() == 3

            products = Product.objects.filter(name__icontains="%")
            assert await products.count() == 3


@pytest.mark.asyncio
async def test_wrong_query_contains_model():
    async with base_ormar_config.database:
        with pytest.raises(QueryDefinitionError):
            product = Product(name="90%-Cotton", rating=2)
            await Product.objects.filter(name__contains=product).count()


@pytest.mark.asyncio
async def test_model_exists():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await User.objects.create(name="Tom")
            assert await User.objects.filter(name="Tom").exists() is True
            assert await User.objects.filter(name="Jane").exists() is False


@pytest.mark.asyncio
async def test_model_count():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await User.objects.create(name="Tom")
            await User.objects.create(name="Jane")
            await User.objects.create(name="Lucy")

            assert await User.objects.count() == 3
            assert await User.objects.filter(name__icontains="T").count() == 1


@pytest.mark.asyncio
async def test_model_limit():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await User.objects.create(name="Tom")
            await User.objects.create(name="Jane")
            await User.objects.create(name="Lucy")

            assert len(await User.objects.limit(2).all()) == 2


@pytest.mark.asyncio
async def test_model_limit_with_filter():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await User.objects.create(name="Tom")
            await User.objects.create(name="Tom")
            await User.objects.create(name="Tom")

            assert (
                len(await User.objects.limit(2).filter(name__iexact="Tom").all()) == 2
            )


@pytest.mark.asyncio
async def test_offset():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await User.objects.create(name="Tom")
            await User.objects.create(name="Jane")

            users = await User.objects.offset(1).limit(1).all()
            assert users[0].name == "Jane"


@pytest.mark.asyncio
async def test_model_first():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with pytest.raises(ormar.NoMatch):
                await User.objects.first()

            assert await User.objects.first_or_none() is None

            tom = await User.objects.create(name="Tom")
            jane = await User.objects.create(name="Jane")

            assert await User.objects.first() == tom
            assert await User.objects.first(name="Jane") == jane
            assert await User.objects.filter(name="Jane").first() == jane
            with pytest.raises(NoMatch):
                await User.objects.filter(name="Lucy").first()

            assert await User.objects.first_or_none(name="Lucy") is None
            assert await User.objects.filter(name="Lucy").first_or_none() is None

            assert await User.objects.order_by("name").first() == jane


@pytest.mark.asyncio
async def test_model_last():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with pytest.raises(ormar.NoMatch):
                await User.objects.last()

            assert await User.objects.last_or_none() is None

            tom = await User.objects.create(name="Tom")
            jane = await User.objects.create(name="Jane")

            assert await User.objects.last() == jane
            assert await User.objects.last(name="Tom") == tom
            assert await User.objects.filter(name="Tom").last() == tom
            with pytest.raises(NoMatch):
                await User.objects.filter(name="Lucy").last()

            assert await User.objects.last_or_none(name="Lucy") is None
            assert await User.objects.filter(name="Lucy").last_or_none() is None

            assert await User.objects.order_by("name").last() == tom


@pytest.mark.asyncio
async def test_model_choices():
    """Test that enum work properly for various types of fields."""
    async with base_ormar_config.database:
        # Test valid enums values.
        await asyncio.gather(
            Country.objects.create(name="Canada", taxed=True, country_code=1),
            Country.objects.create(name="Algeria", taxed=True, country_code=213),
            Country.objects.create(name="Algeria"),
        )

        with pytest.raises(ValueError):
            name, taxed, country_code = "Saudi Arabia", True, 1
            await Country.objects.create(
                name=name, taxed=taxed, country_code=country_code
            )

        with pytest.raises(ValueError):
            name, taxed, country_code = "Algeria", True, 967
            await Country.objects.create(
                name=name, taxed=taxed, country_code=country_code
            )

        # test setting after init also triggers validation
        with pytest.raises(ValueError):
            name, taxed, country_code = "Algeria", True, 967
            country = Country()
            country.country_code = country_code

        with pytest.raises(ValueError):
            name, taxed, country_code = "Saudi Arabia", True, 1
            country = Country()
            country.name = name

        # check also update from queryset
        with pytest.raises(ValueError):
            await Country(name="Belize").save()
            await Country.objects.filter(name="Belize").update(name="Vietnam")


@pytest.mark.asyncio
async def test_nullable_field_model_enum():
    """Test that enum work properly for according to nullable setting"""
    async with base_ormar_config.database:
        c1 = await NullableCountry(name=None).save()
        assert c1.name is None

        with pytest.raises(ValueError):
            await NotNullableCountry(name=None).save()


@pytest.mark.asyncio
async def test_string_choices_field_validation():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            role = await UserRole.objects.create(mode="user", optional_mode=None)
            assert role.mode == "user"
            assert role.optional_mode is None

            role.optional_mode = "manager"
            assert role.optional_mode == "manager"

            role.mode = "admin"
            await role.update()
            refreshed = await UserRole.objects.get(pk=role.pk)
            assert refreshed.mode == "admin"
            assert refreshed.optional_mode == "manager"

            with pytest.raises(ValueError):
                UserRole(mode="guest")

            with pytest.raises(ValueError):
                role.mode = "guest"

            with pytest.raises(ValueError):
                await UserRole(mode="guest").save()

            with pytest.raises(ValueError):
                await UserRole.objects.filter(pk=role.pk).update(mode="guest")

            with pytest.raises(ValueError):
                role.optional_mode = "guest"


@pytest.mark.asyncio
async def test_start_and_end_filters():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await User.objects.create(name="Markos Uj")
            await User.objects.create(name="Maqua Bigo")
            await User.objects.create(name="maqo quidid")
            await User.objects.create(name="Louis Figo")
            await User.objects.create(name="Loordi Kami")
            await User.objects.create(name="Yuuki Sami")

            users = await User.objects.filter(name__startswith="Mar").all()
            assert len(users) == 1

            users = await User.objects.filter(name__istartswith="ma").all()
            assert len(users) == 3

            users = await User.objects.filter(name__istartswith="Maq").all()
            assert len(users) == 2

            users = await User.objects.filter(name__iendswith="AMI").all()
            assert len(users) == 2

            users = await User.objects.filter(name__endswith="Uj").all()
            assert len(users) == 1

            users = await User.objects.filter(name__endswith="igo").all()
            assert len(users) == 2


@pytest.mark.asyncio
async def test_get_and_first():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await User.objects.create(name="Tom")
            await User.objects.create(name="Jane")
            await User.objects.create(name="Lucy")
            await User.objects.create(name="Zack")
            await User.objects.create(name="Ula")

            user = await User.objects.get()
            assert user.name == "Ula"

            user = await User.objects.first()
            assert user.name == "Tom"

            await User2.objects.create(id="Tom", name="Tom")
            await User2.objects.create(id="Jane", name="Jane")
            await User2.objects.create(id="Lucy", name="Lucy")
            await User2.objects.create(id="Zack", name="Zack")
            await User2.objects.create(id="Ula", name="Ula")

            user = await User2.objects.get()
            assert user.name == "Zack"

            user = await User2.objects.first()
            assert user.name == "Jane"


def test_constraints():
    with pytest.raises(pydantic.ValidationError) as e:
        Product(name="T-Shirt", rating=50, in_stock=True)
    assert "Input should be less than or equal to 5 " in str(e.value)
