from typing import Optional

import pytest
import pytest_asyncio

import ormar
from tests import cross_check
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class User(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="oo_users")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Profile(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="oo_profiles")

    id: int = ormar.Integer(primary_key=True)
    bio: Optional[str] = ormar.String(max_length=50, nullable=True)
    user: Optional[User] = ormar.OneToOne(User)


class Passport(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="oo_passports")

    user: User = ormar.OneToOne(User, primary_key=True, related_name="passport")
    number: str = ormar.String(max_length=20)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def clean():
    async with base_ormar_config.database:
        for model in (Passport, Profile, User):
            await model.objects.delete(each=True)
        yield


def test_one_to_one_definition():
    column = Profile.ormar_config.table.c.user
    assert column.unique is True
    reverse = User.ormar_config.model_fields["profile"]
    assert reverse.virtual and reverse.kind.many is False
    assert Profile.ormar_config.model_fields["user"].kind.many is False
    assert User.ormar_config.model_fields["passport"].kind.many is False
    assert Passport.ormar_config.table.c.user.primary_key is True
    assert Profile.ormar_config.model_fields["bio"].kind is None


@pytest.mark.asyncio
async def test_reverse_side_is_a_single_model(monkeypatch):
    monkeypatch.setattr(cross_check, "ENABLED", False)
    ann = await User.objects.create(name="Ann")
    bob = await User.objects.create(name="Bob")
    await Profile.objects.create(bio="hi", user=ann)
    await Passport.objects.create(user=ann, number="P1")
    loaded = (
        await User.objects.select_related(["profile", "passport"])
        .order_by("name")
        .all()
    )
    assert loaded[0].profile.bio == "hi"
    assert loaded[0].passport.number == "P1"
    assert loaded[1].profile is None
    assert loaded[1].passport is None
    fetched = await User.objects.prefetch_related(["profile", "passport"]).get(
        name="Ann"
    )
    assert fetched.profile.bio == "hi"
    assert fetched.passport.number == "P1"
    profile = await Profile.objects.select_related("user").get()
    assert profile.user.name == "Ann"
    assert (await Profile.objects.prefetch_related("user").get()).user.name == "Ann"
    assert [u.name for u in await User.objects.filter(profile__bio="hi").all()] == [
        "Ann"
    ]
    dumped = loaded[0].model_dump()
    assert dumped["profile"]["bio"] == "hi"
    assert dumped["passport"]["number"] == "P1"
    assert bob.profile is None


@pytest.mark.asyncio
async def test_one_to_one_assignment_and_uniqueness():
    ann = await User.objects.create(name="Ann")
    first = await Profile.objects.create(bio="first", user=ann)
    with pytest.raises(Exception):
        await Profile.objects.create(bio="second", user=ann)
    replacement = Profile(bio="replacement")
    ann.profile = replacement
    assert ann.profile is replacement
    assert replacement.user.name == "Ann"
    await first.delete()
    await replacement.save()
    assert (await Profile.objects.select_related("user").get()).user.name == "Ann"


def test_one_to_one_pydantic_models():
    reverse_type = User.get_pydantic().model_fields["profile"].annotation
    assert "list" not in str(reverse_type).lower()
    flat = User.get_pydantic(flatten_fields={"profile"})
    assert flat(name="x", profile=3).profile == 3


def test_default_related_name_is_singular():
    assert "profile" in User.ormar_config.model_fields
    assert "profiles" not in User.ormar_config.model_fields
