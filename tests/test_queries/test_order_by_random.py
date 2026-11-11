from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar.queryset.actions.order_action import RandomOrderAction
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()

ATTEMPTS = 20


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rnd_tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class User(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rnd_users")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    group: int = ormar.Integer()
    tags: Optional[list[Tag]] = ormar.ManyToMany(Tag)


class Note(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rnd_notes")

    id: int = ormar.Integer(primary_key=True)
    text: str = ormar.String(max_length=20)
    user: Optional[User] = ormar.ForeignKey(User, related_name="notes")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def users():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            tags = [
                await Tag.objects.create(name=f"tag{number}") for number in range(3)
            ]
            for number in range(10):
                user = await User.objects.create(name=f"user{number}", group=number % 2)
                for tag in tags:
                    await user.tags.add(tag)
                for note_number in range(10):
                    await Note.objects.create(text=f"note{note_number}", user=user)
            yield


async def orderings(fetch):
    return {tuple(await fetch()) for _ in range(ATTEMPTS)}


async def names(queryset):
    return [user.name for user in await queryset.all()]


@pytest.mark.asyncio
async def test_random_order_and_limit():
    everyone = await orderings(lambda: names(User.objects.order_by("?")))
    assert len(everyone) > 1
    assert all(
        sorted(ordering) == sorted(next(iter(everyone))) for ordering in everyone
    )

    picked = await orderings(lambda: names(User.objects.order_by("-?").limit(3)))
    assert len(picked) > 1
    assert all(len(set(ordering)) == 3 for ordering in picked)

    async def values_list():
        return await User.objects.order_by("?").values_list("name", flatten=True)

    assert len(await orderings(values_list)) > 1


@pytest.mark.asyncio
async def test_random_order_with_select_related_and_limit():

    async def picked():
        users = await (
            User.objects.select_related(["tags", "notes"]).order_by("?").limit(3).all()
        )
        assert [(len(user.tags), len(user.notes)) for user in users] == [(3, 10)] * 3
        return [user.name for user in users]

    assert len(await orderings(picked)) > 1


@pytest.mark.asyncio
async def test_random_first_get_and_slices():

    async def first():
        return [(await User.objects.order_by("?").first()).name]

    async def get():
        return [(await User.objects.order_by("?").get()).name]

    async def tail():
        return await names(User.objects.order_by("?")[-2:])

    assert len(await orderings(first)) > 1
    assert len(await orderings(get)) > 1
    assert len(await orderings(tail)) > 1


@pytest.mark.asyncio
async def test_random_order_after_a_field_order():

    async def grouped():
        users = await User.objects.order_by(["group", ormar.random()]).all()
        assert [user.group for user in users] == [0] * 5 + [1] * 5
        return [user.name for user in users]

    assert len(await orderings(grouped)) > 1
    assert len(User.objects.order_by("?").order_by("-?").order_bys) == 1
    assert len(User.objects.order_by(ormar.random()).order_by("?").order_bys) == 1
    assert isinstance(User.objects.order_by("?").order_bys[0], RandomOrderAction)
    assert repr(ormar.random()) == "ormar.random()"


@pytest.mark.asyncio
async def test_random_order_on_relations_and_prefetch():
    user = await User.objects.get(name="user0")

    async def related_notes():
        notes = await user.notes.order_by(ormar.random()).all()
        return [note.text for note in notes]

    async def prefetched_tags():
        users = await User.objects.prefetch_related(
            ormar.Prefetch("tags", queryset=Tag.objects.order_by(ormar.random()))
        ).all()
        return [tag.name for tag in users[0].tags]

    async def prefetched_notes():
        users = await User.objects.prefetch_related(
            ormar.Prefetch("notes", queryset=Note.objects.order_by("?"))
        ).all()
        return [note.text for note in users[0].notes]

    assert len(await orderings(related_notes)) > 1
    assert len(await orderings(prefetched_tags)) > 1
    assert len(await orderings(prefetched_notes)) > 1
