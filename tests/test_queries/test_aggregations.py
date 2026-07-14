from typing import Optional

import pytest
import pytest_asyncio

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class User(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Task(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tasks")

    id: int = ormar.Integer(primary_key=True)
    user: Optional[User] = ormar.ForeignKey(User, related_name="tasks")
    title: str = ormar.String(max_length=100)
    price: Optional[int] = ormar.Integer(nullable=True)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True, scope="function")
async def cleanup():
    yield
    async with base_ormar_config.database:
        await Task.objects.delete(each=True)
        await User.objects.delete(each=True)


async def seed():
    u1 = await User(name="Alice").save()
    u2 = await User(name="Bob").save()
    await User(name="Carol").save()  # no tasks
    await Task(title="a1", price=10, user=u1).save()
    await Task(title="a2", price=20, user=u1).save()
    await Task(title="b1", price=5, user=u2).save()
    return u1, u2


def test_aggregate_value_objects():
    c = ormar.Count("tasks")
    assert c.function_name == "count"
    assert c.field == "tasks"
    assert c.distinct is False

    cd = ormar.Count("tasks", distinct=True)
    assert cd.distinct is True

    assert ormar.Sum("tasks__price").function_name == "sum"
    assert ormar.Avg("tasks__price").function_name == "avg"
    assert ormar.Min("tasks__price").function_name == "min"
    assert ormar.Max("tasks__price").function_name == "max"
    assert ormar.Sum("tasks__price").field == "tasks__price"


@pytest.mark.asyncio
async def test_annotate_count_via_values():
    async with base_ormar_config.database:
        await seed()
        rows = (
            await User.objects.annotate(task_count=ormar.Count("tasks"))
            .order_by("name")
            .values(["name", "task_count"])
        )
        assert rows == [
            {"name": "Alice", "task_count": 2},
            {"name": "Bob", "task_count": 1},
            {"name": "Carol", "task_count": 0},
        ]


@pytest.mark.asyncio
async def test_annotate_count_distinct_with_column():
    async with base_ormar_config.database:
        await seed()
        rows = (
            await User.objects.annotate(
                task_count=ormar.Count("tasks__id", distinct=True)
            )
            .order_by("name")
            .values(["name", "task_count"])
        )
        assert rows == [
            {"name": "Alice", "task_count": 2},
            {"name": "Bob", "task_count": 1},
            {"name": "Carol", "task_count": 0},
        ]


@pytest.mark.asyncio
async def test_annotate_count_distinct_without_column():
    async with base_ormar_config.database:
        await seed()
        rows = (
            await User.objects.annotate(task_count=ormar.Count("tasks", distinct=True))
            .order_by("name")
            .values(["name", "task_count"])
        )
        assert rows == [
            {"name": "Alice", "task_count": 2},
            {"name": "Bob", "task_count": 1},
            {"name": "Carol", "task_count": 0},
        ]
