from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar.exceptions import QueryDefinitionError
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


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="posts")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    tags = ormar.ManyToMany(Tag, related_name="posts")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True, scope="function")
async def cleanup():
    yield
    async with base_ormar_config.database:
        await Task.objects.delete(each=True)
        await User.objects.delete(each=True)
        await Post.objects.delete(each=True)
        await Tag.objects.delete(each=True)


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


@pytest.mark.asyncio
async def test_order_by_annotation():
    async with base_ormar_config.database:
        await seed()
        names = (
            await User.objects.annotate(task_count=ormar.Count("tasks"))
            .order_by("-task_count")
            .values_list("name", flatten=True)
        )
        assert names == ["Alice", "Bob", "Carol"]

        names_asc = (
            await User.objects.annotate(task_count=ormar.Count("tasks"))
            .order_by("task_count")
            .values_list("name", flatten=True)
        )
        assert names_asc == ["Carol", "Bob", "Alice"]


@pytest.mark.asyncio
async def test_having_on_annotation():
    async with base_ormar_config.database:
        await seed()
        rows = (
            await User.objects.annotate(task_count=ormar.Count("tasks"))
            .having(task_count__gt=1)
            .values_list("name", flatten=True)
        )
        assert rows == ["Alice"]

        rows_zero = (
            await User.objects.annotate(task_count=ormar.Count("tasks"))
            .having(task_count__gte=1)
            .order_by("name")
            .values_list("name", flatten=True)
        )
        assert rows_zero == ["Alice", "Bob"]


def test_having_with_unsupported_operator():
    with pytest.raises(QueryDefinitionError):
        User.objects.annotate(task_count=ormar.Count("tasks")).having(
            task_count__contains=1
        )


@pytest.mark.asyncio
async def test_sum_avg_min_max_annotations():
    async with base_ormar_config.database:
        await seed()
        rows = (
            await User.objects.annotate(
                total=ormar.Sum("tasks__price"),
                avg_price=ormar.Avg("tasks__price"),
                cheapest=ormar.Min("tasks__price"),
                dearest=ormar.Max("tasks__price"),
            )
            .filter(name="Alice")
            .values(["name", "total", "cheapest", "dearest"])
        )
        assert rows == [{"name": "Alice", "total": 30, "cheapest": 10, "dearest": 20}]

        # parent with no children => NULL (not 0) for non-count aggregates
        carol = (
            await User.objects.annotate(total=ormar.Sum("tasks__price"))
            .filter(name="Carol")
            .values(["name", "total"])
        )
        assert carol == [{"name": "Carol", "total": None}]


@pytest.mark.asyncio
async def test_count_m2m_annotation():
    async with base_ormar_config.database:
        t1 = await Tag(name="t1").save()
        t2 = await Tag(name="t2").save()
        p1 = await Post(title="p1").save()
        p2 = await Post(title="p2").save()
        await p1.tags.add(t1)
        await p1.tags.add(t2)
        await p2.tags.add(t1)
        rows = (
            await Post.objects.annotate(tag_count=ormar.Count("tags"))
            .order_by("title")
            .values(["title", "tag_count"])
        )
        assert rows == [
            {"title": "p1", "tag_count": 2},
            {"title": "p2", "tag_count": 1},
        ]


@pytest.mark.asyncio
async def test_qualified_m2m_aggregate_raises():
    async with base_ormar_config.database:
        with pytest.raises(QueryDefinitionError):
            await Post.objects.annotate(x=ormar.Sum("tags__id")).values(["title", "x"])


@pytest.mark.asyncio
async def test_annotation_with_select_related_and_limit():
    async with base_ormar_config.database:
        await seed()
        # select_related hydrates tasks; annotation count stays correct and 1 row/user
        users = (
            await User.objects.select_related("tasks")
            .annotate(task_count=ormar.Count("tasks"))
            .order_by("-task_count")
            .limit(2)
            .all()
        )
        assert [u.name for u in users] == ["Alice", "Bob"]
        # ascending order differs from insertion/pk order (Alice, Bob, Carol),
        # so this discriminates a genuinely applied sort from an inert one
        users_asc = (
            await User.objects.select_related("tasks")
            .annotate(task_count=ormar.Count("tasks"))
            .order_by("task_count")
            .limit(2)
            .all()
        )
        assert [u.name for u in users_asc] == ["Carol", "Bob"]
        # offset skips the top-ranked row (Alice); an inert ORDER BY combined
        # with offset would instead skip Bob and keep Alice in the results
        users_offset = (
            await User.objects.select_related("tasks")
            .annotate(task_count=ormar.Count("tasks"))
            .order_by("-task_count")
            .offset(1)
            .limit(2)
            .all()
        )
        assert [u.name for u in users_offset] == ["Bob", "Carol"]
        # values() with limit + annotation order
        rows = (
            await User.objects.annotate(task_count=ormar.Count("tasks"))
            .order_by("-task_count")
            .limit(1)
            .values(["name", "task_count"])
        )
        assert rows == [{"name": "Alice", "task_count": 2}]


@pytest.mark.asyncio
async def test_annotate_values_without_field_subset():
    async with base_ormar_config.database:
        await seed()
        rows = (
            await User.objects.annotate(task_count=ormar.Count("tasks"))
            .order_by("name")
            .values()
        )
        assert len(rows) == 3
        by_name = {row["name"]: row for row in rows}
        assert by_name["Alice"]["task_count"] == 2
        assert by_name["Bob"]["task_count"] == 1
        assert by_name["Carol"]["task_count"] == 0
        # regular (non-annotation) columns are still present too
        assert "id" in by_name["Alice"]


@pytest.mark.asyncio
async def test_having_with_select_related_and_limit():
    async with base_ormar_config.database:
        await seed()  # Alice (pk 1, 2 tasks), Bob (pk 2, 1 task), Carol (pk 3, 0 tasks)
        dave = await User(name="Dave").save()  # pk 4
        for i in range(3):
            await Task(title=f"d{i}", price=1, user=dave).save()
        # pk order is Alice(2 tasks), Bob(1 task), Carol(0 tasks), Dave(3 tasks).
        # Only Alice and Dave satisfy `having(task_count__gt=1)`. A pagination
        # subquery that picks the first-by-pk 2 parents *before* having is
        # applied would pick Alice and Bob, then the outer filter would drop
        # Bob (1 task), silently returning only [Alice] instead of the correct
        # [Alice, Dave] - this is discriminating against the Fix-1 regression.
        users = (
            await User.objects.select_related("tasks")
            .annotate(task_count=ormar.Count("tasks"))
            .having(task_count__gt=1)
            .limit(2)
            .all()
        )
        assert sorted(u.name for u in users) == ["Alice", "Dave"]
        assert len(users) == 2


def test_annotate_name_collision_raises():
    with pytest.raises(QueryDefinitionError):
        User.objects.annotate(name=ormar.Count("tasks"))


@pytest.mark.asyncio
async def test_having_on_unannotated_name_raises():
    async with base_ormar_config.database:
        await seed()
        with pytest.raises(QueryDefinitionError):
            await User.objects.having(missing__gt=1).all()


@pytest.mark.asyncio
async def test_annotation_independent_of_outer_relation_filter():
    async with base_ormar_config.database:
        u1, _ = await seed()
        await Task(title="a3", price=50, user=u1).save()  # Alice now has 3 tasks
        rows = (
            await User.objects.annotate(task_count=ormar.Count("tasks"))
            .filter(tasks__price__gt=15)
            .order_by("name")
            .values(["name", "task_count"])
        )
        # The outer `.filter(tasks__price__gt=15)` joins `tasks` again (aliased)
        # purely to filter rows; it matches 2 of Alice's tasks (price 20 and
        # 50), so plain SQL join semantics duplicate her row in the flat
        # `.values()` result (pre-existing ormar behaviour, unrelated to
        # annotations - see the `distinct` flag on `QuerySet.count()`).
        # What this test locks down is that `task_count` is unaffected by
        # that outer filter: it is computed by a separate derived-table
        # subquery grouped over the raw child table, so it is `3` (ALL of
        # Alice's tasks) on every duplicated row, not `2` (only the tasks
        # matching the outer filter).
        assert rows == [
            {"name": "Alice", "task_count": 3},
            {"name": "Alice", "task_count": 3},
        ]

        # Same independence property, without the duplication confound:
        # a scalar-column filter (`name="Alice"`) matches Alice's row once,
        # so no join-induced duplication occurs, yet `task_count` is still
        # `3` (ALL of Alice's tasks). A to-many-relation filter duplicates
        # rows (asserted above); a scalar filter does not (asserted below) -
        # in both cases `task_count` reflects every task, independent of
        # the outer filter.
        scalar_filtered_rows = (
            await User.objects.annotate(task_count=ormar.Count("tasks"))
            .filter(name="Alice")
            .values(["name", "task_count"])
        )
        assert scalar_filtered_rows == [{"name": "Alice", "task_count": 3}]
