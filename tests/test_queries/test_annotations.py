import decimal
from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar import Avg, Case, Count, F, Func, Max, Min, Sum, When
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Club(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ann_clubs")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    fee: int = ormar.Integer(default=0)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ann_tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class User(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ann_users")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    score: int = ormar.Integer(default=0, name="user_score")
    club: Optional[Club] = ormar.ForeignKey(Club, nullable=True, related_name="users")
    tags: Optional[list[Tag]] = ormar.ManyToMany(Tag, related_name="users")


class Booking(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ann_bookings")

    id: int = ormar.Integer(primary_key=True)
    user: Optional[User] = ormar.ForeignKey(User, related_name="bookings")
    status: str = ormar.String(max_length=20)
    price: Optional[int] = ormar.Integer(nullable=True)


class Shelf(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ann_shelves")

    code: str = ormar.String(max_length=10, primary_key=True)
    number: int = ormar.Integer(primary_key=True)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ann_books")

    id: int = ormar.Integer(primary_key=True)
    pages: int = ormar.Integer(default=0)
    shelf: Optional[Shelf] = ormar.ForeignKey(Shelf, related_name="books")


create_test_database = init_tests(base_ormar_config)

ACTIVE = Count("bookings", filter={"status": "active"})


@pytest_asyncio.fixture()
async def users():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            chess = await Club.objects.create(name="chess", fee=10)
            await Club.objects.create(name="go", fee=5)
            red = await Tag.objects.create(name="red")
            blue = await Tag.objects.create(name="blue")
            alice = await User.objects.create(name="Alice", score=3, club=chess)
            bob = await User.objects.create(name="Bob", score=12)
            await User.objects.create(name="Carol", score=7, club=chess)
            await alice.tags.add(red)
            await alice.tags.add(blue)
            await bob.tags.add(red)
            for user, status, price in (
                (alice, "active", 10),
                (alice, "active", 20),
                (alice, "cancelled", 40),
                (bob, "active", 5),
                (bob, "cancelled", None),
            ):
                await Booking.objects.create(user=user, status=status, price=price)
            yield


@pytest_asyncio.fixture()
async def shelves():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            for code, number, pages in (
                ("a", 1, [10, 20]),
                ("a", 2, [5]),
                ("b", 1, []),
            ):
                shelf = await Shelf.objects.create(code=code, number=number)
                for count in pages:
                    await Book.objects.create(pages=count, shelf=shelf)
            yield


def by_name(models: list) -> dict:
    return {model.name: dict(ormar.annotations_of(model)) for model in models}


@pytest.mark.asyncio
async def test_annotate_count_via_values(users):
    rows = (
        await User.objects.annotate(booking_count=Count("bookings"))
        .order_by("name")
        .values(["name", "booking_count"])
    )
    assert rows == [
        {"name": "Alice", "booking_count": 3},
        {"name": "Bob", "booking_count": 2},
        {"name": "Carol", "booking_count": 0},
    ]
    for count in (
        Count("bookings__id", distinct=True),
        Count("bookings", distinct=True),
    ):
        rows = (
            await User.objects.annotate(booking_count=count)
            .order_by("name")
            .values_list("booking_count", flatten=True)
        )
        assert rows == [3, 2, 0]


@pytest.mark.asyncio
async def test_order_by_annotation(users):
    queryset = User.objects.annotate(booking_count=Count("bookings"))
    names = await queryset.order_by("-booking_count").values_list("name", flatten=True)
    assert names == ["Alice", "Bob", "Carol"]
    names = await queryset.order_by("booking_count").values_list("name", flatten=True)
    assert names == ["Carol", "Bob", "Alice"]


@pytest.mark.asyncio
async def test_annotate_with_filter_and_filter_on_aggregate(users):
    queryset = User.objects.annotate(active=ACTIVE).order_by(["-active", "id"])
    assert by_name(await queryset.all()) == {
        "Alice": {"active": 2},
        "Bob": {"active": 1},
        "Carol": {"active": 0},
    }
    assert await queryset.filter(ACTIVE > 1).values_list("name", flatten=True) == [
        "Alice"
    ]
    assert await queryset.filter(Count("bookings") >= 1).values_list(
        "name", flatten=True
    ) == ["Alice", "Bob"]


@pytest.mark.asyncio
async def test_sum_avg_min_max_annotations(users):
    rows = (
        await User.objects.annotate(
            total=Sum("bookings__price"),
            avg_price=Avg("bookings__price"),
            cheapest=Min("bookings__price"),
            dearest=Max("bookings__price"),
        )
        .order_by("name")
        .values(["name", "total", "avg_price", "cheapest", "dearest"])
    )
    assert rows[1:] == [
        {"name": "Bob", "total": 5, "avg_price": 5, "cheapest": 5, "dearest": 5},
        {
            "name": "Carol",
            "total": None,
            "avg_price": None,
            "cheapest": None,
            "dearest": None,
        },
    ]
    alice = rows[0]
    assert (alice["total"], alice["cheapest"], alice["dearest"]) == (70, 10, 40)
    assert round(alice["avg_price"], 2) == decimal.Decimal("23.33")
    assert type(alice["total"]) is int
    assert isinstance(alice["avg_price"], decimal.Decimal)


@pytest.mark.asyncio
async def test_count_many_to_many(users):
    queryset = User.objects.annotate(
        tag_count=Count("tags"), red=Count("tags", filter={"name": "red"})
    ).order_by("name")
    assert await queryset.values(["name", "tag_count", "red"]) == [
        {"name": "Alice", "tag_count": 2, "red": 1},
        {"name": "Bob", "tag_count": 1, "red": 1},
        {"name": "Carol", "tag_count": 0, "red": 0},
    ]
    tags = await Tag.objects.annotate(user_count=Count("users")).order_by("name").all()
    assert by_name(tags) == {"blue": {"user_count": 1}, "red": {"user_count": 2}}


@pytest.mark.asyncio
async def test_annotation_with_select_related_and_limit(users):
    queryset = User.objects.select_related("bookings").annotate(
        booking_count=Count("bookings")
    )
    found = await queryset.order_by("-booking_count").limit(2).all()
    assert [user.name for user in found] == ["Alice", "Bob"]
    assert [len(user.bookings) for user in found] == [3, 2]
    assert by_name(found) == {
        "Alice": {"booking_count": 3},
        "Bob": {"booking_count": 2},
    }
    found = await queryset.order_by("booking_count").limit(2).all()
    assert [user.name for user in found] == ["Carol", "Bob"]
    found = await queryset.order_by("-booking_count").offset(1).limit(2).all()
    assert [user.name for user in found] == ["Bob", "Carol"]
    rows = (
        await User.objects.annotate(booking_count=Count("bookings"))
        .order_by("-booking_count")
        .limit(1)
        .values(["name", "booking_count"])
    )
    assert rows == [{"name": "Alice", "booking_count": 3}]


@pytest.mark.asyncio
async def test_filter_on_aggregate_with_select_related_and_limit(users):
    dave = await User.objects.create(name="Dave")
    for _ in range(3):
        await Booking.objects.create(user=dave, status="active")
    found = (
        await User.objects.select_related("bookings")
        .annotate(booking_count=Count("bookings"))
        .filter(Count("bookings") > 2)
        .order_by("-booking_count")
        .limit(2)
        .all()
    )
    assert sorted(user.name for user in found) == ["Alice", "Dave"]


@pytest.mark.asyncio
async def test_annotate_values_without_field_subset(users):
    rows = (
        await User.objects.annotate(booking_count=Count("bookings"))
        .order_by("name")
        .values()
    )
    assert [row["booking_count"] for row in rows] == [3, 2, 0]
    assert rows[0]["name"] == "Alice" and "id" in rows[0]
    assert list(rows[0])[-1] == "booking_count"
    rows = (
        await User.objects.annotate(booking_count=Count("bookings"), active=ACTIVE)
        .order_by("name")
        .values_list(["name", "active"])
    )
    assert rows == [("Alice", 2), ("Bob", 1), ("Carol", 0)]
    rows = (
        await User.objects.annotate(active=ACTIVE)
        .order_by("name")
        .values({"name", "active"})
    )
    assert rows[0] == {"name": "Alice", "active": 2}
    rows = (
        await User.objects.annotate(active=ACTIVE)
        .order_by("name")
        .values({"name": ..., "active": ...})
    )
    assert rows[1] == {"name": "Bob", "active": 1}
    with pytest.raises(QueryDefinitionError):
        await User.objects.annotate(active=ACTIVE).values_list(
            ["name", "active"], flatten=True
        )


@pytest.mark.asyncio
async def test_annotation_independent_of_outer_relation_filter(users):
    rows = (
        await User.objects.annotate(booking_count=Count("bookings"))
        .filter(bookings__price__gt=15)
        .order_by("name")
        .values(["name", "booking_count"])
    )
    assert rows == [
        {"name": "Alice", "booking_count": 3},
        {"name": "Alice", "booking_count": 3},
    ]
    rows = (
        await User.objects.annotate(booking_count=Count("bookings"))
        .filter(name="Alice")
        .values(["name", "booking_count"])
    )
    assert rows == [{"name": "Alice", "booking_count": 3}]


@pytest.mark.asyncio
async def test_composite_primary_key_parent(shelves):
    queryset = Shelf.objects.annotate(
        book_count=Count("books"), pages=Sum("books__pages")
    ).order_by(["-book_count", "code"])
    assert await queryset.values_list(["code", "number", "book_count", "pages"]) == [
        ("a", 1, 2, 30),
        ("a", 2, 1, 5),
        ("b", 1, 0, None),
    ]
    found = await queryset.all()
    assert [ormar.annotations_of(shelf)["book_count"] for shelf in found] == [2, 1, 0]
    found = await queryset.select_related("books").limit(2).all()
    assert [(shelf.code, shelf.number) for shelf in found] == [("a", 1), ("a", 2)]
    assert ormar.annotations_of(found[0]) == {"book_count": 2, "pages": 30}


@pytest.mark.asyncio
async def test_first_last_and_reverse_slicing(users):
    queryset = User.objects.annotate(active=ACTIVE).order_by("-active")
    first = await queryset.first()
    assert first.name == "Alice"
    assert ormar.annotations_of(first) == {"active": 2}
    last = await queryset.last()
    assert last.name == "Carol"
    assert ormar.annotations_of(last) == {"active": 0}
    assert [user.name for user in await queryset[-2:].all()] == ["Bob", "Carol"]
    found = await queryset.select_related("bookings").last()
    assert found.name == "Carol"
    got = await queryset.get(name="Bob")
    assert ormar.annotations_of(got) == {"active": 1}
    got = await User.objects.annotate(active=ACTIVE).get()
    assert got.name == "Carol"


@pytest.mark.asyncio
async def test_annotations_of(users):
    alice = await User.objects.annotate(active=ACTIVE).get(name="Alice")
    annotations = ormar.annotations_of(alice)
    assert annotations == {"active": 2}
    with pytest.raises(TypeError):
        annotations["active"] = 5  # type: ignore[index]
    assert "active" not in alice.model_dump()
    alice.score = 4
    await alice.update()
    reloaded = await User.objects.get(name="Alice")
    assert reloaded.score == 4
    assert ormar.annotations_of(reloaded) == {}
    assert ormar.annotations_of(User(name="new")) == {}
    iterated = [
        dict(ormar.annotations_of(user))
        async for user in User.objects.annotate(active=ACTIVE)
        .order_by("name")
        .iterate()
    ]
    assert iterated == [{"active": 2}, {"active": 1}, {"active": 0}]


@pytest.mark.asyncio
async def test_case_func_and_arithmetic(users):
    level = Case(When(score__gte=10, then=2), When(score__gte=5, then=1), default=0)
    queryset = User.objects.annotate(level=level).order_by(["-level", "name"])
    assert await queryset.values_list(["name", "level"]) == [
        ("Bob", 2),
        ("Carol", 1),
        ("Alice", 0),
    ]
    found = (
        await User.objects.annotate(
            club_name=Func("upper", F("club__name")),
            total=F("score") + F("club__fee"),
        )
        .filter(club__isnull=False)
        .order_by("-total")
        .all()
    )
    assert by_name(found) == {
        "Carol": {"club_name": "CHESS", "total": 17},
        "Alice": {"club_name": "CHESS", "total": 13},
    }
    assert [user.name for user in found] == ["Carol", "Alice"]
    assert found[0].club.name == "chess"
    has_active = Case(When(ACTIVE > 0, then=True), default=False)
    found = (
        await User.objects.select_related("bookings")
        .annotate(has_active=has_active)
        .order_by(["has_active", "name"])
        .limit(2)
        .all()
    )
    assert [user.name for user in found] == ["Carol", "Alice"]


@pytest.mark.asyncio
async def test_invalid_annotations(users):
    for name in ("name", "bookings", "tags", "club", "pk", "user_score"):
        with pytest.raises(QueryDefinitionError, match="field or column"):
            User.objects.annotate(**{name: Count("bookings")})
    with pytest.raises(QueryDefinitionError, match="already annotated"):
        User.objects.annotate(active=ACTIVE).annotate(active=Count("tags"))
    with pytest.raises(QueryDefinitionError, match="takes an expression"):
        User.objects.annotate(active=1)
    with pytest.raises(QueryDefinitionError, match="only through foreign keys"):
        User.objects.annotate(price=F("bookings__price"))
    with pytest.raises(QueryDefinitionError, match="annotate\\(\\) has to come"):
        User.objects.order_by("-active").annotate(active=ACTIVE)
    with pytest.raises(QueryDefinitionError):
        await User.objects.prefetch_related(
            ormar.Prefetch("bookings", queryset=Booking.objects.annotate(n=F("id")))
        ).all()


@pytest.mark.asyncio
async def test_count_exists_and_aggregates_ignore_annotations(users):
    queryset = User.objects.annotate(active=ACTIVE).order_by("-active")
    assert await queryset.count() == 3
    assert await queryset.exists()
    assert await queryset.min("name") == "Alice"
    assert await queryset.filter(ACTIVE > 1).count() == 1
    assert await queryset.select_related("bookings").count(distinct=False) == 6


@pytest.mark.asyncio
async def test_queryset_proxy(users):
    chess = await Club.objects.get(name="chess")
    found = await chess.users.annotate(active=ACTIVE).order_by("-active").all()
    assert by_name(found) == {"Alice": {"active": 2}, "Carol": {"active": 0}}
    assert [user.name for user in found] == ["Alice", "Carol"]
    red = await Tag.objects.get(name="red")
    found = await red.users.annotate(active=ACTIVE).order_by("active").all()
    assert [user.name for user in found] == ["Bob", "Alice"]
    assert by_name(found) == {"Alice": {"active": 2}, "Bob": {"active": 1}}


@pytest.mark.asyncio
async def test_prefetch_related_keeps_main_annotations(users):
    found = (
        await User.objects.prefetch_related("bookings")
        .annotate(active=ACTIVE)
        .order_by(["-active", "name"])
        .all()
    )
    assert [len(user.bookings) for user in found] == [3, 2, 0]
    assert by_name(found)["Alice"] == {"active": 2}


@pytest.mark.asyncio
async def test_templates_are_shared_by_annotation_values(users):
    cache = template_cache_for(model_cls=User)
    cache.clear()
    for status, expected in (("active", [2, 1, 0]), ("cancelled", [1, 1, 0])):
        queryset = User.objects.annotate(
            matched=Count("bookings", filter={"status": status}),
            shifted=F("score") + 1,
        ).order_by("name")
        assert await queryset.values_list("matched", flatten=True) == expected
    assert len(cache.entries) == 1
    await (
        User.objects.annotate(
            matched=Count("bookings", filter={"price__gt": 1}), shifted=F("score") + 1
        )
        .order_by("name")
        .values_list("matched", flatten=True)
    )
    assert len(cache.entries) == 2
    await (
        User.objects.annotate(
            other=Count("bookings", filter={"status": "x"}), shifted=F("score") + 1
        )
        .order_by("name")
        .values_list("other", flatten=True)
    )
    assert len(cache.entries) == 3
    shifted = (
        await User.objects.annotate(shifted=F("score") + 5)
        .order_by("name")
        .values_list("shifted", flatten=True)
    )
    assert shifted == [8, 17, 12]
    assert len(cache.entries) == 4


def test_cache_keys_cover_annotations():
    def key_of(queryset: ormar.QuerySet) -> tuple:
        return queryset.select_cache_key(
            variant=(), limit=None, offset=None, order_bys=None
        )

    base = key_of(User.objects.annotate(active=ACTIVE))
    assert base == key_of(
        User.objects.annotate(active=Count("bookings", filter={"status": "x"}))
    )
    assert base != key_of(User.objects)
    assert base != key_of(User.objects.annotate(other=ACTIVE))
    assert base != key_of(User.objects.annotate(active=Count("bookings")))
    assert key_of(User.objects.annotate(a=ACTIVE, b=F("id"))) != key_of(
        User.objects.annotate(b=F("id"), a=ACTIVE)
    )
