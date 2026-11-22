from typing import Any, ForwardRef, Optional

import pytest
import pytest_asyncio

import ormar
from ormar import Avg, Case, Count, F, Greatest, Max, Min, Sum, When
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.select_action import resolve_expression
from ormar.queryset.expressions import expression_key
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Instructor(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="agg_instructors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Training(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="agg_trainings")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=20)
    price: int = ormar.Integer(default=0)
    instructor: Optional[Instructor] = ormar.ForeignKey(Instructor, nullable=True)


class Badge(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="agg_badges")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Member(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="agg_members")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    booked: int = ormar.Integer(default=0)
    score: Optional[int] = ormar.Integer(nullable=True)
    badges: Optional[list[Badge]] = ormar.ManyToMany(Badge, related_name="members")


class Booking(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="agg_bookings")

    id: int = ormar.Integer(primary_key=True)
    member: Optional[Member] = ormar.ForeignKey(Member, related_name="bookings")
    training: Optional[Training] = ormar.ForeignKey(
        Training, nullable=True, related_name="bookings"
    )
    status: str = ormar.String(max_length=20)
    seats: int = ormar.Integer(default=1)


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="agg_categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    child_count: int = ormar.Integer(default=0)
    parent = ormar.ForeignKey(
        ForwardRef("Category"), nullable=True, related_name="children"
    )


Category.update_forward_refs()


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="agg_people")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    friends = ormar.ManyToMany(ForwardRef("Person"), related_name="friend_of")


Person.update_forward_refs()


class Library(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="agg_libraries")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Shelf(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="agg_shelves")

    code: str = ormar.String(max_length=10, primary_key=True)
    number: int = ormar.Integer(primary_key=True)
    library: Optional[Library] = ormar.ForeignKey(Library, related_name="shelves")


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="agg_books")

    id: int = ormar.Integer(primary_key=True)
    pages: int = ormar.Integer(default=0)
    shelf: Optional[Shelf] = ormar.ForeignKey(Shelf, related_name="books")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def members():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            ike = await Instructor.objects.create(name="Ike")
            yoga = await Training.objects.create(
                title="yoga", price=100, instructor=ike
            )
            judo = await Training.objects.create(title="judo", price=50)
            ann = await Member.objects.create(name="ann")
            bob = await Member.objects.create(name="bob")
            await Member.objects.create(name="cyd")
            gold = await Badge.objects.create(name="gold")
            silver = await Badge.objects.create(name="silver")
            await ann.badges.add(gold)
            await ann.badges.add(silver)
            await bob.badges.add(gold)
            for member, training, status, seats in (
                (ann, yoga, "active", 2),
                (ann, judo, "active", 1),
                (ann, judo, "cancelled", 3),
                (bob, yoga, "cancelled", 1),
                (bob, None, "active", 4),
            ):
                await Booking.objects.create(
                    member=member, training=training, status=status, seats=seats
                )
            yield


@pytest_asyncio.fixture()
async def categories():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            root = await Category.objects.create(name="root")
            first = await Category.objects.create(name="first", parent=root)
            await Category.objects.create(name="second", parent=root)
            await Category.objects.create(name="leaf", parent=first)
            yield


@pytest_asyncio.fixture()
async def people():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            ann = await Person.objects.create(name="ann")
            bob = await Person.objects.create(name="bob")
            cyd = await Person.objects.create(name="cyd")
            await ann.friends.add(bob)
            await ann.friends.add(cyd)
            await bob.friends.add(cyd)
            yield


@pytest_asyncio.fixture()
async def shelves():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            library = await Library.objects.create(name="city")
            for code, number, pages in (
                ("a", 1, [10, 20]),
                ("a", 2, [5]),
                ("b", 1, []),
            ):
                shelf = await Shelf.objects.create(
                    code=code, number=number, library=library
                )
                for count in pages:
                    await Book.objects.create(pages=count, shelf=shelf)
            yield


async def written(expression: Any) -> dict:
    await Member.objects.update(each=True, score=expression)
    rows = await Member.objects.order_by("name").values_list(["name", "score"])
    return dict(rows)


async def names(*groups: ormar.queryset.clause.FilterGroup, **kwargs: Any) -> list:
    queryset = Member.objects.filter(*groups, **kwargs).order_by("name")
    return await queryset.values_list("name", flatten=True)


@pytest.mark.asyncio
async def test_count_over_reverse_foreign_key(members):
    assert await written(Count("bookings")) == {"ann": 3, "bob": 2, "cyd": 0}
    assert await written(Count("bookings", filter={"status": "active"})) == {
        "ann": 2,
        "bob": 1,
        "cyd": 0,
    }
    assert await written(Count("bookings__training")) == {
        "ann": 3,
        "bob": 1,
        "cyd": 0,
    }
    assert await written(Count("bookings__training", distinct=True)) == {
        "ann": 2,
        "bob": 1,
        "cyd": 0,
    }
    assert await written(Count("bookings", distinct=True)) == {
        "ann": 3,
        "bob": 2,
        "cyd": 0,
    }


@pytest.mark.asyncio
async def test_sum_min_max_over_reverse_foreign_key(members):
    assert await written(Sum("bookings__seats")) == {"ann": 6, "bob": 5, "cyd": None}
    assert await written(Min("bookings__seats")) == {"ann": 1, "bob": 1, "cyd": None}
    assert await written(Max("bookings__seats")) == {"ann": 3, "bob": 4, "cyd": None}
    assert await written(Sum("bookings__seats", filter={"status": "active"})) == {
        "ann": 3,
        "bob": 4,
        "cyd": None,
    }


@pytest.mark.asyncio
async def test_avg_in_filters(members):
    assert await names(Avg("bookings__seats") > 2.2) == ["bob"]
    assert await names(Avg("bookings__seats") == 2) == ["ann"]
    assert await names(Avg("bookings__seats").isnull(True)) == ["cyd"]


@pytest.mark.asyncio
async def test_multi_step_paths(members):
    assert await written(Sum("bookings__training__price")) == {
        "ann": 200,
        "bob": 100,
        "cyd": None,
    }
    assert await written(
        Count("bookings", filter={"training__instructor__name": "Ike"})
    ) == {"ann": 1, "bob": 1, "cyd": 0}
    assert await written(
        Max(
            "bookings__training__price",
            filter=ormar.and_(training__instructor__isnull=True),
        )
    ) == {"ann": 50, "bob": None, "cyd": None}


@pytest.mark.asyncio
async def test_filter_groups_and_expressions(members):
    assert await written(
        Count("bookings", filter=ormar.or_(status="cancelled", seats__gte=4))
    ) == {"ann": 1, "bob": 2, "cyd": 0}
    assert await written(Count("bookings", filter=F("seats") > 1)) == {
        "ann": 2,
        "bob": 1,
        "cyd": 0,
    }
    assert await written(Count("bookings", filter=Booking.status == "active")) == {
        "ann": 2,
        "bob": 1,
        "cyd": 0,
    }
    assert await written(Count("bookings", filter={})) == {
        "ann": 3,
        "bob": 2,
        "cyd": 0,
    }


@pytest.mark.asyncio
async def test_many_to_many(members):
    assert await written(Count("badges")) == {"ann": 2, "bob": 1, "cyd": 0}
    assert await written(Count("badges", filter={"name": "silver"})) == {
        "ann": 1,
        "bob": 0,
        "cyd": 0,
    }
    gold_members = Badge.objects.filter(
        Count("members", filter={"name__in": ["ann", "cyd"]}) == 1
    )
    assert await gold_members.order_by("name").values_list("name", flatten=True) == [
        "gold",
        "silver",
    ]
    assert await Badge.objects.filter(Count("members") > 1).values_list(
        "name", flatten=True
    ) == ["gold"]


@pytest.mark.asyncio
async def test_filter_with_pagination_and_select_related(members):
    queryset = (
        Member.objects.select_related("bookings")
        .filter(Count("bookings", filter={"status": "active"}) > 0)
        .order_by("name")
    )
    paged = await queryset.limit(1).offset(1).all()
    assert [member.name for member in paged] == ["bob"]
    assert len(paged[0].bookings) == 2
    first = await queryset.limit(1).all()
    assert [member.name for member in first] == ["ann"]
    assert len(first[0].bookings) == 3
    assert await queryset.count() == 2
    assert await names(Count("bookings") >= 2, name__ne="bob") == ["ann"]
    assert await Member.objects.exclude(Count("bookings") > 0).values_list(
        "name", flatten=True
    ) == ["cyd"]


@pytest.mark.asyncio
async def test_aggregates_as_filter_values(members):
    await Member.objects.update(each=True, booked=2)
    assert await names(booked__lt=Count("bookings")) == ["ann"]
    assert await names(booked=Count("bookings")) == ["bob"]
    assert await names(booked=Count("badges") + 1) == ["bob"]


@pytest.mark.asyncio
async def test_update_with_aggregates(members):
    await Member.objects.update(
        each=True, booked=Count("bookings", filter={"status": "active"})
    )
    rows = await Member.objects.order_by("name").values_list(["name", "booked"])
    assert dict(rows) == {"ann": 2, "bob": 1, "cyd": 0}
    await Member.objects.filter(Count("badges") > 1).update(booked=F("booked") * 10)
    rows = await Member.objects.order_by("name").values_list(["name", "booked"])
    assert dict(rows) == {"ann": 20, "bob": 1, "cyd": 0}
    await Member.objects.filter(name="bob").update(
        score=Count("bookings") * 2 + Count("badges")
    )
    assert await Member.objects.filter(name="bob").values_list(
        "score", flatten=True
    ) == [5]


@pytest.mark.asyncio
async def test_aggregates_inside_case_and_functions(members):
    assert await written(
        Case(When(Count("bookings") > 2, then=10), default=Count("badges"))
    ) == {"ann": 10, "bob": 1, "cyd": 0}
    assert await written(
        Case(
            When(Count("bookings", filter={"status": "cancelled"}) == 1, then=7),
            default=Sum("bookings__seats", filter={"status": "active"}),
        )
    ) == {"ann": 7, "bob": 7, "cyd": None}
    assert await written(Greatest(Count("bookings"), Count("badges"), 1)) == {
        "ann": 3,
        "bob": 2,
        "cyd": 1,
    }
    assert await names(
        Case(When(name="bob", then=Count("bookings")), default=0) == 2
    ) == ["bob"]


@pytest.mark.asyncio
async def test_self_referencing_relation(categories):
    queryset = Category.objects.order_by("name")
    assert await queryset.filter(Count("children") > 0).values_list(
        "name", flatten=True
    ) == ["first", "root"]
    assert await queryset.filter(
        Count("children", filter=Count("children") > 0) > 0
    ).values_list("name", flatten=True) == ["root"]
    assert await queryset.filter(
        Count("children", filter={"name__startswith": "s"}) == 1
    ).values_list("name", flatten=True) == ["root"]
    await Category.objects.update(each=True, child_count=Count("children"))
    assert dict(await queryset.values_list(["name", "child_count"])) == {
        "first": 1,
        "leaf": 0,
        "root": 2,
        "second": 0,
    }
    await Category.objects.filter(Count("children") == 0, name__ne="second").delete()
    assert await queryset.values_list("name", flatten=True) == [
        "first",
        "root",
        "second",
    ]


@pytest.mark.asyncio
async def test_composite_keys(shelves):
    queryset = Shelf.objects.order_by(["code", "number"])
    assert await queryset.filter(Count("books") > 0).values_list(
        ["code", "number"]
    ) == [("a", 1), ("a", 2)]
    assert await queryset.filter(Sum("books__pages") > 10).values_list(
        ["code", "number"]
    ) == [("a", 1)]
    assert await queryset.filter(Count("books") == 0).values_list(
        ["code", "number"]
    ) == [("b", 1)]
    assert await Library.objects.filter(Count("shelves") == 3).count() == 1
    assert await Library.objects.filter(
        Count("shelves", filter={"code": "a"}) == 2
    ).exists()
    assert await queryset.filter(Count("books__shelf") == 1).values_list(
        ["code", "number"]
    ) == [("a", 2)]


@pytest.mark.asyncio
async def test_templates_are_shared_by_filter_values(members, monkeypatch):
    cache = template_cache_for(model_cls=Member)
    cache.clear()
    for status in ("active", "cancelled"):
        await names(Count("bookings", filter={"status": status}) > 1)
    assert len(cache.entries) == 1
    await names(Count("bookings", filter={"seats": 1}) > 1)
    assert len(cache.entries) == 2
    await names(Count("badges") > 1)
    assert len(cache.entries) == 3
    assert await names(Count("bookings", filter={"status": "active"}) > 1) == ["ann"]
    assert await names(Count("bookings", filter={"status": "cancelled"}) > 0) == [
        "ann",
        "bob",
    ]


def test_keys_cover_aggregates():
    def key_of(node: Any) -> object:
        return expression_key(node=resolve_expression(node=node, model=Member)[0])

    base = key_of(Count("bookings", filter={"status": "a"}))
    assert base == key_of(Count("bookings", filter={"status": "b"}))
    assert base != key_of(Count("bookings", filter={"seats": 1}))
    assert base != key_of(Count("bookings", filter={"status__ne": "a"}))
    assert base != key_of(Count("bookings"))
    assert base != key_of(Count("bookings", filter={"status": "a"}, distinct=True))
    assert base != key_of(Count("badges", filter={"name": "a"}))
    assert key_of(Sum("bookings__seats")) != key_of(Max("bookings__seats"))
    assert key_of(Sum("bookings__seats")) != key_of(Sum("bookings__id"))


@pytest.mark.asyncio
async def test_aggregates_are_not_changed_by_queries(members):
    shared = Count("bookings", filter={"status": "active"})
    assert await names(shared > 1) == ["ann"]
    assert await written(shared) == {"ann": 2, "bob": 1, "cyd": 0}
    assert shared.actions == []
    assert shared.source is None
    assert shared.condition is not None and shared.condition.actions == []


@pytest.mark.asyncio
async def test_invalid_aggregates(members):
    with pytest.raises(QueryDefinitionError, match="relation path string"):
        Count("")
    with pytest.raises(QueryDefinitionError, match="filter group"):
        Count("bookings", filter="status")  # type: ignore[arg-type]
    with pytest.raises(QueryDefinitionError, match=r"queryset\.count\(\)"):
        Member.objects.filter(Count("name") > 1)
    with pytest.raises(QueryDefinitionError, match=r"queryset\.sum\(\)"):
        Booking.objects.filter(Sum("member__booked") > 1)
    with pytest.raises(QueryDefinitionError, match="reverse foreign key"):
        Member.objects.filter(Count("missing") > 1)
    with pytest.raises(QueryDefinitionError, match="through model"):
        Member.objects.filter(Count("memberbadge") > 1)
    with pytest.raises(QueryDefinitionError, match="has no field 'missing'"):
        Member.objects.filter(Count("bookings__missing") > 1)
    with pytest.raises(QueryDefinitionError, match="'seats' is not a relation"):
        Member.objects.filter(Count("bookings__seats__id") > 1)
    with pytest.raises(QueryDefinitionError, match="only through foreign keys"):
        Member.objects.filter(Count("bookings__training__bookings") > 1)
    with pytest.raises(QueryDefinitionError, match="non-relation field"):
        Member.objects.filter(Sum("bookings__training") > 1)
    with pytest.raises(QueryDefinitionError, match="several columns"):
        Library.objects.filter(Count("shelves", distinct=True) > 1)
    with pytest.raises(QueryDefinitionError, match="filters read related models"):
        Member.objects.filter(
            Count("bookings", filter={"training__bookings__seats": 1}) > 1
        )
    with pytest.raises(QueryDefinitionError, match="upsert"):
        await Member.objects.create(
            id=1,
            name="ann",
            on_conflict="update",
            update_values={"booked": Count("bookings")},
        )


@pytest.mark.asyncio
async def test_writes_reading_the_written_table_through_the_relation(members):
    assert await written(Count("bookings", filter={"member__name": "ann"})) == {
        "ann": 3,
        "bob": 0,
        "cyd": 0,
    }
    await Member.objects.update(each=True, booked=1)
    assert await written(Sum("bookings__member__booked")) == {
        "ann": 3,
        "bob": 2,
        "cyd": None,
    }
    await Member.objects.filter(
        Count("bookings", filter={"member__name": "x"}) > 5
    ).delete()
    assert await Member.objects.count() == 3
    await Booking.objects.filter(member__name="bob").delete()
    await Member.objects.filter(
        Count("bookings", filter={"member__name": "bob"}) == 0, name="bob"
    ).delete()
    assert await names() == ["ann", "cyd"]


@pytest.mark.asyncio
async def test_self_referencing_select_related_and_filter(categories):
    loaded = (
        await Category.objects.select_related("children")
        .filter(Count("children") > 0, children__name="leaf")
        .all()
    )
    assert [category.name for category in loaded] == ["first"]
    assert [child.name for child in loaded[0].children] == ["leaf"]
    with_parent = (
        await Category.objects.select_related("parent")
        .filter(Count("children", filter={"parent__name": "first"}) > 0)
        .all()
    )
    assert [category.name for category in with_parent] == ["first"]
    assert with_parent[0].parent.name == "root"


@pytest.mark.asyncio
async def test_self_referencing_many_to_many(people):
    queryset = Person.objects.order_by("name")
    assert await queryset.filter(Count("friends") > 0).values_list(
        "name", flatten=True
    ) == ["ann", "bob"]
    assert await queryset.filter(Count("friend_of") > 0).values_list(
        "name", flatten=True
    ) == ["bob", "cyd"]
    assert await queryset.filter(
        Count("friends", filter={"name": "cyd"}) == 1
    ).values_list("name", flatten=True) == ["ann", "bob"]


@pytest.mark.asyncio
async def test_queryset_proxy(members):
    gold = await Badge.objects.get(name="gold")
    assert [
        member.name for member in await gold.members.filter(Count("bookings") > 2).all()
    ] == ["ann"]
    ann = await Member.objects.get(name="ann")
    assert [
        badge.name for badge in await ann.badges.filter(Count("members") > 1).all()
    ] == ["gold"]


@pytest.mark.asyncio
async def test_aggregates_on_both_sides(members):
    assert await names(Count("bookings") > Count("badges")) == ["ann", "bob"]
    assert await names(
        Count("bookings", filter={"status": "active"})
        == Count("badges", filter={"name": "gold"})
    ) == ["bob", "cyd"]
