import datetime
import functools
from typing import Optional

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

import ormar
from ormar.queryset import templates
from ormar.queryset.queries.query import Query
from ormar.queryset.templates import (
    SlotBinder,
    TemplateCache,
    canonical_actions,
    rendered_statement,
    template_cache_for,
)
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, create_config

base_ormar_config = create_config()


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="template_authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="template_books")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    year: int = ormar.Integer(default=2000)
    author: Optional[Author] = ormar.ForeignKey(Author)


class Event(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="template_events")

    id: int = ormar.Integer(primary_key=True)
    data: dict = ormar.JSON(default={})
    day: datetime.date = ormar.Date(default=datetime.date(2020, 6, 1))
    at: datetime.datetime = ormar.DateTime(default=datetime.datetime(2020, 6, 1, 12, 0))
    moment: datetime.time = ormar.Time(default=datetime.time(10, 0))
    flag: bool = ormar.Boolean(default=True)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def connected():
    async with base_ormar_config.database:
        yield


def build_with_slots(queryset, **kwargs):
    binder = SlotBinder(
        canonical_actions(queryset.filter_clauses, queryset.exclude_clauses)
    )
    statement = queryset.build_select_expression(binder=binder, **kwargs)
    return str(statement.compile()), statement.compile().params


@pytest.mark.asyncio
async def test_binder_names_filter_values(connected):
    queryset = Book.objects.filter(title="a", id__gt=3).exclude(year=2)
    sql, params = build_with_slots(queryset)
    assert sql.index(":ormar_p0") < sql.index(":ormar_p1") < sql.index(":ormar_p2")
    assert (params["ormar_p0"], params["ormar_p1"], params["ormar_p2"]) == ("a", 3, 2)


@pytest.mark.asyncio
async def test_binder_names_limit_and_offset(connected):
    sql, params = build_with_slots(Book.objects.limit(5).offset(10))
    assert ":ormar_limit" in sql and ":ormar_offset" in sql
    assert (params["ormar_limit"], params["ormar_offset"]) == (5, 10)


@pytest.mark.asyncio
async def test_pagination_subquery_reuses_filter_slot(connected):
    queryset = Book.objects.select_related("author").filter(title="a").limit(2)
    sql, params = build_with_slots(queryset)
    assert sql.count(":ormar_p0") == 2
    assert params["ormar_p0"] == "a"
    assert params["ormar_limit"] == 2


@pytest.mark.asyncio
async def test_binder_handles_groups_in_and_model_values(connected):
    author = Author(id=7, name="x")
    queryset = Book.objects.filter(
        ormar.or_(title__in=["a", "b"], author=author), ~ormar.and_(year=1)
    )
    sql, params = build_with_slots(queryset)
    assert sorted(params) == ["ormar_p0", "ormar_p1", "ormar_p2"]
    assert sorted(map(str, params.values())) == sorted(["['a', 'b']", "7", "1"])


@pytest.mark.asyncio
async def test_no_binder_keeps_anonymous_parameters(connected):
    queryset = Book.objects.select_related("author").filter(title="a").limit(2)
    assert "ormar_" not in str(queryset.build_select_expression().compile())


def key(queryset, variant=(), limit=None, offset=None, order_bys=None):
    return queryset.template_key(variant, limit, offset, order_bys)


@pytest.mark.asyncio
async def test_same_shape_different_values_share_key(connected):
    assert key(Book.objects.filter(title="a", id__gt=1)) == key(
        Book.objects.filter(title="b", id__gt=9)
    )
    assert key(Book.objects.filter(id__in=[1, 2])) == key(
        Book.objects.filter(id__in=[1, 2, 3, 4])
    )
    assert key(Book.objects.limit(1).offset(5)) == key(Book.objects.limit(9).offset(2))


@pytest.mark.asyncio
async def test_model_value_shares_key_with_its_pk(connected):
    author = Author(id=3, name="x")
    assert key(Book.objects.filter(author=author)) == key(Book.objects.filter(author=3))
    unsaved = Author(name="y")
    assert key(Book.objects.filter(author=unsaved)) == key(
        Book.objects.filter(author=None)
    )


@pytest.mark.asyncio
async def test_sql_changing_differences_get_distinct_keys(connected):
    variants = [
        Book.objects.filter(title=None),
        Book.objects.filter(title="a"),
        Book.objects.filter(title__contains="50%"),
        Book.objects.filter(title__contains="ab"),
        Book.objects.filter(title__isnull=True),
        Book.objects.filter(title__isnull=False),
        Book.objects.filter(title__in=["a"]),
        Book.objects.exclude(title="a"),
        Book.objects.select_related("author").filter(title="a"),
        Book.objects.fields(["id", "title"]).filter(title="a"),
        Book.objects.exclude_fields(["year"]).filter(title="a"),
        Book.objects.order_by("-title").filter(title="a"),
        Book.objects.filter(title="a").limit(1),
        Book.objects.filter(title="a").offset(3),
        Book.objects.filter(ormar.or_(title="a", year=1)),
        Book.objects.filter(ormar.and_(title="a", year=1)),
        Book.objects.filter(~ormar.and_(title="a", year=1)),
    ]
    keys = [key(queryset) for queryset in variants]
    assert len(set(keys)) == len(keys)
    assert key(variants[1], variant=("count",)) != keys[1]


class BuildCounter:
    def __init__(self, original):
        self.original = original
        self.calls = 0

    def __get__(self, instance, owner):
        return functools.partial(self.call, instance)

    def call(self, query, *args, **kwargs):
        self.calls += 1
        return self.original(query, *args, **kwargs)


@pytest.mark.asyncio
async def test_template_hit_reuses_statement(connected):
    template_cache_for(Book).clear()
    first = Book.objects.filter(id=1).build_select_template()
    second = Book.objects.filter(id=2).build_select_template()
    assert first.statement is second.statement
    assert (first.params, second.params) == ({"ormar_p0": 1}, {"ormar_p0": 2})


@pytest.mark.asyncio
async def test_template_params_skip_values_without_binds(connected):
    template = (
        Book.objects.filter(title=None, year__isnull=True, id=3)
        .limit(4)
        .offset(0)
        .build_select_template()
    )
    assert template.params == {"ormar_p2": 3, "ormar_limit": 4}


def test_cache_is_bounded():
    cache = TemplateCache(max_size=2)
    for name in ("a", "b", "c"):
        cache.put(name, name)
    assert (cache.get("a"), cache.get("b"), cache.get("c")) == (None, "b", "c")


@pytest.mark.asyncio
async def test_hit_without_cross_check_skips_building(connected, monkeypatch):
    template_cache_for(Book).clear()
    monkeypatch.setattr(templates, "CROSS_CHECK", False)
    Book.objects.filter(title="warm").build_select_template()
    counter = BuildCounter(Query.build_select_expression)
    monkeypatch.setattr(Query, "build_select_expression", counter)
    Book.objects.filter(title="hit").build_select_template()
    assert counter.calls == 0
    Book.objects.filter(year=5).build_select_template()
    assert counter.calls == 1


@pytest.mark.asyncio
async def test_cross_check_detects_mismatch(connected, monkeypatch):
    template_cache_for(Book).clear()
    monkeypatch.setattr(templates, "CROSS_CHECK", True)
    monkeypatch.setattr(
        Book.objects.__class__, "template_key", lambda *args, **kwargs: ("same",)
    )
    Book.objects.filter(title="a").build_select_template()
    with pytest.raises(AssertionError, match="template"):
        Book.objects.filter(year=1).build_select_template()


@pytest.mark.asyncio
async def test_cross_check_passes_for_matching_hit(connected, monkeypatch):
    template_cache_for(Book).clear()
    monkeypatch.setattr(templates, "CROSS_CHECK", True)
    queryset = Book.objects.select_related("author").filter(title="a").limit(2)
    first = queryset.build_select_template()
    second = Book.objects.select_related("author").filter(title="b").limit(3)
    assert second.build_select_template().statement is first.statement


async def run_reads(title):
    await Book.objects.filter(title=title).all()
    await Book.objects.get_or_none(title=title)
    await Book.objects.filter(title=title).first_or_none()
    await Book.objects.filter(title=title).values(["id", "title"])
    await Book.objects.filter(title=title).count()
    await Book.objects.filter(title=title).count(distinct=False)
    await Book.objects.filter(title=title).exists()
    await Book.objects.filter(title=title).max("year")
    await Book.objects.filter(title=title).sum(["year", "id"])
    await Book.objects.get_or_none()
    return [book async for book in Book.objects.filter(title=title).iterate()]


@pytest.mark.asyncio
async def test_read_paths_use_templates(connected, monkeypatch):
    monkeypatch.setattr(templates, "CROSS_CHECK", False)
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Book.objects.create(title="warm", year=1)
        await run_reads("warm")
        counter = BuildCounter(Query.build_select_expression)
        monkeypatch.setattr(Query, "build_select_expression", counter)
        streamed = await run_reads("warm")
    assert counter.calls == 0
    assert [book.title for book in streamed] == ["warm"]


@pytest.mark.asyncio
async def test_boolean_filters_stay_literal(connected):
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Event.objects.create(data={"name": "abc"})
        for _ in range(2):
            assert await Event.objects.filter(flag=True).count() == 1
            assert await Event.objects.filter(flag=False).count() == 0


@pytest.mark.asyncio
@pytest.mark.skipif(
    DATABASE_URL.startswith("postgresql"),
    reason="PostgreSQL rejects json LIKE text and date/time vs text comparisons",
)
async def test_string_values_on_typed_columns_bind_like_sqlalchemy(  # noqa: E501
    connected,
):  # pragma: no cover
    async with base_ormar_config.database.transaction(force_rollback=True):
        await Event.objects.create(data={"name": "abc"})
        for _ in range(2):
            assert await Event.objects.filter(data__contains="abc").count() == 1
            assert await Event.objects.filter(day__gte="2020-01-01").count() == 1
            assert await Event.objects.filter(at__gte="2020-01-01").count() == 1
            assert await Event.objects.filter(moment__gte="09:00").count() == 1


def test_numbered_placeholders_compare_per_occurrence():
    shared = sa.bindparam("ormar_p0", type_=sa.Integer())
    table = Book.ormar_config.table
    stmt = table.select().where(table.c.id == shared).where(table.c.year == shared)
    sql, values, types = rendered_statement(stmt, {"ormar_p0": 4}, PGDialect_asyncpg())
    assert sql.count("?") == 2 and "$" not in sql
    assert values == [4, 4] and types == ["Integer", "Integer"]


@pytest.mark.asyncio
async def test_value_type_is_part_of_shape(connected):
    assert key(Event.objects.filter(day__gte="2020-01-01")) != key(
        Event.objects.filter(day__gte=datetime.date(2020, 1, 1))
    )


@pytest.mark.asyncio
async def test_limit_zero_does_not_share_paginated_template(connected):
    async with base_ormar_config.database.transaction(force_rollback=True):
        author = await Author.objects.create(name="a1")
        for title in ("x", "y", "z"):
            await Book.objects.create(title=title, author=author)
        empty = await Author.objects.select_related("books").limit(0).all()
        limited = await Author.objects.select_related("books").limit(1).all()
    assert empty == []
    assert len(limited[0].books) == 3


@pytest.mark.asyncio
async def test_cached_statement_holds_no_values(connected):
    template_cache_for(Book).clear()
    template = Book.objects.filter(id__in=list(range(50))).build_select_template()
    values = [
        bind.value for bind in template.statement._generate_cache_key().bindparams
    ]
    assert values and all(value is None for value in values)
