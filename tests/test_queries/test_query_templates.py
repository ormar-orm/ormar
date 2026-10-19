import functools
from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar.queryset import templates
from ormar.queryset.queries.query import Query
from ormar.queryset.templates import (
    SlotBinder,
    TemplateCache,
    canonical_actions,
    template_cache_for,
)
from tests.lifespan import init_tests
from tests.settings import create_config

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
