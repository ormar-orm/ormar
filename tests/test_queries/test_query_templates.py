from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar.queryset.templates import SlotBinder, canonical_actions
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
