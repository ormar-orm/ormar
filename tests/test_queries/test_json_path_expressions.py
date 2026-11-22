from typing import Any, Optional

import pytest
import pytest_asyncio
from sqlalchemy.dialects import mysql, postgresql, sqlite
from sqlalchemy.dialects.mysql.mariadb import MariaDBDialect

import ormar
from ormar import F
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.expressions import expression_key, json_path_clause
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Publisher(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="json_path_publishers")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20, unique=True)
    info: Optional[Any] = ormar.JSON(nullable=True)
    city: Optional[str] = ormar.String(max_length=20, nullable=True)
    staff: Optional[int] = ormar.Integer(nullable=True)


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="json_path_authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    city: Optional[str] = ormar.String(max_length=20, nullable=True)
    publisher: Optional[Publisher] = ormar.ForeignKey(
        Publisher, nullable=True, related_name="authors"
    )


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def publishers():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            alpha = await Publisher.objects.create(
                name="alpha",
                info={
                    "city": "Warsaw",
                    "staff": 12,
                    "rating": 4.5,
                    "active": True,
                    "address": {"street": "Main"},
                    "tags": ["sea", "war"],
                },
            )
            beta = await Publisher.objects.create(
                name="beta",
                info={
                    "city": "Krakow",
                    "staff": 3,
                    "rating": 3.25,
                    "active": False,
                    "address": {"street": "Long"},
                    "tags": ["sky"],
                },
            )
            await Publisher.objects.create(
                name="gamma", info={"city": "Gdansk", "staff": None}
            )
            await Publisher.objects.create(name="delta")
            await Author.objects.create(name="Ann", publisher=alpha)
            await Author.objects.create(name="Bob", publisher=alpha)
            await Author.objects.create(name="Cyd", publisher=beta)
            await Author.objects.create(name="Dee")
            yield


async def names(*groups: ormar.queryset.clause.FilterGroup, **kwargs: Any) -> list:
    queryset = Publisher.objects.filter(*groups, **kwargs).order_by("name")
    return await queryset.values_list("name", flatten=True)


@pytest.mark.asyncio
async def test_filters_with_each_cast(publishers):
    assert await names(F("info")["city"].as_string() == "Warsaw") == ["alpha"]
    assert await names(Publisher.info["staff"].as_integer() > 5) == ["alpha"]
    assert await names(F("info")["rating"].as_float() < 4.0) == ["beta"]
    assert await names(F("info")["active"].as_boolean() == True) == ["alpha"]  # noqa
    assert await names(F("info")["active"].as_boolean() == False) == ["beta"]  # noqa


@pytest.mark.asyncio
async def test_comparable_lookups(publishers):
    city = F("info")["city"].as_string()
    assert await names(city.in_(["Krakow", "Gdansk"])) == ["beta", "gamma"]
    assert await names(city.icontains("KRA")) == ["beta"]
    assert await names(city.startswith("Gd")) == ["gamma"]
    assert await names(ormar.or_(city == "Warsaw", F("name") == "delta")) == [
        "alpha",
        "delta",
    ]
    assert await Publisher.objects.exclude(city == "Warsaw").count() == 2


@pytest.mark.asyncio
async def test_nested_keys_and_array_indexes(publishers):
    street = F("info")["address"]["street"].as_string()
    assert await names(street == "Long") == ["beta"]
    assert await names(F("info")["tags"][0].as_string() == "sky") == ["beta"]
    assert await names(Publisher.info["tags"][1].as_string() == "war") == ["alpha"]


@pytest.mark.asyncio
async def test_missing_key_and_json_null_read_as_null(publishers):
    staff = F("info")["staff"].as_integer()
    assert await names(staff.isnull(True)) == ["delta", "gamma"]
    assert await names(staff == None) == ["delta", "gamma"]  # noqa
    active = F("info")["active"].as_boolean()
    assert await names(active.isnull(True)) == ["delta", "gamma"]
    assert await names(F("info")["city"].as_string() != "Warsaw") == [
        "beta",
        "gamma",
    ]
    assert await names(F("info")["tags"][5].as_string().isnull(False)) == []


@pytest.mark.asyncio
async def test_json_path_as_filter_value(publishers):
    await Publisher.objects.filter(name="beta").update(city="Krakow")
    assert await names(city=F("info")["city"].as_string()) == ["beta"]
    assert await names(
        F("info")["staff"].as_integer() > F("info")["rating"].as_float()
    ) == ["alpha"]


@pytest.mark.asyncio
async def test_related_path_is_joined(publishers):
    city = F("publisher__info")["city"].as_string()
    authors = await Author.objects.filter(city == "Warsaw").order_by("name").all()
    assert [author.name for author in authors] == ["Ann", "Bob"]
    staff = Author.publisher.info["staff"].as_integer()
    authors = await Author.objects.filter(staff < 5).all()
    assert [author.name for author in authors] == ["Cyd"]


@pytest.mark.asyncio
async def test_update_values(publishers):
    await Publisher.objects.update(
        each=True,
        city=F("info")["city"].as_string(),
        staff=Publisher.info["staff"].as_integer() + 1,
    )
    rows = await Publisher.objects.order_by("name").values_list(
        ["name", "city", "staff"]
    )
    assert rows == [
        ("alpha", "Warsaw", 13),
        ("beta", "Krakow", 4),
        ("delta", None, None),
        ("gamma", "Gdansk", None),
    ]


@pytest.mark.asyncio
async def test_update_values_from_related_path(publishers):
    await Author.objects.update(
        each=True, city=F("publisher__info")["address"]["street"].as_string()
    )
    rows = await Author.objects.order_by("name").values_list(["name", "city"])
    assert rows == [("Ann", "Main"), ("Bob", "Main"), ("Cyd", "Long"), ("Dee", None)]


@pytest.mark.asyncio
async def test_pagination_with_select_related(publishers):
    staff = F("info")["staff"].as_integer()
    page = (
        await Publisher.objects.select_related("authors")
        .filter(staff > 1)
        .order_by("name")
        .paginate(page=2, page_size=1)
        .all()
    )
    assert [publisher.name for publisher in page] == ["beta"]
    assert [author.name for author in page[0].authors] == ["Cyd"]
    city = F("publisher__info")["city"].as_string()
    authors = (
        await Author.objects.select_related("publisher")
        .filter(city.in_(["Warsaw", "Krakow"]))
        .order_by("-name")
        .paginate(page=1, page_size=2)
        .all()
    )
    assert [(author.name, author.publisher.name) for author in authors] == [
        ("Cyd", "beta"),
        ("Bob", "alpha"),
    ]


@pytest.mark.asyncio
async def test_template_cache_keys_path_shapes_not_values(publishers):
    cache = template_cache_for(model_cls=Publisher)
    cache.clear()
    for city in ("Warsaw", "Krakow"):
        assert len(await names(F("info")["city"].as_string() == city)) == 1
    assert await names(F("info")["staff"].as_string() == "12") == ["alpha"]
    assert len(cache.entries) == 1
    assert await names(F("info")["address"]["street"].as_string() == "Main") == [
        "alpha"
    ]
    assert await names(F("info")["tags"]["x"].as_string() == "sea") == []
    assert len(cache.entries) == 2
    assert await names(F("info")["tags"][0].as_string() == "sea") == ["alpha"]
    assert await names(F("info")["tags"][1].as_string() == "war") == ["alpha"]
    assert len(cache.entries) == 3
    assert await names(F("info")["staff"].as_integer() == 12) == ["alpha"]
    assert await names(F("info")[1].as_string().isnull(False)) == []
    assert len(cache.entries) == 5


def test_expression_key():
    path = F("info")["tags"][0]
    assert expression_key(node=path.as_string()) == (
        "json",
        "info",
        (str, int),
        "string",
    )
    assert expression_key(node=F("info")["other"][7].as_string()) == (
        expression_key(node=path.as_string())
    )
    assert expression_key(node=F("info")["tags"]["0"].as_string()) != (
        expression_key(node=path.as_string())
    )
    assert expression_key(node=path.as_float()) != (
        expression_key(node=path.as_string())
    )


@pytest.mark.asyncio
async def test_non_json_field_is_rejected(publishers):
    with pytest.raises(QueryDefinitionError, match="not an ormar.JSON field"):
        await names(F("name")["x"].as_string() == "a")
    with pytest.raises(QueryDefinitionError, match="not an ormar.JSON field"):
        await Author.objects.filter(Author.publisher.name["x"].as_string() == "a").all()
    with pytest.raises(QueryDefinitionError, match="non-relation field"):
        await names(F("missing")["x"].as_string() == "a")


@pytest.mark.asyncio
async def test_path_without_cast_is_rejected(publishers):
    with pytest.raises(QueryDefinitionError, match="needs a cast"):
        await names(F("info")["city"] == "Warsaw")
    with pytest.raises(QueryDefinitionError, match="needs a cast"):
        await Publisher.objects.update(each=True, city=F("info")["city"])


@pytest.mark.parametrize(
    "key",
    [
        -1,
        True,
        1.5,
        None,
        "",
        " city",
        'a"b',
        "a\\b",
        "{a}",
        "a,b",
        "NULL",
        "a\nb",
        "\x00",
    ],
)
def test_invalid_keys_are_rejected(key):
    with pytest.raises(QueryDefinitionError, match="JSON path keys"):
        F("info")[key]
    with pytest.raises(QueryDefinitionError, match="JSON path keys"):
        F("info")["address"][key]


def test_cast_ends_the_path():
    with pytest.raises(QueryDefinitionError, match="before casting"):
        F("info")["address"].as_string()["street"]
    with pytest.raises(QueryDefinitionError, match="one cast"):
        F("info")["staff"].as_integer().as_float()


def test_relation_accessor_has_no_json_path():
    with pytest.raises(QueryDefinitionError, match="non-relation fields"):
        Author.publisher["city"]


@pytest.mark.parametrize(
    "dialect, string_sql, boolean_sql",
    [
        (postgresql.dialect(), "->>", "AS BOOLEAN"),
        (mysql.dialect(), "JSON_UNQUOTE(JSON_EXTRACT(", "IS NOT NULL"),
        (MariaDBDialect(), "JSON_UNQUOTE(JSON_EXTRACT(", "IS NOT NULL"),
        (sqlite.dialect(), "CAST(JSON_EXTRACT(", "AS BOOLEAN"),
    ],
)
def test_json_path_sql_per_dialect(dialect, string_sql, boolean_sql):
    column = Publisher.ormar_config.table.c.info
    string = json_path_clause(
        node=F("info")["city"].as_string(), column=column, key="ormar_p0_0"
    )
    assert string_sql in str(string.compile(dialect=dialect))
    boolean = json_path_clause(
        node=F("info")["a"][0].as_boolean(), column=column, key=None
    )
    assert boolean_sql in str(boolean.compile(dialect=dialect))


def test_expressions_are_not_iterable():
    for expression in (Publisher.name, Publisher.info, F("info"), F("info")["a"]):
        with pytest.raises(TypeError):
            list(expression)
        with pytest.raises(TypeError):
            "x" in expression  # noqa: B015
    with pytest.raises(TypeError):
        Publisher.objects.fields(Publisher.name)
    with pytest.raises(TypeError):
        Publisher.objects.exclude_fields(Publisher.name)


@pytest.mark.asyncio
async def test_upsert_update_values(publishers):
    await Publisher.objects.create(
        name="alpha",
        on_conflict="update",
        conflict_target=["name"],
        update_values={"city": F("info")["address"]["street"].as_string()},
    )
    await Publisher.objects.bulk_create(
        [Publisher(name="beta"), Publisher(name="gamma"), Publisher(name="omega")],
        on_conflict="update",
        conflict_target=["name"],
        update_values={
            "city": Publisher.info["city"].as_string(),
            "staff": F("info")["staff"].as_integer() + 1,
        },
    )
    rows = await Publisher.objects.order_by("name").values_list(
        ["name", "city", "staff"]
    )
    assert rows == [
        ("alpha", "Main", None),
        ("beta", "Krakow", 4),
        ("delta", None, None),
        ("gamma", "Gdansk", None),
        ("omega", None, None),
    ]
