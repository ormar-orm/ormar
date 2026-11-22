from typing import ForwardRef, Optional

import pytest
import pytest_asyncio

import ormar
from ormar.queryset import queryset as queryset_module
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Writer(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggd_writers")

    id: int = ormar.Integer(primary_key=True)
    rating: int = ormar.Integer()
    bonus: Optional[int] = ormar.Integer(nullable=True)


class Topic(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggd_topics")

    id: int = ormar.Integer(primary_key=True)
    priority: int = ormar.Integer()


class ArticleTopic(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggd_articles_x_topics")

    id: int = ormar.Integer(primary_key=True)
    sort_order: int = ormar.Integer(name="sort_col")


class Article(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggd_articles")

    id: int = ormar.Integer(primary_key=True)
    score: int = ormar.Integer()
    writer: Optional[Writer] = ormar.ForeignKey(Writer, related_name="articles")
    topics = ormar.ManyToMany(Topic, through=ArticleTopic, related_name="articles")


class Warehouse(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggd_warehouses")

    region: int = ormar.Integer(primary_key=True)
    code: int = ormar.Integer(primary_key=True, name="code_no")
    capacity: int = ormar.Integer()


class Crate(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggd_crates")

    aisle: int = ormar.Integer(primary_key=True)
    slot: int = ormar.Integer(primary_key=True)
    weight: int = ormar.Integer()
    warehouse: Optional[Warehouse] = ormar.ForeignKey(Warehouse, related_name="crates")


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggd_people")

    id: int = ormar.Integer(primary_key=True, name="person_id")
    age: int = ormar.Integer()
    parent = ormar.ForeignKey(ForwardRef("Person"), related_name="children")


Person.update_forward_refs()


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True, scope="function")
async def cleanup():
    yield
    async with base_ormar_config.database:
        for model in (ArticleTopic, Article, Topic, Writer, Crate, Warehouse):
            await model.objects.delete(each=True)
        await Person.objects.update(each=True, parent=None)
        await Person.objects.delete(each=True)


async def sample_data() -> dict:
    """
    Writers rated 5 (bonus NULL), 5 (bonus 4) and 8 (bonus 2). The first writer
    has articles scored 3 and 9, the second one an article scored 6, the third
    one none. The first article has topics of priority 1 and 4 (sort orders 10
    and 20), the second one the topic of priority 4 (sort order 30).
    """
    first_writer = await Writer(rating=5).save()
    second_writer = await Writer(rating=5, bonus=4).save()
    await Writer(rating=8, bonus=2).save()
    first_topic = await Topic(priority=1).save()
    second_topic = await Topic(priority=4).save()
    first_article = await Article(score=3, writer=first_writer).save()
    second_article = await Article(score=9, writer=first_writer).save()
    await Article(score=6, writer=second_writer).save()
    await first_article.topics.add(first_topic, sort_order=10)
    await first_article.topics.add(second_topic, sort_order=20)
    await second_article.topics.add(second_topic, sort_order=30)
    return {
        "writer": first_writer,
        "article": first_article,
        "topic": second_topic,
    }


async def aggregate(queryset, func_name, columns):
    """Run the aggregate function ``func_name`` of ``queryset`` on ``columns``."""
    result = await getattr(queryset, func_name)(columns)
    if isinstance(result, dict):
        return {
            column: None if value is None else float(value)
            for column, value in result.items()
        }
    return None if result is None else float(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected", [("sum", 18), ("avg", 6)], ids=["sum", "avg"]
)
async def test_own_field_counts_once_per_row_with_reverse_join(func_name, expected):
    async with base_ormar_config.database:
        await sample_data()
        querysets = [
            Writer.objects,
            Writer.objects.select_related("articles"),
            Writer.objects.select_related("articles__topics"),
        ]
        for queryset in querysets:
            assert await aggregate(queryset, func_name, "rating") == expected
        auto_joined = await aggregate(
            Writer.objects, func_name, ["rating", "articles__score"]
        )
        assert auto_joined["rating"] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected", [("sum", 10), ("avg", 5)], ids=["sum", "avg"]
)
async def test_own_field_counts_once_per_row_with_filter_join(func_name, expected):
    async with base_ormar_config.database:
        await sample_data()
        queryset = Writer.objects.filter(articles__score__gt=0)
        assert await aggregate(queryset, func_name, "rating") == expected
        queryset = Writer.objects.filter(articles__topics__priority__gt=0)
        assert await aggregate(queryset, func_name, "rating") == 5


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected", [("sum", 15), ("avg", 5)], ids=["sum", "avg"]
)
async def test_forward_path_counts_once_per_row_of_queried_model(func_name, expected):
    async with base_ormar_config.database:
        await sample_data()
        querysets = [Article.objects, Article.objects.select_related("topics")]
        for queryset in querysets:
            assert await aggregate(queryset, func_name, "writer__rating") == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [
        ("sum", {"score": 18, "priority": 9, "rating": 15}),
        ("avg", {"score": 6, "priority": 3, "rating": 5}),
    ],
    ids=["sum", "avg"],
)
async def test_deeper_to_many_chains(func_name, expected):
    async with base_ormar_config.database:
        await sample_data()
        queryset = Writer.objects.select_related("articles__topics")
        assert (
            await aggregate(queryset, func_name, "articles__score")
            == (expected["score"])
        )
        assert (
            await aggregate(Writer.objects, func_name, "articles__topics__priority")
            == (expected["priority"])
        )
        assert (
            await aggregate(Topic.objects, func_name, "articles__writer__rating")
            == (expected["rating"])
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [
        ("sum", {"sort": 60, "filtered": 50, "priority": 5, "score": 15}),
        ("avg", {"sort": 20, "filtered": 25, "priority": 2.5, "score": 5}),
    ],
    ids=["sum", "avg"],
)
async def test_many_to_many_and_through_fields(func_name, expected):
    async with base_ormar_config.database:
        await sample_data()
        querysets = [
            Article.objects,
            Article.objects.select_related(["writer", "topics"]),
            Article.objects.select_related(["writer__articles", "topics"]),
        ]
        for queryset in querysets:
            sort_order = await aggregate(
                queryset, func_name, "topics__articletopic__sort_order"
            )
            assert sort_order == expected["sort"]
            priority = await aggregate(queryset, func_name, "topics__priority")
            assert priority == {"sum": 9, "avg": 3}[func_name]
        writer_queryset = Writer.objects.select_related("articles")
        sort_order = await aggregate(
            writer_queryset, func_name, "articles__topics__articletopic__sort_order"
        )
        assert sort_order == expected["sort"]
        filtered_queryset = Article.objects.filter(topics__priority__gt=1)
        sort_order = await aggregate(
            filtered_queryset, func_name, "topics__articletopic__sort_order"
        )
        assert sort_order == expected["filtered"]
        result = await aggregate(
            Topic.objects, func_name, ["priority", "articles__score"]
        )
        assert result == {
            "priority": expected["priority"],
            "articles__score": expected["score"],
        }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [
        ("sum", {"capacity": 20, "crates__weight": 9, "warehouse": 30}),
        ("avg", {"capacity": 10, "crates__weight": 3, "warehouse": 10}),
    ],
    ids=["sum", "avg"],
)
async def test_composite_primary_keys(func_name, expected):
    async with base_ormar_config.database:
        first_warehouse = await Warehouse(region=1, code=7, capacity=10).save()
        second_warehouse = await Warehouse(region=2, code=7, capacity=10).save()
        await Crate(aisle=1, slot=1, weight=2, warehouse=first_warehouse).save()
        await Crate(aisle=1, slot=2, weight=2, warehouse=first_warehouse).save()
        await Crate(aisle=2, slot=1, weight=5, warehouse=second_warehouse).save()
        queryset = Warehouse.objects.select_related("crates")
        assert (
            await aggregate(queryset, func_name, "capacity") == (expected["capacity"])
        )
        result = await aggregate(
            Warehouse.objects, func_name, ["capacity", "crates__weight"]
        )
        assert result == {
            "capacity": expected["capacity"],
            "crates__weight": expected["crates__weight"],
        }
        assert (
            await aggregate(Crate.objects, func_name, "warehouse__capacity")
            == (expected["warehouse"])
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [
        ("sum", {"score": 18, "writer__rating": 15, "topics__priority": 9}),
        ("avg", {"score": 6, "writer__rating": 5, "topics__priority": 3}),
    ],
    ids=["sum", "avg"],
)
async def test_multiple_columns_mixing_own_forward_and_reverse(func_name, expected):
    async with base_ormar_config.database:
        await sample_data()
        result = await aggregate(
            Article.objects, func_name, ["score", "writer__rating", "topics__priority"]
        )
        assert result == expected
        result = await aggregate(
            Writer.objects,
            func_name,
            ["rating", "articles__score", "articles__topics__priority"],
        )
        assert result == {
            "rating": {"sum": 18, "avg": 6}[func_name],
            "articles__score": expected["score"],
            "articles__topics__priority": expected["topics__priority"],
        }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [
        ("sum", {"score": 12, "priority": 9, "rating": 10, "topics": 5}),
        ("avg", {"score": 6, "priority": 3, "rating": 5, "topics": 2.5}),
    ],
    ids=["sum", "avg"],
)
async def test_queryset_proxy(func_name, expected):
    async with base_ormar_config.database:
        instances = await sample_data()
        result = await aggregate(
            instances["writer"].articles, func_name, ["score", "topics__priority"]
        )
        assert result == {
            "score": expected["score"],
            "topics__priority": expected["priority"],
        }
        result = await aggregate(
            instances["topic"].articles, func_name, ["score", "writer__rating"]
        )
        assert result == {
            "score": expected["score"],
            "writer__rating": expected["rating"],
        }
        priority = await aggregate(instances["article"].topics, func_name, "priority")
        assert priority == expected["topics"]


@pytest.mark.asyncio
@pytest.mark.parametrize("func_name", ["sum", "avg", "max", "min"])
async def test_empty_results_are_none(func_name):
    async with base_ormar_config.database:
        await sample_data()
        queryset = Writer.objects.select_related("articles").filter(rating__gt=100)
        assert await aggregate(queryset, func_name, "rating") is None
        result = await aggregate(queryset, func_name, ["rating", "articles__score"])
        assert result == {"rating": None, "articles__score": None}


@pytest.mark.asyncio
async def test_avg_counts_rows_with_equal_values():
    """
    Writers rated 5, 5 and 8 average to 6: rows with equal values count each,
    only the rows repeated by a join are dropped.
    """
    async with base_ormar_config.database:
        await sample_data()
        queryset = Writer.objects.select_related("articles")
        assert await aggregate(queryset, "avg", "rating") == 6
        assert await aggregate(queryset, "sum", "rating") == 18


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected_bonus, expected_score",
    [("sum", 6, 18), ("avg", 3, 6)],
    ids=["sum", "avg"],
)
async def test_null_values_are_skipped(func_name, expected_bonus, expected_score):
    async with base_ormar_config.database:
        await sample_data()
        queryset = Writer.objects.select_related("articles")
        result = await aggregate(queryset, func_name, ["bonus", "articles__score"])
        assert result == {"bonus": expected_bonus, "articles__score": expected_score}
        nobody = Writer.objects.select_related("articles").filter(bonus__isnull=True)
        assert await aggregate(nobody, func_name, "bonus") is None


@pytest.fixture()
def recorded_statements(monkeypatch):
    """
    Records ``(function name, SQL)`` of every aggregate statement built, with
    the template caches of the models cleared so that each call builds one.
    """
    statements = []
    original_statement = queryset_module.aggregate_statement

    def record_statement(select, **statement_kwargs):
        statement = original_statement(select, **statement_kwargs)
        statements.append((statement_kwargs["func_name"], str(statement)))
        return statement

    monkeypatch.setattr(queryset_module, "aggregate_statement", record_statement)
    for model in (Writer, Article, Topic):
        template_cache_for(model_cls=model).clear()
    return statements


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [
        ("max", {"rating": 8, "articles__score": 9, "articles__topics__priority": 4}),
        ("min", {"rating": 5, "articles__score": 3, "articles__topics__priority": 1}),
    ],
)
async def test_max_and_min_read_joined_rows_directly(
    func_name, expected, recorded_statements
):
    async with base_ormar_config.database:
        await sample_data()
        columns = list(expected)
        queryset = Writer.objects.select_related("articles__topics")
        assert await aggregate(queryset, func_name, columns) == expected
        assert await aggregate(Writer.objects, func_name, columns) == expected
        await aggregate(queryset, "sum", columns)
    recorded_funcs = {recorded_func for recorded_func, _ in recorded_statements}
    assert recorded_funcs == {func_name, "sum"}
    for recorded_func, statement in recorded_statements:
        assert ("DISTINCT" in statement) == (recorded_func == "sum")


@pytest.mark.asyncio
@pytest.mark.parametrize("func_name", ["sum", "avg"])
async def test_joins_not_repeating_rows_skip_distinct(func_name, recorded_statements):
    """
    Forward joins and to-many joins on the aggregated column's own path do not
    repeat its rows, so the joined select is aggregated directly.
    """
    async with base_ormar_config.database:
        instances = await sample_data()
        await aggregate(
            Article.objects.select_related("writer"),
            func_name,
            ["score", "writer__rating"],
        )
        await aggregate(instances["writer"].articles, func_name, "score")
        await aggregate(Writer.objects, func_name, "articles__score")
        await aggregate(Writer.objects, func_name, "articles__topics__priority")
        assert recorded_statements
        assert all("DISTINCT" not in sql for _, sql in recorded_statements)
        recorded_statements.clear()
        result = await aggregate(
            Writer.objects, func_name, ["rating", "articles__score"]
        )
        assert result == {
            "rating": {"sum": 18, "avg": 6}[func_name],
            "articles__score": {"sum": 18, "avg": 6}[func_name],
        }
        assert all(sql.count("DISTINCT") == 1 for _, sql in recorded_statements)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [
        ("sum", {"first_two": 10, "last_two": 13, "score": 18}),
        ("avg", {"first_two": 5, "last_two": 6.5, "score": 6}),
    ],
    ids=["sum", "avg"],
)
async def test_limit_and_offset_count_each_limited_row_once(func_name, expected):
    async with base_ormar_config.database:
        await sample_data()
        queryset = Writer.objects.select_related("articles").order_by("id")
        first_two = queryset.limit(2)
        assert await aggregate(first_two, func_name, "rating") == expected["first_two"]
        result = await aggregate(first_two, func_name, ["rating", "articles__score"])
        assert result == {
            "rating": expected["first_two"],
            "articles__score": expected["score"],
        }
        last_two = queryset.offset(1).limit(2)
        assert await aggregate(last_two, func_name, "rating") == expected["last_two"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [
        (
            "sum",
            {"age": 110, "children": 100, "grandchildren": 70, "parent": 40},
        ),
        ("avg", {"age": 22, "children": 25, "grandchildren": 35, "parent": 10}),
    ],
    ids=["sum", "avg"],
)
async def test_self_referencing_relation(func_name, expected):
    async with base_ormar_config.database:
        root = await Person(age=10).save()
        first_child = await Person(age=10, parent=root).save()
        await Person(age=20, parent=root).save()
        await Person(age=30, parent=first_child).save()
        await Person(age=40, parent=first_child).save()
        queryset = Person.objects.select_related("children")
        assert await aggregate(queryset, func_name, "age") == expected["age"]
        result = await aggregate(Person.objects, func_name, ["age", "children__age"])
        assert result == {"age": expected["age"], "children__age": expected["children"]}
        nested_queryset = Person.objects.select_related("children__children")
        result = await aggregate(
            nested_queryset, func_name, ["children__age", "children__children__age"]
        )
        assert result == {
            "children__age": expected["children"],
            "children__children__age": expected["grandchildren"],
        }
        assert (
            await aggregate(Person.objects, func_name, "parent__age")
            == (expected["parent"])
        )
