from typing import Optional

import pytest
import pytest_asyncio

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Writer(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggr_join_writers")

    id: int = ormar.Integer(primary_key=True)
    rating: int = ormar.Integer()


class Topic(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggr_join_topics")

    id: int = ormar.Integer(primary_key=True)
    priority: int = ormar.Integer()


class ArticleTopic(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggr_join_articles_x_topics")

    id: int = ormar.Integer(primary_key=True)
    sort_order: int = ormar.Integer(name="sort_col")


class Article(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="aggr_join_articles")

    id: int = ormar.Integer(primary_key=True)
    score: int = ormar.Integer()
    writer: Optional[Writer] = ormar.ForeignKey(Writer, related_name="articles")
    topics = ormar.ManyToMany(Topic, through=ArticleTopic, related_name="articles")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(autouse=True, scope="function")
async def cleanup():
    yield
    async with base_ormar_config.database:
        await ArticleTopic.objects.delete(each=True)
        await Article.objects.delete(each=True)
        await Topic.objects.delete(each=True)
        await Writer.objects.delete(each=True)


async def sample_data() -> Writer:
    """
    Two writers (ratings 5 and 8) with articles scored 3 and 9 (first writer)
    and 6 (second writer). The first article has topics of priority 1 and 4
    (sort orders 10 and 20), the second one the topic of priority 4 (sort
    order 30), the third one no topics.
    """
    first_writer = await Writer(rating=5).save()
    second_writer = await Writer(rating=8).save()
    first_topic = await Topic(priority=1).save()
    second_topic = await Topic(priority=4).save()
    first_article = await Article(score=3, writer=first_writer).save()
    second_article = await Article(score=9, writer=first_writer).save()
    await Article(score=6, writer=second_writer).save()
    await first_article.topics.add(first_topic, sort_order=10)
    await first_article.topics.add(second_topic, sort_order=20)
    await second_article.topics.add(second_topic, sort_order=30)
    return first_writer


async def aggregate(queryset, func_name, columns):
    """Run the aggregate function ``func_name`` of ``queryset`` on ``columns``."""
    return await getattr(queryset, func_name)(columns)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [("max", 8), ("min", 5), ("sum", 18), ("avg", 6)],
)
async def test_aggregate_joins_forward_relation(func_name, expected):
    async with base_ormar_config.database:
        await sample_data()
        result = await aggregate(Article.objects, func_name, "writer__rating")
        assert float(result) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected",
    [("max", 9), ("min", 3), ("sum", 18), ("avg", 6)],
)
async def test_aggregate_joins_reverse_relation(func_name, expected):
    async with base_ormar_config.database:
        await sample_data()
        result = await aggregate(Writer.objects, func_name, "articles__score")
        assert float(result) == expected
        explicit_queryset = Writer.objects.select_related("articles")
        explicit_result = await aggregate(
            explicit_queryset, func_name, "articles__score"
        )
        assert explicit_result == result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected_priority, expected_sort_order",
    [("max", 4, 30), ("min", 1, 10), ("sum", 9, 60), ("avg", 3, 20)],
)
async def test_aggregate_joins_many_to_many_relation(
    func_name, expected_priority, expected_sort_order
):
    async with base_ormar_config.database:
        await sample_data()
        priority = await aggregate(Article.objects, func_name, "topics__priority")
        assert float(priority) == expected_priority
        sort_order = await aggregate(
            Article.objects, func_name, "topics__articletopic__sort_order"
        )
        assert float(sort_order) == expected_sort_order


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected_priority, expected_sort_order",
    [("max", 4, 30), ("min", 1, 10), ("sum", 9, 60), ("avg", 3, 20)],
)
async def test_aggregate_joins_multi_hop_relation(
    func_name, expected_priority, expected_sort_order
):
    async with base_ormar_config.database:
        await sample_data()
        priority = await aggregate(
            Writer.objects, func_name, "articles__topics__priority"
        )
        assert float(priority) == expected_priority
        sort_order = await aggregate(
            Writer.objects, func_name, "articles__topics__articletopic__sort_order"
        )
        assert float(sort_order) == expected_sort_order


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected_score, expected_priority",
    [("max", 9, 4), ("min", 3, 1), ("sum", 18, 9), ("avg", 6, 3)],
)
async def test_aggregate_joins_relations_of_multiple_columns(
    func_name, expected_score, expected_priority
):
    async with base_ormar_config.database:
        await sample_data()
        result = await aggregate(
            Article.objects, func_name, ["writer__rating", "topics__priority"]
        )
        assert float(result["topics__priority"]) == expected_priority
        result = await aggregate(
            Writer.objects, func_name, ["articles__score", "articles__writer__rating"]
        )
        assert float(result["articles__score"]) == expected_score


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "func_name, expected_score, expected_priority",
    [("max", 9, 4), ("min", 3, 1), ("sum", 12, 9), ("avg", 6, 3)],
)
async def test_aggregate_joins_relation_of_queryset_proxy(
    func_name, expected_score, expected_priority
):
    async with base_ormar_config.database:
        writer = await sample_data()
        assert float(await aggregate(writer.articles, func_name, "score")) == (
            expected_score
        )
        priority = await aggregate(writer.articles, func_name, "topics__priority")
        assert float(priority) == expected_priority


@pytest.mark.asyncio
@pytest.mark.parametrize("func_name", ["max", "min", "sum", "avg"])
async def test_aggregate_reuses_joins_of_select_related_and_filters(func_name):
    async with base_ormar_config.database:
        await sample_data()
        queryset = Writer.objects.select_related("articles")
        assert await aggregate(queryset, func_name, "articles__score") == (
            await aggregate(Writer.objects, func_name, "articles__score")
        )

        filtered_queryset = Writer.objects.filter(articles__score__lt=9)
        result = await aggregate(filtered_queryset, func_name, "articles__score")
        assert float(result) == {"max": 6, "min": 3, "sum": 9, "avg": 4.5}[func_name]

        nested_queryset = Article.objects.select_related("topics").filter(
            topics__priority__gt=1
        )
        result = await aggregate(
            nested_queryset, func_name, "topics__articletopic__sort_order"
        )
        assert float(result) == {"max": 30, "min": 20, "sum": 50, "avg": 25}[func_name]


@pytest.mark.asyncio
async def test_aggregate_counts_rows_of_joined_reverse_relations():
    """
    Like with ``select_related()``, a reverse or many to many join returns one
    row per related model, so the own fields of the queried model repeat for
    each related row and ``sum()`` / ``avg()`` count them once per row.
    """
    async with base_ormar_config.database:
        await sample_data()
        assert await Writer.objects.sum("rating") == 13
        joined_queryset = Writer.objects.select_related("articles")
        assert await joined_queryset.sum("rating") == 18
        result = await Writer.objects.sum(["rating", "articles__score"])
        assert result == {"rating": 18, "articles__score": 18}
        assert await Writer.objects.filter(articles__score__gt=0).sum("rating") == 18
        nested_queryset = Writer.objects.select_related("articles__topics")
        assert await nested_queryset.sum("articles__score") == 21
        assert await Writer.objects.sum("articles__topics__priority") == 9
