from typing import Any, Optional

import pytest
import pytest_asyncio
from tests.lifespan import init_tests
from tests.settings import create_config

import ormar

pytestmark = pytest.mark.asyncio

graph_ormar_config = create_config()
GRAPH_SIZE = 6
LINKS_PER_PAIR = 2


def make_graph_model(index: int, targets: list[type[ormar.Model]]) -> type[ormar.Model]:
    """
    Create a model with ``LINKS_PER_PAIR`` foreign keys to each of the given
    models, so every model of the graph is related to every other one (directly
    or reversed) and the number of relation paths grows factorially.

    :param index: number of the model in the graph
    :type index: int
    :param targets: models to point foreign keys at
    :type targets: list[type[ormar.Model]]
    :return: new model class
    :rtype: type[ormar.Model]
    """
    attrs: dict[str, Any] = {
        "ormar_config": graph_ormar_config.copy(tablename=f"dense_graph_{index}"),
        "__annotations__": {"id": int, "name": str},
        "id": ormar.Integer(primary_key=True),
        "name": ormar.String(max_length=100),
    }
    for target in targets:
        for link in range(LINKS_PER_PAIR):
            field_name = f"link_{link}_{target.ormar_config.tablename}"
            attrs["__annotations__"][field_name] = Optional[target]
            attrs[field_name] = ormar.ForeignKey(
                target, related_name=f"links_{link}_from_{index}", nullable=True
            )
    return type(f"DenseGraph{index}", (ormar.Model,), attrs)


GRAPH: list[type[ormar.Model]] = []
for graph_index in range(GRAPH_SIZE):
    GRAPH.append(make_graph_model(index=graph_index, targets=list(GRAPH)))
Leaf = GRAPH[-1]

create_graph_database = init_tests(graph_ormar_config)


@pytest_asyncio.fixture(autouse=True)
async def connect_graph_database():
    async with graph_ormar_config.database:
        yield


def test_dense_graph_has_many_relation_paths():
    assert len(Leaf._iterate_related_models()) == 9240


@pytest.mark.parametrize("num_models", [1, 10, 50])
async def test_dumping_models_with_dense_relation_graph(aio_benchmark, num_models: int):
    leaves = [Leaf(id=i, name=f"leaf {i}") for i in range(num_models)]

    @aio_benchmark
    async def dump():
        return [leaf.model_dump() for leaf in leaves]

    assert len(dump()) == num_models


@pytest.mark.parametrize(
    "dump_kwargs",
    [
        {"include": {"id", "name"}},
        {"exclude": {"link_0_dense_graph_0", "link_1_dense_graph_0"}},
        {"exclude": {"link_0_dense_graph_0": {"links_0_from_1"}}},
        {"include": {"id": ..., "link_0_dense_graph_0": {"id", "name"}}},
    ],
    ids=["include", "exclude", "nested-exclude", "nested-include"],
)
async def test_dumping_with_include_exclude_on_dense_relation_graph(
    aio_benchmark, dump_kwargs: dict
):
    root = GRAPH[0](id=1, name="root")
    leaves = [
        Leaf(id=i, name=f"leaf {i}", link_0_dense_graph_0=root) for i in range(10)
    ]

    @aio_benchmark
    async def dump():
        return [leaf.model_dump(**dump_kwargs) for leaf in leaves]

    assert len(dump()) == 10


@pytest.mark.parametrize("num_models", [10, 50])
async def test_bulk_create_with_dense_relation_graph(aio_benchmark, num_models: int):
    root = await GRAPH[0].objects.create(name="root")

    @aio_benchmark
    async def insert():
        await Leaf.objects.bulk_create(
            [
                Leaf(name=f"leaf {i}", link_0_dense_graph_0=root)
                for i in range(num_models)
            ]
        )

    insert()
