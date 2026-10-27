"""Benchmark for materializing joined rows in ``_process_query_result_rows``.

Rows of the same model reached through the same relation path are built once
per query result (``RowRegistry``) and reused by every row they appear in;
repeated rows only attach their new reverse / many-to-many children.

Shape:

    Project (1) -> Task (N, FK) -> Tag (M, m2m, shared across tasks)

A single ``Project`` produces ``N * M`` result rows on
``select_related(["tasks", "tasks__tags"]).all()``, so the main model repeats
in every row and each task repeats once per tag. Queried from the ``Task``
side the same shape repeats the parent ``Project`` in every row.
"""

import pytest

import ormar
from benchmarks.conftest import base_ormar_config

pytestmark = pytest.mark.asyncio


class BenchTag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bench_merge_tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class BenchProject(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bench_merge_projects")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class BenchTask(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bench_merge_tasks")

    id: int = ormar.Integer(primary_key=True)
    project: BenchProject = ormar.ForeignKey(
        BenchProject, index=True, related_name="tasks"
    )
    title: str = ormar.String(max_length=100)
    tags: list[BenchTag] = ormar.ManyToMany(BenchTag)


@pytest.mark.parametrize(
    ("num_tasks", "tags_per_task"),
    [(5, 5), (10, 10), (20, 10)],
)
async def test_select_related_nested_merge(
    aio_benchmark, num_tasks: int, tags_per_task: int
):
    """Project -> Tasks (FK) -> Tags (m2m) workload.

    Result row count is ``num_tasks * tags_per_task`` for one project; every
    row resolves to the same Project and adds one Task / Tag pair to it.
    """
    project = await BenchProject(name="P").save()
    tags = [await BenchTag(name=f"t{i}").save() for i in range(tags_per_task)]
    for i in range(num_tasks):
        task = await BenchTask(project=project, title=f"T{i}").save()
        for tag in tags:
            await task.tags.add(tag)

    @aio_benchmark
    async def query():
        return await BenchProject.objects.select_related(["tasks", "tasks__tags"]).all()

    result = query()
    assert len(result) == 1
    assert len(result[0].tasks) == num_tasks
    for task in result[0].tasks:
        assert len(task.tags) == tags_per_task


@pytest.mark.parametrize(
    ("num_tasks", "tags_per_task"),
    [(5, 5), (10, 10), (20, 10)],
)
async def test_select_related_shared_parent(
    aio_benchmark, num_tasks: int, tags_per_task: int
):
    """Task -> Project (FK) + Tags (m2m) workload.

    Result row count is ``num_tasks * tags_per_task``; every row carries the
    same Project, which is built once and shared by all returned tasks.
    """
    project = await BenchProject(name="P").save()
    tags = [await BenchTag(name=f"t{i}").save() for i in range(tags_per_task)]
    for i in range(num_tasks):
        task = await BenchTask(project=project, title=f"T{i}").save()
        for tag in tags:
            await task.tags.add(tag)

    @aio_benchmark
    async def query():
        return await BenchTask.objects.select_related(["project", "tags"]).all()

    result = query()
    assert len(result) == num_tasks
    assert all(task.project is result[0].project for task in result)
    for task in result:
        assert len(task.tags) == tags_per_task
