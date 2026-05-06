"""Benchmark for nested-join row merging in ``_process_query_result_rows``.

The workload below targets the path optimized by the in-place index
assignment in ``_merge_items_lists``: a parent that fans out across many
joined rows so the matched branch fires repeatedly during
``_recursive_add``, with both halves of late-round merges accumulating
overlapping child PKs.

Shape:

    Project (1) -> Task (N, FK) -> Tag (M, m2m, shared across tasks)

A single ``Project`` produces ``N * M`` result rows on
``select_related(["tasks", "tasks__tags"]).all()``. The pairwise
``_recursive_add`` consolidates the duplicates, and in the deeper rounds
both sides hold overlapping ``Task`` PKs (because every adjacent row
carries the same task with a different tag); inside each task the tag
lists also accumulate and overlap across recursion halves.
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
    row materializes a duplicate Project with one Task (with one Tag).
    The merge path consolidates duplicates pairwise via ``_recursive_add``
    — covers ``_merge_items_lists`` end-to-end through the full join /
    materialize / merge pipeline.
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


@pytest.mark.parametrize("list_size", [10, 50, 100])
def test_merge_items_lists_pk_overlap(benchmark, list_size: int):
    """Microbenchmark for ``_merge_items_lists`` with full PK overlap.

    Constructs two equally sized lists of saved tasks where every entry
    in ``current_field`` matches an entry in ``other_value`` by PK. This
    is the worst case the per-pair list rebuild used to be O(N) on — K
    matches, each filtering an N-element ``value_to_set``. With
    ``other_idx`` driving in-place writes the cost drops from O(K·N) to
    O(K).

    The benchmark calls the merge classmethod directly so the SA query /
    row materialization overhead is excluded — the signal we want is the
    inner loop only. Every task is fully populated (no relations to
    recurse into) so ``merge_two_instances`` is light and the
    ``_merge_items_lists`` body itself dominates.
    """
    project = BenchProject(id=1, name="p")
    current_field = [
        BenchTask(id=i, project=project, title=f"T{i}") for i in range(list_size)
    ]
    other_value = [
        BenchTask(id=i, project=project, title=f"T{i}") for i in range(list_size)
    ]

    benchmark(
        BenchTask._merge_items_lists,
        field_name="tasks",
        current_field=current_field,
        other_value=other_value,
        relation_map={"tasks": ...},
    )
