# Aggregate annotations

`annotate()` adds a named, per-parent aggregate value (count/sum/avg/min/max of a
related model) to a query, computed with a single pre-grouped derived-table
`LEFT JOIN` rather than a correlated subquery per row. It composes with
`filter()`, `having()`, `order_by()`, `select_related()`, `limit()`/`offset()`,
and `values()`/`values_list()`.

* `annotate(**aggregates: AggregateFunction) -> QuerySet`
* `having(**filters: Any) -> QuerySet`

Aggregate expression objects (imported from the `ormar` top level):

* `ormar.Count(field: str, distinct: bool = False)`
* `ormar.Sum(field: str)`
* `ormar.Avg(field: str)`
* `ormar.Min(field: str)`
* `ormar.Max(field: str)`

!!!note
    This page documents phase 1 of aggregation support ("Mode A" - one aggregate
    value per parent row). True `GROUP BY` rollups over a non-unique column
    (`.group_by("country")`) are not implemented yet - see
    [Limitations](#limitations) below.

## annotate

`field` is either a relation name (`"tasks"`) for a bare `Count`, or a
`relation__column` path (`"tasks__price"`) for `Sum`/`Avg`/`Min`/`Max` (and
optionally `Count` too).

Given the following models:

```python
class User(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Task(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tasks")

    id: int = ormar.Integer(primary_key=True)
    user: Optional[User] = ormar.ForeignKey(User, related_name="tasks")
    title: str = ormar.String(max_length=100)
    price: Optional[int] = ormar.Integer(nullable=True)
```

```python
rows = (
    await User.objects.annotate(task_count=ormar.Count("tasks"))
    .order_by("name")
    .values(["name", "task_count"])
)
assert rows == [
    {"name": "Alice", "task_count": 2},
    {"name": "Bob", "task_count": 1},
    {"name": "Carol", "task_count": 0},  # no tasks
]
```

Several aggregates can be requested at once, including several columns of the
same relation:

```python
rows = (
    await User.objects.annotate(
        total=ormar.Sum("tasks__price"),
        avg_price=ormar.Avg("tasks__price"),
        cheapest=ormar.Min("tasks__price"),
        dearest=ormar.Max("tasks__price"),
    )
    .filter(name="Alice")
    .values(["name", "total", "cheapest", "dearest"])
)
assert rows == [{"name": "Alice", "total": 30, "cheapest": 10, "dearest": 20}]
```

`Count(field, distinct=True)` counts distinct related rows/values, matching the
existing `distinct` flag on `QuerySet.count()`:

```python
await User.objects.annotate(
    task_count=ormar.Count("tasks__id", distinct=True)
).values(["name", "task_count"])
```

Passing `distinct=True` to a bare `Count("relation")` (no column) is a no-op:
distinct only changes anything when it is applied to an explicit column, so use
`Count("relation__column", distinct=True)` as shown above if you need it.

### Zero vs. `NULL` for parents with no children

`Count` over a parent with no related rows returns `0` (the derived aggregate is
wrapped in `COALESCE(..., 0)`). `Sum`/`Avg`/`Min`/`Max` are **not** coalesced, so
they return `None` for a childless parent - there is no sensible zero for "the
average of no rows":

```python
carol = (
    await User.objects.annotate(total=ormar.Sum("tasks__price"))
    .filter(name="Carol")  # Carol has no tasks
    .values(["name", "total"])
)
assert carol == [{"name": "Carol", "total": None}]
```

## Ordering by an annotation

`order_by()` accepts an annotated name exactly like a regular column, ascending
or descending (this closes
[issue #566](https://github.com/ormar-orm/ormar/issues/566), sorting parents by
the number of related rows):

```python
names = (
    await User.objects.annotate(task_count=ormar.Count("tasks"))
    .order_by("-task_count")
    .values_list("name", flatten=True)
)
assert names == ["Alice", "Bob", "Carol"]
```

## having

`having()` filters rows by an annotated aggregate value - the annotation
equivalent of `filter()`. Supported suffixes are `exact` (default), `gt`,
`gte`, `lt`, `lte` and `ne`:

```python
rows = (
    await User.objects.annotate(task_count=ormar.Count("tasks"))
    .having(task_count__gt=1)
    .values_list("name", flatten=True)
)
assert rows == ["Alice"]
```

Any other suffix (e.g. `contains`) raises `QueryDefinitionError`.

## values() and values_list() output

Annotated names flow through `values()`/`values_list()` as extra keys/columns,
whether or not you pass an explicit field subset. If you don't pass `fields`,
every regular column is returned alongside every annotation:

```python
rows = (
    await User.objects.annotate(task_count=ormar.Count("tasks"))
    .order_by("name")
    .values()
)
# every row carries the normal columns (id, name, ...) plus task_count
assert all("task_count" in row for row in rows)
```

Passing an explicit field list works the same way as for regular columns -
list the annotation name alongside the columns you want:

```python
await User.objects.annotate(task_count=ormar.Count("tasks")).values(
    ["name", "task_count"]
)
```

!!!note
    Annotated values are only available through `values()`/`values_list()` in
    this phase. They are not (yet) attached as attributes on hydrated model
    instances returned by `all()`/`get()`.

## Many-to-many relations

A bare `Count` over a many-to-many relation is supported - it groups over the
through table, so the far side of the relation is never joined:

```python
rows = (
    await Post.objects.annotate(tag_count=ormar.Count("tags"))
    .order_by("title")
    .values(["title", "tag_count"])
)
```

A **qualified** aggregate over a many-to-many relation (`Sum("tags__price")`,
or even `Count("tags__id")`) is not supported and raises `QueryDefinitionError`.
Only `Count("relation")` without a column works over many-to-many.

## Composing with select_related and pagination

Because the aggregate is joined 1:1 onto the parent primary key, it does not
change the number of parent rows, so it composes freely with `select_related()`
and with `limit()`/`offset()`:

```python
users = (
    await User.objects.select_related("tasks")
    .annotate(task_count=ormar.Count("tasks"))
    .order_by("-task_count")
    .limit(2)
    .all()
)
```

## Limitations

- **No `group_by()` / rollups yet.** This phase only supports one aggregate
  value per parent row (joined by primary key). Grouping by an arbitrary,
  non-unique column (e.g. "count of tasks per country") is the more general
  request tracked in
  [issue #266](https://github.com/ormar-orm/ormar/issues/266) and is planned as
  a future addition, not covered by `annotate()`/`having()` today.
- **Many-to-many is Count-only.** A qualified aggregate over a many-to-many
  relation (`Sum("tags__price")`, `Count("tags__id")`) raises
  `QueryDefinitionError`; only bare `Count("relation")` is supported over
  many-to-many, as shown above.
- **An outer to-many relation filter can duplicate parent rows.** This is
  pre-existing `ormar` behaviour unrelated to annotations (see the `distinct`
  flag on `QuerySet.count()`): filtering on a to-many relation's column
  (`.filter(tasks__price__gt=15)`) joins that relation again to evaluate the
  filter, and plain SQL join semantics duplicate the parent row once per
  matching child in the flat `values()`/`values_list()` result. The annotation
  itself is unaffected by that outer filter - it is computed by its own
  derived-table subquery grouped over the raw child table, so it reports the
  same, correct value (based on **all** matching children, not just the ones
  matched by the outer filter) on every duplicated row:

```python
rows = (
    await User.objects.annotate(task_count=ormar.Count("tasks"))
    .filter(tasks__price__gt=15)  # matches 2 of Alice's 3 tasks
    .order_by("name")
    .values(["name", "task_count"])
)
# Alice's row is duplicated by the outer filter's join (pre-existing
# behaviour), but task_count is 3 (all of Alice's tasks) on both rows.
assert rows == [
    {"name": "Alice", "task_count": 3},
    {"name": "Alice", "task_count": 3},
]
```
