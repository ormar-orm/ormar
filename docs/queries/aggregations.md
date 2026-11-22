# Aggregation functions

Currently 6 aggregation functions are supported.


* `count(distinct: bool = True) -> int`
* `exists() -> bool`
* `sum(columns) -> Any`
* `avg(columns) -> Any`
* `min(columns) -> Any`
* `max(columns) -> Any`


* `QuerysetProxy`
    * `QuerysetProxy.count(distinct=True)` method
    * `QuerysetProxy.exists()` method
    * `QuerysetProxy.sum(columns)` method
    * `QuerysetProxy.avg(columns)` method
    * `QuerysetProxy.min(column)` method
    * `QuerysetProxy.max(columns)` method


## count

`count(distinct: bool = True) -> int`

Returns number of rows matching the given criteria (i.e. applied with `filter` and `exclude`).
If `distinct` is `True` (the default), this will return the number of primary rows selected. If `False`,
the count will be the total number of rows returned
(including extra rows for `one-to-many` or `many-to-many` left `select_related` table joins).
`False` is the legacy (buggy) behavior for workflows that depend on it.

```python
class Book(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=DatabaseConnection(DATABASE_URL),
        metadata=sqlalchemy.MetaData(),
        tablename="book"
    )

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    author: str = ormar.String(max_length=100)
    genre: str = ormar.String(
        max_length=100,
        default="Fiction",
        choices=["Fiction", "Adventure", "Historic", "Fantasy"],
    )
```

```python
# returns count of rows in db for Books model
no_of_books = await Book.objects.count()
```

## exists

`exists() -> bool`

Returns a bool value to confirm if there are rows matching the given criteria (applied with `filter` and `exclude`)

```python
class Book(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=DatabaseConnection(DATABASE_URL),
        metadata=sqlalchemy.MetaData(),
        tablename="book"
    )

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    author: str = ormar.String(max_length=100)
    genre: str = ormar.String(
        max_length=100,
        default="Fiction",
        choices=["Fiction", "Adventure", "Historic", "Fantasy"],
    )
```

```python
# returns a boolean value if given row exists
has_sample = await Book.objects.filter(title='Sample').exists()
```

## sum

`sum(columns) -> Any`

Returns sum value of columns for rows matching the given criteria (applied with `filter` and `exclude` if set before).

You can pass one or many column names including related columns.

As of now each column passed is aggregated separately (so `sum(col1+col2)` is not possible,
you can have `sum(col1, col2)` and later add 2 returned sums in python)

You cannot `sum` non numeric columns.

If you aggregate on one column, the single value is directly returned as a result
If you aggregate on multiple columns a dictionary with column: result pairs is returned

Given models like follows

```Python
--8<-- "../docs_src/aggregations/docs001.py"
```

A sample usage might look like following

```python
author = await Author(name="Author 1").save()
await Book(title="Book 1", year=1920, ranking=3, author=author).save()
await Book(title="Book 2", year=1930, ranking=1, author=author).save()
await Book(title="Book 3", year=1923, ranking=5, author=author).save()

assert await Book.objects.sum("year") == 5773
result = await Book.objects.sum(["year", "ranking"])
assert result == dict(year=5773, ranking=9)

try:
    # cannot sum string column
    await Book.objects.sum("title")
except ormar.QueryDefinitionError:
    pass

assert await Author.objects.select_related("books").sum("books__year") == 5773
result = await Author.objects.select_related("books").sum(
    ["books__year", "books__ranking"]
)
assert result == dict(books__year=5773, books__ranking=9)

assert (
    await Author.objects.select_related("books")
    .filter(books__year__lt=1925)
    .sum("books__year")
    == 3843
)
```

## avg

`avg(columns) -> Any`

Returns avg value of columns for rows matching the given criteria (applied with `filter` and `exclude` if set before).

You can pass one or many column names including related columns.

As of now each column passed is aggregated separately (so `sum(col1+col2)` is not possible,
you can have `sum(col1, col2)` and later add 2 returned sums in python)

You cannot `avg` non numeric columns.

If you aggregate on one column, the single value is directly returned as a result
If you aggregate on multiple columns a dictionary with column: result pairs is returned

```Python
--8<-- "../docs_src/aggregations/docs001.py"
```

A sample usage might look like following

```python
author = await Author(name="Author 1").save()
await Book(title="Book 1", year=1920, ranking=3, author=author).save()
await Book(title="Book 2", year=1930, ranking=1, author=author).save()
await Book(title="Book 3", year=1923, ranking=5, author=author).save()

assert round(float(await Book.objects.avg("year")), 2) == 1924.33
result = await Book.objects.avg(["year", "ranking"])
assert round(float(result.get("year")), 2) == 1924.33
assert result.get("ranking") == 3.0

try:
    # cannot avg string column
    await Book.objects.avg("title")
except ormar.QueryDefinitionError:
    pass

result = await Author.objects.select_related("books").avg("books__year")
assert round(float(result), 2) == 1924.33
result = await Author.objects.select_related("books").avg(
    ["books__year", "books__ranking"]
)
assert round(float(result.get("books__year")), 2) == 1924.33
assert result.get("books__ranking") == 3.0

assert (
    await Author.objects.select_related("books")
    .filter(books__year__lt=1925)
    .avg("books__year")
    == 1921.5
)
```

## min

`min(columns) -> Any`

Returns min value of columns for rows matching the given criteria (applied with `filter` and `exclude` if set before).

You can pass one or many column names including related columns.

As of now each column passed is aggregated separately (so `sum(col1+col2)` is not possible,
you can have `sum(col1, col2)` and later add 2 returned sums in python)

If you aggregate on one column, the single value is directly returned as a result
If you aggregate on multiple columns a dictionary with column: result pairs is returned

```Python
--8<-- "../docs_src/aggregations/docs001.py"
```

A sample usage might look like following

```python
author = await Author(name="Author 1").save()
await Book(title="Book 1", year=1920, ranking=3, author=author).save()
await Book(title="Book 2", year=1930, ranking=1, author=author).save()
await Book(title="Book 3", year=1923, ranking=5, author=author).save()

assert await Book.objects.min("year") == 1920
result = await Book.objects.min(["year", "ranking"])
assert result == dict(year=1920, ranking=1)

assert await Book.objects.min("title") == "Book 1"

assert await Author.objects.select_related("books").min("books__year") == 1920
result = await Author.objects.select_related("books").min(
    ["books__year", "books__ranking"]
)
assert result == dict(books__year=1920, books__ranking=1)

assert (
    await Author.objects.select_related("books")
    .filter(books__year__gt=1925)
    .min("books__year")
    == 1930
)
```

## max

`max(columns) -> Any`

Returns max value of columns for rows matching the given criteria (applied with `filter` and `exclude` if set before).

Returns min value of columns for rows matching the given criteria (applied with `filter` and `exclude` if set before).

You can pass one or many column names including related columns.

As of now each column passed is aggregated separately (so `sum(col1+col2)` is not possible,
you can have `sum(col1, col2)` and later add 2 returned sums in python)

If you aggregate on one column, the single value is directly returned as a result
If you aggregate on multiple columns a dictionary with column: result pairs is returned

```Python
--8<-- "../docs_src/aggregations/docs001.py"
```

A sample usage might look like following

```python
author = await Author(name="Author 1").save()
await Book(title="Book 1", year=1920, ranking=3, author=author).save()
await Book(title="Book 2", year=1930, ranking=1, author=author).save()
await Book(title="Book 3", year=1923, ranking=5, author=author).save()

assert await Book.objects.max("year") == 1930
result = await Book.objects.max(["year", "ranking"])
assert result == dict(year=1930, ranking=5)

assert await Book.objects.max("title") == "Book 3"

assert await Author.objects.select_related("books").max("books__year") == 1930
result = await Author.objects.select_related("books").max(
    ["books__year", "books__ranking"]
)
assert result == dict(books__year=1930, books__ranking=5)

assert (
    await Author.objects.select_related("books")
    .filter(books__year__lt=1925)
    .max("books__year")
    == 1923
)
```

## Aggregates of related rows

The functions above aggregate the rows of the queryset into one value.
`ormar.Count`, `ormar.Sum`, `ormar.Avg`, `ormar.Min` and `ormar.Max` compute one value
per row instead, over the rows related to it, e.g. members ordered by the number of
their active bookings. Each one is a correlated subquery and works wherever
[expressions](update.md#column-expressions) do: compared with `==`, `<`, `>`... in
`filter()` / `exclude()`, as a filter value, as an `update()` value and inside
`Case`, `Func` and arithmetic.

```python
from ormar import Count, Sum

# members with more than 3 active bookings
await Member.objects.filter(
    Count("bookings", filter={"status": "active"}) > 3
).all()

# store the number of bookings on each member
await Member.objects.update(each=True, booked=Count("bookings"))

# members whose booked trainings cost more than 500 in total
await Member.objects.filter(Sum("bookings__training__price") > 500).all()
```

* The path starts with a reverse foreign key or a many to many relation of the
  queried model and may continue through foreign keys to the aggregated field
  (`"bookings__training__price"`). To aggregate a field of the queried rows
  themselves use `queryset.sum()` and the other functions above.
* `filter=` restricts the aggregated rows. It takes a dict of keyword filters or a
  filter group (`ormar.and_()`, `ormar.or_()`, `Booking.status == "active"`), both
  written from the related model, and reads its fields and fields reached from it
  through foreign keys.
* `Count(path, filter=None, distinct=False)` counts the related rows, `0` when there
  are none. A path ending in a field counts its non-null values, a path ending in
  a relation counts the related models (`Count("bookings__training",
  distinct=True)` counts distinct trainings).
* `Sum`, `Avg`, `Min` and `Max` take a path ending in a field and give `NULL` when
  there are no related rows. `Min` and `Max` keep the type of the field, `Avg` is a
  decimal. The type of `Sum` depends on the database: the sum of an integer field
  is a decimal on MySQL and a numeric on PostgreSQL for `bigint` fields
  ([annotations](#annotations) return it as an `int`).
* Aggregates are not allowed in `update_values` of upserts.

## Annotations

`annotate(**expressions)` adds values computed by the database to each returned
row, by name, e.g. users ordered by the number of their active bookings. Any
expression works: aggregates of related rows, `Func`, `Case`, and arithmetic of
fields of the model and of models reached through foreign keys (joined like
`select_related()`).

```python
import ormar
from ormar import Count

users = (
    await User.objects.annotate(
        active=Count("bookings", filter={"status": "active"})
    )
    .order_by(["-active", "id"])
    .limit(20)
    .all()
)
ormar.annotations_of(users[0])["active"]  # -> 3

await User.objects.annotate(active=Count("bookings")).values(["name", "active"])
# -> [{"name": "Alice", "active": 3}, ...]
```

* `ormar.annotations_of(model)` returns a read-only mapping of the annotations of a
  model loaded by an annotated queryset (empty for other models). Annotations are
  not fields: `model_dump()` does not return them and `save()` / `update()` do not
  write them.
* `values()` and `values_list()` return the annotations after the fields: all of
  them without `fields`, else the ones named in `fields` (`values(["name",
  "active"])`, `values_list("active", flatten=True)`).
* `order_by("-active")` orders by an annotation; call it after the `annotate()`
  adding the name. With `select_related()` and `limit()` the annotation also
  picks the limited main models; `first()`, `last()` and negative slices work
  like with fields. Where `NULL` values go depends on the database: first in
  ascending order on SQLite and MySQL, last on PostgreSQL. `Count` is never
  `NULL`; wrap other expressions in `Coalesce` to choose.
* Annotation names are not filter keys: to filter by the value compare the
  expression itself, `filter(Count("bookings") > 3)`.
* An annotation name cannot be a field, relation or column name of the model, or
  another annotation.
* `count()`, `exists()` and `sum()` / `avg()` / `min()` / `max()` ignore the
  annotations and the orders by them.
* With `select_related()` of a reverse relation the rows of a model repeat, each
  with the same value; the model keeps one.
* Annotations belong to the queried model only: `prefetch_related()` models get
  none and a `Prefetch` queryset cannot be annotated.
* Types: `Count` is an `int`, `Avg` a `Decimal`, `Sum` of an integer field an
  `int`, `Min`, `Max` and `Sum` of other fields the type of the field. Other
  expressions are returned as the database driver returns them, e.g. a `Sum`
  inside arithmetic is a decimal on MySQL.

## QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey`
returns the list of related models.

But at the same time it exposes a subset of QuerySet API, so you can filter, create,
select related etc related models directly from parent model.

### annotate

Works exactly the same as [annotate](#annotations) above but annotates the related
models: `await club.users.annotate(active=Count("bookings")).all()`.

### count

Works exactly the same as [count](#count) function above but allows you to select columns from related
objects from other side of the relation.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### exists

Works exactly the same as [exists](#exists) function above but allows you to select columns from related
objects from other side of the relation.

### sum

Works exactly the same as [sum](#sum) function above but allows you to sum columns from related
objects from other side of the relation.

### avg

Works exactly the same as [avg](#avg) function above but allows you to average columns from related
objects from other side of the relation.

### min

Works exactly the same as [min](#min) function above but allows you to select minimum of columns from related
objects from other side of the relation.

### max

Works exactly the same as [max](#max) function above but allows you to select maximum of columns from related
objects from other side of the relation.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

[querysetproxy]: ../relations/queryset-proxy.md
