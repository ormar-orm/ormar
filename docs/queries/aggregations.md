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

## QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey`
returns the list of related models.

But at the same time it exposes a subset of QuerySet API, so you can filter, create,
select related etc related models directly from parent model.

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
