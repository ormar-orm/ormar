# Update data in database

Following methods and functions allow updating existing data in the database.

* `update(each: bool = False, **kwargs) -> int`
* `update_returning(each: bool = False, **kwargs) -> list[Model]`
* `update_or_create(**kwargs) -> Model`
* `bulk_update(objects: list[Model], columns: list[str] = None) -> None`


* `Model`
    * `Model.update()` method
    * `Model.upsert()` method
    * `Model.save_related()` method


* `QuerysetProxy`
    * `QuerysetProxy.update_returning(each: bool = False, **kwargs)` method
    * `QuerysetProxy.update_or_create(**kwargs)` method

## update

`update(each: bool = False, **kwargs) -> int`

QuerySet level update is used to update multiple records with the same value at once.

You either have to filter the QuerySet first or provide a `each=True` flag to update
whole table.

If you do not provide this flag or a filter a `QueryDefinitionError` will be raised.

Return number of rows updated.

Values are validated and converted like in `create()`: a foreign key accepts the
related model, a dictionary with its pk or the pk itself (`None` clears it), and a
JSON field accepts a value to dump or a JSON string.

```Python hl_lines="46-48"
--8<-- "../docs_src/queries/docs002.py"
```

!!!warning 
    Queryset needs to be filtered before updating to prevent accidental overwrite.

    To update whole database table `each=True` needs to be provided as a safety switch

Filters may follow relations of any kind, the update changes the rows the same filters
select in `all()` (limit and offset do not apply to updates):

```python
await Booking.objects.filter(
    status="waitlisted", training__starts_at__lte=now
).update(status="expired")
await Track.objects.filter(album__genres__name="jazz").update(position=0)
```

Such filters select the matched primary keys in a subquery
(`WHERE id IN (SELECT ...)`).

### Column expressions

A value can be computed by the database from the row's own columns, with a field
accessor (`Model.field`) or `ormar.F("field")` combined with `+`, `-` and `*`. The
row is changed in one `UPDATE`, so concurrent updates are not lost:

```python
# UPDATE products SET stock = products.stock + 1 WHERE products.id = 1
await Product.objects.filter(id=1).update(stock=Product.stock + 1)

# take one item only while any is left; returns 0 when sold out
sold = await Product.objects.filter(id=1, stock__gt=0).update(stock=F("stock") - 1)

await Product.objects.update(each=True, price=F("price") * 2 + F("shipping"))
```

* The literal goes on the right side (`F("stock") + 1`), `None` and booleans are
  rejected. A `NULL` column makes the result `NULL`.
* An expression reads non-relation columns of the updated model, or of models
  reached through its foreign keys, and cannot set a relation (see below).
* It cannot read a column set by the same `update()` (MySQL would read the new value,
  other databases the old one), including fields with `on_update`. Reading the
  column it sets is fine.
* The database computes the value, so pydantic validators of the field do not run.

#### Reading related models

An expression can read fields of models reached through foreign keys, also
several relations deep:

```python
# UPDATE tracks SET position = albums.rank * 10 FROM albums AS ... WHERE ...
await Track.objects.filter(title="a").update(position=F("album__rank") * 10)
await Track.objects.update(each=True, position=F("album__label__bonus") + Track.album.rank)
```

* The related tables are joined in the `UPDATE` (`UPDATE ... FROM` on PostgreSQL
  and SQLite 3.33+, a multi-table `UPDATE` on MySQL and MariaDB).
* Rows with no related row on the path (a `NULL` foreign key) are not updated and
  not counted in the returned number.
* Reverse and many to many relations are rejected with `QueryDefinitionError`, as
  they can match many rows; filter on them instead.
* An expression reading the updated table through a relation (`F("parent__rank")`)
  reads the values from before the update.

#### Functions

`ormar.Greatest`, `ormar.Least` and `ormar.Coalesce` take two or more arguments,
at least one of them a field or an expression, and combine with other expressions:

```python
from ormar import Coalesce, F, Greatest, Least

# never move the read marker back
await Member.objects.filter(id=1).update(read_id=Greatest(F("read_id"), newest))
# never go below zero
await Link.objects.filter(id=1).update(uses=Greatest(F("uses") - n, 0))
await Pass.objects.filter(id=1).update(refunded=Coalesce(F("refunded"), 0) + amount)
await Product.objects.update(each=True, price=Least(F("price"), F("max_price") * 2))
```

* `Greatest` / `Least` compile to `GREATEST()` / `LEAST()`, on SQLite to the
  scalar `max()` / `min()`.
* With a `NULL` argument PostgreSQL ignores it, while MySQL and SQLite return
  `NULL`. Wrap nullable fields in `Coalesce` for the same result everywhere.
* Literal arguments are typed like in arithmetic, from the first field or expression
  argument and the literal (`Greatest(F("count"), 2.5)` compares with a float).

`ormar.Random()` is a random float from 0 (inclusive) to 1 (exclusive), drawn by
the database for each row:

```python
from ormar import F, Random

await Player.objects.update(each=True, score=Random())
await Player.objects.update(each=True, score=Random() * 10 + F("bonus"))
```

It compiles to `random()` on PostgreSQL, `rand()` on MySQL and on SQLite to its
integer `random()` scaled into the same range. Use it with float or decimal
columns: an integer column rounds the value on PostgreSQL and MySQL, while SQLite
stores the fraction.

## update_returning

`update_returning(each: bool = False, **kwargs) -> list[Model]`

Updates the rows like `update()` (same filters, `each` flag, value validation and
column expressions) and returns them as stored after the update, instead of their
number:

```python
expired = await Booking.objects.filter(
    status="waitlisted", training__starts_at__lte=now
).update_returning(status="expired")
for booking in expired:
    await notify(booking.member.pk, booking.training.pk)

# the value as the database computed it
[counter] = await Counter.objects.filter(name="tickets").update_returning(
    value=F("value") + 1
)
print(counter.value)
```

* The models hold the columns of the model's own table: relations are not loaded,
  a foreign key is a related model with only its primary key set, like in `all()`
  without `select_related()`. `fields()`, `select_related()` and `order_by()` of
  the queryset do not apply, the rows come back in no particular order.
* With no matching row nothing is updated and an empty list is returned.
* Rows not updated because an expression found no related row are not returned.
* No signals are sent, like in `update()`.
* `QuerysetProxy.update_returning()` (e.g. `album.tracks.update_returning(...)`)
  updates the related models in one statement, unlike its `update()`, which calls
  `update()` of each model; fields of a through model are not updated, and
  `limit()` / `offset()` do not apply (like in `QuerySet.update()`), while the
  proxy's `update()` respects them.

!!!note "Backends"
    PostgreSQL and SQLite (3.35+, the SQLite version Python is built with) run one
    `UPDATE ... RETURNING`.

    MySQL and MariaDB have no `UPDATE ... RETURNING`, so inside the open
    transaction, or a new one, three statements run:
    `SELECT <primary key> ... FOR UPDATE` with the same filters, `UPDATE ... WHERE
    <primary key> IN (...)` and a locking select (`FOR UPDATE`) of the updated rows
    by primary key. The rows stay locked until the transaction ends and the locking
    read sees their latest committed values (not the snapshot of the open
    transaction), so the result is the same as with `RETURNING`, also when other
    transactions changed the same rows.

## update_or_create

`update_or_create(**kwargs) -> Model`

Updates the model, or in case there is no match in database creates a new one.

```Python hl_lines="44-52"
--8<-- "../docs_src/queries/docs003.py"
```

!!!note 
    Note that if you want to create a new object you either have to pass pk column
    value or pk column has to be set as autoincrement

## bulk_update

`bulk_update(objects: list["Model"], columns: list[str] = None) -> None`

Allows to update multiple instance at once.

All `Models` passed need to have primary key column populated.

You can also select which fields to update by passing `columns` list as a list of string
names.

```python hl_lines="8"
# continuing the example from bulk_create
# update objects
for todo in todoes:
    todo.completed = False

# perform update of all objects at once
# objects need to have pk column set, otherwise exception is raised
await ToDo.objects.bulk_update(todoes)

completed = await ToDo.objects.filter(completed=False).all()
assert len(completed) == 3
```

## Model methods

Each model instance have a set of methods to `save`, `update` or `load` itself.

###update

You can update models by updating your model attributes (fields) and calling `update()` method.

If you try to update a model without a primary key set a `ModelPersistenceError` exception will be thrown.


!!!tip
    Read more about `update()` method in [models-update](../models/methods.md#update)

###upsert

It's a proxy to either `save()` or `update(**kwargs)` methods of a Model.
If the pk is set the `update()` method will be called.

!!!tip
    Read more about `upsert()` method in [models-upsert][models-upsert]

###save_related

Method goes through all relations of the `Model` on which the method is called, 
and calls `upsert()` method on each model that is **not** saved. 

!!!tip
    Read more about `save_related()` method in [models-save-related][models-save-related]

## QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey` returns the list of related models.

But at the same time it exposes subset of QuerySet API, so you can filter, create, select related etc related models directly from parent model.

### update_or_create

Works exactly the same as [update_or_create](#update_or_create) function above but allows you to update or create related objects
from other side of the relation.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

[querysetproxy]: ../relations/queryset-proxy.md
[models-upsert]: ../models/methods.md#upsert
[models-save-related]: ../models/methods.md#save_related
