# Read data from database

Following methods allow you to load data from the database.

* `get(*args, **kwargs) -> Model`
* `get_or_none(*args, **kwargs) -> Optional[Model]`
* `get_or_create(_defaults: Optional[dict[str, Any]] = None, *args, **kwargs) -> tuple[Model, bool]`
* `first(*args, **kwargs) -> Model`
* `first_or_none(*args, **kwargs) -> Optional[Model]`
* `last(*args, **kwargs) -> Model`
* `last_or_none(*args, **kwargs) -> Optional[Model]`
* `all(*args, **kwargs) -> list[Optional[Model]]`
* `iterate(*args, **kwargs) -> AsyncGenerator[Model]`


* `Model`
    * `Model.load()` method


* `QuerysetProxy`
    * `QuerysetProxy.get(*args, **kwargs)` method
    * `QuerysetProxy.get_or_none(*args, **kwargs)` method
    * `QuerysetProxy.get_or_create(_defaults: Optional[dict[str, Any]] = None, *args, **kwargs)` method
    * `QuerysetProxy.first(*args, **kwargs)` method
    * `QuerysetProxy.first_or_none(*args, **kwargs)` method
    * `QuerysetProxy.last(*args, **kwargs)` method
    * `QuerysetProxy.last_or_none(*args, **kwargs)` method
    * `QuerysetProxy.all(*args, **kwargs)` method

## get

`get(*args, **kwargs) -> Model`

Gets a single row from the db matching the criteria set by args/kwargs.

When criteria are set (either through args/kwargs or via a chained `filter`/`exclude`)
the query fetches every matching row and asserts that exactly one row comes back.

If no criteria are set, `get()` falls back to returning the last row of the table
ordered by primary key descending.

Passing a criteria is equivalent to calling `filter(*args, **kwargs).get()`.

```python
class Track(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="track"
    )

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    name: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
```

```python
track = await Track.objects.get(name='The Bird')
# note that above is equivalent to await Track.objects.filter(name='The Bird').get()
track2 = track = await Track.objects.get()
track == track2
# True since it's the only row in db in our example
# and get without arguments returns the last row by pk column desc
```

!!!warning
    If no row meets the criteria `NoMatch` exception is raised.

    If criteria are set (via `filter`/`exclude` or args/kwargs) and more than
    one row matches them, the `MultipleMatches` exception is raised. The no-criteria
    fallback always returns a single row and cannot raise `MultipleMatches`.

## get_or_none

`get_or_none(*args, **kwargs) -> Model`

Exact equivalent of get described above but instead of raising the exception returns `None` if no db record matching the criteria is found.


## get_or_create

`get_or_create(_defaults: Optional[dict[str, Any]] = None, *args, **kwargs) -> tuple[Model, bool]`

Combination of create and get methods.

Tries to get a row meeting the criteria and if `NoMatch` exception is raised it creates
a new one with given kwargs and _defaults.

```python
class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="album")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    year: int = ormar.Integer()
```

```python
album, created = await Album.objects.get_or_create(name='The Cat', _defaults={"year": 1999})
assert created is True
assert album.name == "The Cat"
assert album.year == 1999
# object is created as it does not exist
album2, created = await Album.objects.get_or_create(name='The Cat')
assert created is False
assert album == album2
# return True as the same db row is returned
```

!!!warning 
    Despite being an equivalent row from database the `album` and `album2` in
    example above are 2 different python objects!
    Updating one of them will not refresh the second one until you explicitly load() the
    fresh data from db.

!!!note 
    Note that if you want to create a new object you either have to pass pk column
    value or pk column has to be set as autoincrement

!!!note
    Concurrent calls fall back to a second `get` if `create` trips a unique
    constraint, returning the winning row with `created=False`. Wrap in
    `database.transaction()` for stricter atomicity.

## first

`first(*args, **kwargs) -> Model`

Gets the first row from the db ordered by primary key column ascending.

Passing args and/or kwargs is a shortcut and equals to calling
`filter(*args, **kwargs).first()`.

The query is always executed with `LIMIT 1` so at most one row is fetched —
unlike `get()`, `first()` does not assert that the criteria match exactly one
row and never raises `MultipleMatches`. Use `get()` when you want that
assertion.

```python
class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="album")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
```

```python
await Album.objects.create(name='The Cat')
await Album.objects.create(name='The Dog')
album = await Album.objects.first()
# first row by primary_key column asc
assert album.name == 'The Cat'
```

!!!warning
    If no row meets the criteria `NoMatch` exception is raised.

## first_or_none

`first_or_none(*args, **kwargs) -> Optional[Model]`

Exact equivalent of `first` described above but instead of raising `NoMatch`
returns `None` if no db record matching the criteria is found.

```python
empty = await Album.objects.first_or_none()
# None — no rows in the table
missing = await Album.objects.first_or_none(name='The Missing')
# None — no row matches the filter
```

## last

`last(*args, **kwargs) -> Model`

Gets the last row from the db ordered by primary key column descending.
Complementary to `first()` — the default pk ordering is flipped and the top
row is returned. When you combine `last()` with `order_by(...)`, the user's
ordering is flipped too, so `order_by("name").last()` returns the row that
would sort last by name.

```python
await Album.objects.create(name='The Cat')
await Album.objects.create(name='The Dog')
album = await Album.objects.last()
# last row by primary_key column desc
assert album.name == 'The Dog'

album = await Album.objects.order_by("name").last()
# last row by name (alphabetical)
assert album.name == 'The Dog'
```

!!!warning
    Same as `first()` — raises `NoMatch` if no rows match. Like `first()`,
    `last()` always runs with `LIMIT 1` and never raises `MultipleMatches`;
    use `get()` if you want that assertion.

## last_or_none

`last_or_none(*args, **kwargs) -> Optional[Model]`

Exact equivalent of `last` described above but instead of raising `NoMatch`
returns `None` if no db record matching the criteria is found.

```python
empty = await Album.objects.last_or_none()
# None — no rows in the table
```

## all

`all(*args, **kwargs) -> list[Optional["Model"]]`

Returns all rows from a database for given model for set filter options.

Passing kwargs is a shortcut and equals to calling `filter(*args, **kwargs).all()`.

If there are no rows meeting the criteria an empty list is returned.

```python
class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="album")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="track")

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    title: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
```

```python
tracks = await Track.objects.select_related("album").all(album__title='Sample')
# will return a list of all Tracks for album Sample
# for more on joins visit joining and subqueries section

tracks = await Track.objects.all()
# will return a list of all Tracks in database

```

## iterate

`iterate(*args, **kwargs) -> AsyncGenerator["Model"]`

Return async iterable generator for all rows from a database for given model.

Passing args and/or kwargs is a shortcut and equals to calling `filter(*args, **kwargs).iterate()`.

If there are no rows meeting the criteria an empty async generator is returned.

```python
class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="album")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
```

```python
await Album.objects.create(name='The Cat')
await Album.objects.create(name='The Dog')
# will asynchronously iterate all Album models yielding one main model at a time from the generator
async for album in Album.objects.iterate():
    print(album.name)

# The Cat
# The Dog

```

!!!warning
    Use of `iterate()` causes previous `prefetch_related()` calls to be ignored;
    since these two optimizations do not make sense together.

    If `iterate()` & `prefetch_related()` are used together the `QueryDefinitionError` exception is raised.

## Model methods

Each model instance have a set of methods to `save`, `update` or `load` itself.

### load

You can load the `ForeignKey` related model by calling `load()` method.

`load()` can be used to refresh the model from the database (if it was changed by some other process).

!!!tip
    Read more about `load()` method in [models methods](../models/methods.md#load)

## QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey`
returns the list of related models.

But at the same time it exposes subset of QuerySet API, so you can filter, create,
select related etc related models directly from parent model.

### get

Works exactly the same as [get](./#get) function above but allows you to fetch related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### get_or_none

Exact equivalent of get described above but instead of raising the exception returns `None` if no db record matching the criteria is found.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section


### get_or_create

Works exactly the same as [get_or_create](./#get_or_create) function above but allows
you to query or create related objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### first

Works exactly the same as [first](./#first) function above but allows you to query
related objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### first_or_none

Works exactly the same as [first_or_none](./#first_or_none) function above but
returns `None` instead of raising `NoMatch`, and works on the relation side.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### last

Works exactly the same as [last](./#last) function above but allows you to query
related objects from other side of the relation.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### last_or_none

Works exactly the same as [last_or_none](./#last_or_none) function above but
returns `None` instead of raising `NoMatch`, and works on the relation side.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### all

Works exactly the same as [all](./#all) function above but allows you to query related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section


[querysetproxy]: ../relations/queryset-proxy.md
