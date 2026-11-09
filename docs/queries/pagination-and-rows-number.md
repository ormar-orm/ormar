#Pagination and rows number

Following methods allow you to paginate and limit number of rows in queries. 

* `paginate(page: int) -> QuerySet`
* `limit(limit_count: int) -> QuerySet`
* `offset(offset: int) -> QuerySet`
* `__getitem__(key: int | slice) -> QuerySet`
* `get() -> Model`
* `first() -> Model`
* `first_or_none() -> Optional[Model]`
* `last() -> Model`
* `last_or_none() -> Optional[Model]`


* `QuerysetProxy`
    * `QuerysetProxy.paginate(page: int)` method
    * `QuerysetProxy.limit(limit_count: int)` method
    * `QuerysetProxy.offset(offset: int)` method
    * `QuerysetProxy.__getitem__(key: int | slice)` method
    * `QuerysetProxy.first_or_none()` method
    * `QuerysetProxy.last()` method
    * `QuerysetProxy.last_or_none()` method

## paginate

`paginate(page: int, page_size: int = 20) -> QuerySet`

Combines the `offset` and `limit` methods based on page number and size

```python
class Track(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=DatabaseConnection(DATABASE_URL),
        metadata=sqlalchemy.MetaData(),
        tablename="track"
    )

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    name: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
```

```python
tracks = await Track.objects.paginate(3).all()
# will return 20 tracks starting at row 41 
# (with default page size of 20)
```

Note that `paginate(2)` is equivalent to `offset(20).limit(20)`

## limit

`limit(limit_count: int, limit_raw_sql: bool = None) -> QuerySet`

You can limit the results to desired number of parent models.

To limit the actual number of database query rows instead of number of main models
use the `limit_raw_sql` parameter flag, and set it to `True`.

```python
class Track(ormar.Model):
    ormar.OrmarConfig(
        database=DatabaseConnection(DATABASE_URL),
        metadata=sqlalchemy.MetaData(),
        tablename="track"
    )

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    name: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
```

```python
tracks = await Track.objects.limit(1).all()
# will return just one Track
```

!!!note
    All methods that do not return the rows explicitly returns a QuerySet instance so you can chain them together
    
    So operations like `filter()`, `select_related()`, `limit()` and `offset()` etc. can be chained.
    
    Something like `Track.object.select_related("album").filter(album__name="Malibu").offset(1).limit(1).all()`

## offset

`offset(offset: int, limit_raw_sql: bool = None) -> QuerySet`

You can also offset the results by desired number of main models.

To offset the actual number of database query rows instead of number of main models
use the `limit_raw_sql` parameter flag, and set it to `True`.

```python
class Track(ormar.Model):
    ormar.OrmarConfig(
        database=DatabaseConnection(DATABASE_URL),
        metadata=sqlalchemy.MetaData(),
        tablename="track"
    )

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    name: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
```

```python
tracks = await Track.objects.offset(1).limit(1).all()
# will return just one Track, but this time the second one
```

!!!note
    All methods that do not return the rows explicitly returns a QuerySet instance so you can chain them together
    
    So operations like `filter()`, `select_related()`, `limit()` and `offset()` etc. can be chained.
    
    Something like `Track.object.select_related("album").filter(album__name="Malibu").offset(1).limit(1).all()`


## slicing with `__getitem__`

A `QuerySet` can also be sliced with Python integer or slice syntax. Each
call returns a new `QuerySet` with LIMIT/OFFSET set accordingly — you still
need to `await` it with `.all()` to actually run the query.

```python
# positive bounds map directly to LIMIT/OFFSET
first_ten = await Track.objects[:10].all()
page_two = await Track.objects[10:20].all()
single = await Track.objects[5].all()  # returns a one-element list
```

Negative indices and negative slice bounds are supported too. Internally
they are translated into a reversed-order query plus an in-memory list
reversal, so the caller still sees results in the original ordering:

```python
last_five = await Track.objects[-5:].all()
last_one = await Track.objects[-1].all()      # one-element list
tail_slice = await Track.objects[-10:-5].all()
```

!!!note
    Slice shapes that would require a `COUNT(*)` round-trip to resolve — a
    bare `[:-N]`, or mixed positive/negative bounds like `[3:-2]` — raise
    `QueryDefinitionError`. If you need "all except the last N", fetch
    `.count()` explicitly and combine it with `.offset()`/`.limit()`.

    A `step` other than `1` is not supported, and a non-integer / non-slice
    key (e.g. `objects["foo"]`) also raises `QueryDefinitionError`.

    Each slice replaces any previous pagination state rather than composing
    with it — avoid chaining multiple slices on the same queryset.


## get

`get(**kwargs) -> Model` 

Gets the first row from the db meeting the criteria set by kwargs.

If no criteria is set it will return the last row in db sorted by pk.
(The criteria cannot be set also with filter/exclude).

!!!tip
    To read more about `get` visit [read/get](read.md#get)


## first

`first() -> Model`

Gets the first row from the db ordered by primary key column ascending.

!!!tip
    To read more about `first` visit [read/first](read.md#first)


## first_or_none

`first_or_none() -> Optional[Model]`

Same as `first()` but returns `None` instead of raising `NoMatch` when no
row matches.

!!!tip
    To read more about `first_or_none` visit [read/first_or_none](read.md#first_or_none)


## last

`last() -> Model`

Gets the last row from the db ordered by primary key column descending.
Complementary to `first()` — the default pk ordering is flipped and the top
row is returned.

```python
newest = await Track.objects.last()
newest_by_name = await Track.objects.order_by("name").last()
```

!!!tip
    To read more about `last` visit [read/last](read.md#last)


## last_or_none

`last_or_none() -> Optional[Model]`

Same as `last()` but returns `None` instead of raising `NoMatch` when no
row matches.

!!!tip
    To read more about `last_or_none` visit [read/last_or_none](read.md#last_or_none)


## QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey`
returns the list of related models.

But at the same time it exposes subset of QuerySet API, so you can filter, create,
select related etc related models directly from parent model.

### paginate

Works exactly the same as [paginate](#paginate) function above but allows you to paginate related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### limit

Works exactly the same as [limit](#limit) function above but allows you to paginate related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### offset

Works exactly the same as [offset](#offset) function above but allows you to paginate related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### slicing with `__getitem__`

Works exactly the same as [slicing](#slicing-with-__getitem__) above but on the relation
side:

```python
recent_cars = await user.cars[-3:].all()
```

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

[querysetproxy]: ../relations/queryset-proxy.md
