#Pagination and rows number

Following methods allow you to paginate and limit number of rows in queries. 

* `paginate(page: int) -> QuerySet`
* `limit(limit_count: int) -> QuerySet`
* `offset(offset: int) -> QuerySet`
* `get() -> Model`
* `first() -> Model`


* `QuerysetProxy`
    * `QuerysetProxy.paginate(page: int)` method
    * `QuerysetProxy.limit(limit_count: int)` method
    * `QuerysetProxy.offset(offset: int)` method

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
    
    Something like `Track.objects.select_related("album").filter(album__name="Malibu").offset(1).limit(1).all()`

!!!note
    You can slice the results; But note that negative indexing is not supported.

    Something like: `Track.objects[5:10].all()`

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
    
    Something like `Track.objects.select_related("album").filter(album__name="Malibu").offset(1).limit(1).all()`

!!!note
    You can slice the results; But note that negative indexing is not supported.

    Something like: `Track.objects[5:10].all()`


## get

`get(**kwargs) -> Model` 

Gets the first row from the db meeting the criteria set by kwargs.

If no criteria is set it will return the last row in db sorted by pk.
(The criteria cannot be set also with filter/exclude).

!!!tip
    To read more about `get` visit [read/get](./read/#get)


## first

`first() -> Model` 

Gets the first row from the db ordered by primary key column ascending.

!!!tip
    To read more about `first` visit [read/first](./read/#first)


## QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey`
returns the list of related models.

But at the same time it exposes subset of QuerySet API, so you can filter, create,
select related etc related models directly from parent model.

### paginate

Works exactly the same as [paginate](./#paginate) function above but allows you to paginate related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### limit

Works exactly the same as [limit](./#limit) function above but allows you to paginate related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### offset

Works exactly the same as [offset](./#offset) function above but allows you to paginate related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

[querysetproxy]: ../relations/queryset-proxy.md
