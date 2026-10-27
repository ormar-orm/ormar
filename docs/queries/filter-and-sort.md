# Filtering and sorting data

You can use following methods to filter the data (sql where clause).

* `filter(*args, **kwargs) -> QuerySet`
* `exclude(*args, **kwargs) -> QuerySet`
* `get(*args, **kwargs) -> Model`
* `get_or_none(*args, **kwargs) -> Optional[Model]`
* `get_or_create(_defaults: Optional[dict[str, Any]] = None, *args, **kwargs) -> tuple[Model, bool]`
* `all(*args, **kwargs) -> list[Optional[Model]]`


* `QuerysetProxy`
    * `QuerysetProxy.filter(*args, **kwargs)` method
    * `QuerysetProxy.exclude(*args, **kwargs)` method
    * `QuerysetProxy.get(*args, **kwargs)` method
    * `QuerysetProxy.get_or_none(*args, **kwargs)` method
    * `QuerysetProxy.get_or_create(_defaults: Optional[dict[str, Any]] = None, *args, **kwargs)` method
    * `QuerysetProxy.all(*args, **kwargs)` method

And following methods to sort the data (sql order by clause).

* `order_by(columns:Union[list, str, OrderAction]) -> QuerySet`
* `QuerysetProxy`
    * `QuerysetProxy.order_by(columns:Union[list, str, OrderAction])` method

## Filtering

### filter

`filter(*args, **kwargs) -> QuerySet`

Allows you to filter by any `Model` attribute/field as well as to fetch instances, with
a filter across an FK relationship.

```python
class Album(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    is_best_seller: bool = ormar.Boolean(default=False)

class Track(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    name: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
    play_count: int = ormar.Integer(nullable=True)
```

```python
track = Track.objects.filter(name="The Bird").get()
# will return a track with name equal to 'The Bird'

tracks = Track.objects.filter(album__name="Fantasies").all()
# will return all tracks where the columns album name = 'Fantasies'
```

### Django style filters

You can use special filter suffix to change the filter operands:

*  **exact** - exact match to value, sql `column = <VALUE>` 
       * can be written as`album__name__exact='Malibu'`
*  **iexact** - exact match sql `column = <VALUE>` (case insensitive)
      * can be written as`album__name__iexact='malibu'`
*  **contains** - sql `column LIKE '%<VALUE>%'`
       * can be written as`album__name__contains='Mal'`
*  **icontains** - sql `column LIKE '%<VALUE>%'` (case insensitive)
       * can be written as`album__name__icontains='mal'`
*  **in** - sql ` column IN (<VALUE1>, <VALUE2>, ...)`
       * can be written as`album__name__in=['Malibu', 'Barclay']`
*  **isnull** - sql `column IS NULL` (and sql `column IS NOT NULL`) 
      * can be written as`album__name__isnull=True` (isnotnull `album__name__isnull=False`)
*  **gt** - sql `column > <VALUE>` (greater than)
       * can be written as`position__gt=3`
*  **gte** - sql `column >= <VALUE>` (greater or equal than)
       * can be written as`position__gte=3`
*  **lt** - sql `column < <VALUE>` (lower than)
       * can be written as`position__lt=3`
*  **lte** - sql `column <= <VALUE>` (lower equal than)
       * can be written as`position__lte=3` 
*  **startswith** - sql `column LIKE '<VALUE>%'` (exact start match)
       * can be written as`album__name__startswith='Mal'`
*  **istartswith** - sql `column LIKE '<VALUE>%'` (case insensitive)
       * can be written as`album__name__istartswith='mal'`
*  **endswith** - sql `column LIKE '%<VALUE>'` (exact end match)
       * can be written as`album__name__endswith='ibu'`
*  **iendswith** - sql `column LIKE '%<VALUE>'` (case insensitive)
       * can be written as`album__name__iendswith='IBU'` 
    
Some samples:

```python
# sql: ( product.name = 'Test'  AND  product.rating >= 3.0 ) 
Product.objects.filter(name='Test', rating__gte=3.0).get()

# sql: ( product.name = 'Test' AND product.rating >= 3.0 ) 
#       OR (categories.name IN ('Toys', 'Books'))
Product.objects.filter(
    ormar.or_(
        ormar.and_(name='Test', rating__gte=3.0), 
        categories__name__in=['Toys', 'Books'])
    ).get()
# note: to read more about and_ and or_ read complex filters section below
```

### Python style filters

*  **exact** - exact match to value, sql `column = <VALUE>` 
      * can be written as `Track.album.name == 'Malibu`
*  **iexact** - exact match sql `column = <VALUE>` (case insensitive)
      * can be written as `Track.album.name.iexact('malibu')`
*  **contains** - sql `column LIKE '%<VALUE>%'`
       * can be written as `Track.album.name % 'Mal')`
       * can be written as `Track.album.name.contains('Mal')`
*  **icontains** - sql `column LIKE '%<VALUE>%'` (case insensitive)
       * can be written as `Track.album.name.icontains('mal')`
*  **in** - sql ` column IN (<VALUE1>, <VALUE2>, ...)`
       * can be written as `Track.album.name << ['Malibu', 'Barclay']`
       * can be written as `Track.album.name.in_(['Malibu', 'Barclay'])`
*  **isnull** - sql `column IS NULL` (and sql `column IS NOT NULL`) 
       * can be written as `Track.album.name >> None`
       * can be written as `Track.album.name.isnull(True)`
       * not null can be written as `Track.album.name.isnull(False)`
       * not null can be written as `~(Track.album.name >> None)`
       * not null can be written as `~(Track.album.name.isnull(True))`
*  **gt** - sql `column > <VALUE>` (greater than)
       * can be written as `Track.album.name > 3`
*  **gte** - sql `column >= <VALUE>` (greater or equal than)
       * can be written as `Track.album.name >= 3`
*  **lt** - sql `column < <VALUE>` (lower than)
       * can be written as `Track.album.name < 3`
*  **lte** - sql `column <= <VALUE>` (lower equal than)
       * can be written as `Track.album.name <= 3`
*  **startswith** - sql `column LIKE '<VALUE>%'` (exact start match)
       * can be written as `Track.album.name.startswith('Mal')`
*  **istartswith** - sql `column LIKE '<VALUE>%'` (case insensitive)
       * can be written as `Track.album.name.istartswith('mal')`
*  **endswith** - sql `column LIKE '%<VALUE>'` (exact end match)
       * can be written as `Track.album.name.endswith('ibu')`
*  **iendswith** - sql `column LIKE '%<VALUE>'` (case insensitive)
       * can be written as `Track.album.name.iendswith('IBU')`

Some samples:

```python
# sql: ( product.name = 'Test'  AND  product.rating >= 3.0 ) 
Product.objects.filter(
    (Product.name == 'Test') & (Product.rating >= 3.0)
).get()

# sql: ( product.name = 'Test' AND product.rating >= 3.0 ) 
#       OR (categories.name IN ('Toys', 'Books'))
Product.objects.filter(
        ((Product.name == 'Test') & (Product.rating >= 3.0)) | 
        (Product.categories.name << ['Toys', 'Books'])
    ).get()
```

!!!note
    A foreign-key accessor can also be compared directly to a primary-key value
    or a model instance, which is equivalent to the kwargs form and stays a
    single-table filter (no JOIN is added):

    ```python
    # both produce: WHERE books.author = 5
    Book.objects.filter(author=5)
    Book.objects.filter(Book.author == 5)

    # with an instance - the PK is extracted for you
    Book.objects.filter(Book.author == tolkien)
    ```

    This only applies to own FK columns. Many-to-many and reverse relations
    still require targeting a concrete column, e.g. `Category.products.name`.

!!!note 
    All methods that do not return the rows explicitly returns a QuerySet instance so
    you can chain them together

    So operations like `filter()`, `select_related()`, `limit()` and `offset()` etc. can be chained.
    
    Something like `Track.object.select_related("album").filter(album__name="Malibu").offset(1).limit(1).all()`

!!!warning 
    Note that you do not have to specify the `%` wildcard in contains and other
    filters, it's added for you. If you include `%` in your search value it will be escaped
    and treated as literal percentage sign inside the text.

### exclude

`exclude(*args, **kwargs) -> QuerySet`

Works exactly the same as filter and all modifiers (suffixes) are the same, but returns
a not condition.

So if you use `filter(name='John')` which equals to `where name = 'John'` in SQL,
the `exclude(name='John')` equals to `where name <> 'John'`

Note that all conditions are joined so if you pass multiple values it becomes a union of
conditions.

`exclude(name='John', age>=35)` will become `where not (name='John' and age>=35)`

Each chained `exclude()` call adds its own condition, so all of them apply.

`exclude(name='John').exclude(age>=35)` will become `where not name='John' and not age>=35`

```python
class Album(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    is_best_seller: bool = ormar.Boolean(default=False)

class Track(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    name: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
    play_count: int = ormar.Integer(nullable=True)
```

```python
notes = await Track.objects.exclude(position_gt=3).all()
# returns all tracks with position < 3
```

## Complex filters (including OR)

By default both `filter()` and `exclude()` methods combine provided filter options with
`AND` condition so `filter(name="John", age__gt=30)` translates into `WHERE name = 'John' AND age > 30`.

Sometimes it's useful to query the database with conditions that should not be applied 
jointly like `WHERE name = 'John' OR age > 30`, or build a complex where query that you would
like to have bigger control over. After all `WHERE (name = 'John' OR age > 30) and city='New York'` is
completely different than `WHERE name = 'John' OR (age > 30 and city='New York')`.

In order to build `OR` and nested conditions ormar provides two functions that can be used in 
`filter()` and `exclude()` in `QuerySet` and `QuerysetProxy`. 

!!!note
    Note that you can provide those methods in any other method like `get()` or `all()` that accepts `*args`. 

Call to `or_` and `and_` can be nested in each other, as well as combined with keyword arguments.
Since it sounds more complicated than it is, let's look at some examples.

Given a sample models like this:
```python
base_ormar_config = ormar.OrmarConfig(
    database=DatabaseConnection(DATABASE_URL),
    metadata=sqlalchemy.MetaData(),
)


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    author: Optional[Author] = ormar.ForeignKey(Author)
    title: str = ormar.String(max_length=100)
    year: int = ormar.Integer(nullable=True)
```

Let's create some sample data:

```python
tolkien = await Author(name="J.R.R. Tolkien").save()
await Book(author=tolkien, title="The Hobbit", year=1933).save()
await Book(author=tolkien, title="The Lord of the Rings", year=1955).save()
await Book(author=tolkien, title="The Silmarillion", year=1977).save()
sapkowski = await Author(name="Andrzej Sapkowski").save()
await Book(author=sapkowski, title="The Witcher", year=1990).save()
await Book(author=sapkowski, title="The Tower of Fools", year=2002).save()
```

We can construct some sample complex queries:

Let's select books of Tolkien **OR** books written after 1970

sql:
`WHERE ( authors.name = 'J.R.R. Tolkien' OR books.year > 1970 )`

#### Django style
```python
books = (
    await Book.objects.select_related("author")
    .filter(ormar.or_(author__name="J.R.R. Tolkien", year__gt=1970))
    .all()
)
assert len(books) == 5
```

#### Python style
```python
books = (
    await Book.objects.select_related("author")
    .filter((Book.author.name=="J.R.R. Tolkien") | (Book.year > 1970))
    .all()
)
assert len(books) == 5
```

Now let's select books written after 1960 or before 1940 which were written by Tolkien.

sql:
`WHERE ( books.year > 1960 OR books.year < 1940 ) AND authors.name = 'J.R.R. Tolkien'`

#### Django style
```python
# OPTION 1 - split and into separate call
books = (
    await Book.objects.select_related("author")
    .filter(ormar.or_(year__gt=1960, year__lt=1940))
    .filter(author__name="J.R.R. Tolkien")
    .all()
)
assert len(books) == 2

# OPTION 2 - all in one
books = (
    await Book.objects.select_related("author")
    .filter(
        ormar.and_(
            ormar.or_(year__gt=1960, year__lt=1940),
            author__name="J.R.R. Tolkien",
        )
    )
    .all()
)

assert len(books) == 2
assert books[0].title == "The Hobbit"
assert books[1].title == "The Silmarillion"
```

#### Python style
```python
books = (
    await Book.objects.select_related("author")
    .filter((Book.year > 1960) | (Book.year < 1940))
    .filter(Book.author.name == "J.R.R. Tolkien")
    .all()
)
assert len(books) == 2

# OPTION 2 - all in one
books = (
    await Book.objects.select_related("author")
    .filter(
        (
            (Book.year > 1960) | (Book.year < 1940)
        ) & (Book.author.name == "J.R.R. Tolkien")
    )
    .all()
)

assert len(books) == 2
assert books[0].title == "The Hobbit"
assert books[1].title == "The Silmarillion"
```

Books of Sapkowski from before 2000 or books of Tolkien written after 1960

sql:
`WHERE ( ( books.year > 1960 AND authors.name = 'J.R.R. Tolkien' ) OR ( books.year < 2000 AND authors.name = 'Andrzej Sapkowski' ) ) `

#### Django style
```python
books = (
    await Book.objects.select_related("author")
    .filter(
        ormar.or_(
            ormar.and_(year__gt=1960, author__name="J.R.R. Tolkien"),
            ormar.and_(year__lt=2000, author__name="Andrzej Sapkowski"),
        )
    )
    .all()
)
assert len(books) == 2
```

#### Python style
```python
books = (
    await Book.objects.select_related("author")
    .filter(    
        ((Book.year > 1960) & (Book.author.name == "J.R.R. Tolkien")) |
        ((Book.year < 2000) & (Book.author.name == "Andrzej Sapkowski"))
    )
    .all()
)
assert len(books) == 2
```

Of course those functions can have more than 2 conditions, so if we for example want 
books that contains 'hobbit':

sql:
`WHERE ( ( books.year > 1960 AND authors.name = 'J.R.R. Tolkien' ) OR 
( books.year < 2000 AND os0cec_authors.name = 'Andrzej Sapkowski' ) OR 
books.title LIKE '%hobbit%' )`

#### Django style
```python
books = (
    await Book.objects.select_related("author")
    .filter(
        ormar.or_(
            ormar.and_(year__gt=1960, author__name="J.R.R. Tolkien"),
            ormar.and_(year__lt=2000, author__name="Andrzej Sapkowski"),
            title__icontains="hobbit",
        )
    )
    .all()
)
```

#### Python style
```python
books = (
    await Book.objects.select_related("author")
    .filter(    
        ((Book.year > 1960) & (Book.author.name == "J.R.R. Tolkien")) |
        ((Book.year < 2000) & (Book.author.name == "Andrzej Sapkowski")) |
        (Book.title.icontains("hobbit"))
    )
    .all()
)
```

If you want or need to you can nest deeper conditions as deep as you want, in example to
achieve a query like this:

sql:
```
WHERE ( ( ( books.year > 1960 OR books.year < 1940 ) 
AND authors.name = 'J.R.R. Tolkien' ) OR 
( books.year < 2000 AND authors.name = 'Andrzej Sapkowski' ) )
```

You can construct a query as follows:

#### Django style
```python
books = (
    await Book.objects.select_related("author")
    .filter(
        ormar.or_(
            ormar.and_(
                ormar.or_(year__gt=1960, year__lt=1940),
                author__name="J.R.R. Tolkien",
            ),
            ormar.and_(year__lt=2000, author__name="Andrzej Sapkowski"),
        )
    )
    .all()
)
assert len(books) == 3
assert books[0].title == "The Hobbit"
assert books[1].title == "The Silmarillion"
assert books[2].title == "The Witcher"
```

#### Python style
```python
books = (
    await Book.objects.select_related("author")
    .filter(        
        (
            (
            (Book.year > 1960) |
            (Book.year < 1940)
            ) &
            (Book.author.name == "J.R.R. Tolkien")
        ) |
        (
            (Book.year < 2000) & 
            (Book.author.name == "Andrzej Sapkowski")
        )
    )
    .all()
)
assert len(books) == 3
assert books[0].title == "The Hobbit"
assert books[1].title == "The Silmarillion"
assert books[2].title == "The Witcher"
```

By now you should already have an idea how `ormar.or_` and `ormar.and_` works.
Of course, you could chain them in any other methods of queryset, so in example a perfectly
valid query can look like follows:

```python
books = (
    await Book.objects.select_related("author")
    .filter(ormar.or_(year__gt=1980, author__name="Andrzej Sapkowski"))
    .filter(title__startswith="The")
    .limit(1)
    .offset(1)
    .order_by("-id")
    .all()
)
assert len(books) == 1
assert books[0].title == "The Witcher"
```

Same applies to python style chaining and nesting.

#### Django style

Note that with django style you cannot provide the same keyword argument several times so queries like `filter(ormar.or_(name='Jack', name='John'))` are not allowed. If you want to check the same
column for several values simply use `in` operator: `filter(name__in=['Jack','John'])`.

If you pass only one parameter to `or_` or `and_` functions it's simply wrapped in parenthesis and
has no effect on actual query, so in the end all 3 queries are identical:

```python
await Book.objects.filter(title='The Hobbit').get()
await Book.objects.filter(ormar.or_(title='The Hobbit')).get()
await Book.objects.filter(ormar.and_(title='The Hobbit')).get()
```

!!!note
    Note that `or_` and `and_` queries will have `WHERE (title='The Hobbit')` but the parenthesis is redundant and has no real effect.

This feature can be used if you **really** need to use the same field name twice.
Remember that you cannot pass the same keyword arguments twice to the function, so
how you can query in example `WHERE (authors.name LIKE '%tolkien%') OR (authors.name LIKE '%sapkowski%'))`?

You cannot do:
```python
books = (
    await Book.objects.select_related("author")
        .filter(ormar.or_(
        author__name__icontains="tolkien",
        author__name__icontains="sapkowski" # you cannot use same keyword twice in or_!
    ))                                      # python syntax error
        .all()
)
```

But you can do this:

```python
books = (
    await Book.objects.select_related("author")
        .filter(ormar.or_(
        ormar.and_(author__name__icontains="tolkien"), # one argument == just wrapped in ()
        ormar.and_(author__name__icontains="sapkowski")
    ))
        .all()
)
assert len(books) == 5
```

#### Python style

Note that with python style you can perfectly use the same fields as many times as you want.

```python
books = (
    await Book.objects.select_related("author")
        .filter(
        (Book.author.name.icontains("tolkien")) |
        (Book.author.name.icontains("sapkowski"))
    ))                                      
        .all()
)
```

## get

`get(*args, **kwargs) -> Model`

Gets the first row from the db meeting the criteria set by kwargs.

When any args and/or kwargs are passed it's a shortcut equivalent to calling `filter(*args, **kwargs).get()`

!!!tip
    To read more about `filter` go to [filter](./#filter).
    
    To read more about `get` go to [read/get](../read/#get)

## get_or_none

Exact equivalent of get described above but instead of raising the exception returns `None` if no db record matching the criteria is found.

## get_or_create

`get_or_create(_defaults: Optional[dict[str, Any]] = None, *args, **kwargs) -> tuple[Model, bool]`

Combination of create and get methods.

When any args and/or kwargs are passed it's a shortcut equivalent to calling `filter(*args, **kwargs).get_or_create()`

!!!tip
    To read more about `filter` go to [filter](./#filter).
    
    To read more about `get_or_create` go to [read/get_or_create](../read/#get_or_create)

!!!warning
    When given item does not exist you need to pass kwargs for all required fields of the
    model, including but not limited to primary_key column (unless it's autoincrement).

## all

`all(*args, **kwargs) -> list[Optional["Model"]]`

Returns all rows from a database for given model for set filter options.

When any kwargs are passed it's a shortcut equivalent to calling `filter(*args, **kwargs).all()`

!!!tip
    To read more about `filter` go to [filter](./#filter).
    
    To read more about `all` go to [read/all](../read/#all)

### QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey`
returns the list of related models.

But at the same time it exposes subset of QuerySet API, so you can filter, create,
select related etc related models directly from parent model.

#### filter

Works exactly the same as [filter](./#filter) function above but allows you to filter related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section


#### exclude

Works exactly the same as [exclude](./#exclude) function above but allows you to filter related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section


#### get

Works exactly the same as [get](./#get) function above but allows you to filter related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

#### get_or_none

Exact equivalent of get described above but instead of raising the exception returns `None` if no db record matching the criteria is found.


#### get_or_create

Works exactly the same as [get_or_create](./#get_or_create) function above but allows
you to filter related objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

#### all

Works exactly the same as [all](./#all) function above but allows you to filter related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

## Sorting

### order_by

`order_by(columns: Union[list, str, OrderAction]) -> QuerySet`

With `order_by()` you can order the results from database based on your choice of
fields.

You can provide a string with field name or list of strings with different fields.

Ordering in sql will be applied in order of names you provide in order_by.

!!!tip 
    By default if you do not provide ordering `ormar` explicitly orders by all
    primary keys

!!!warning 
    If you are sorting by nested models that causes that the result rows are
    unsorted by the main model
    `ormar` will combine those children rows into one main model.

    Sample raw database rows result (sort by child model desc):
    ```
    MODEL: 1 - Child Model - 3
    MODEL: 2 - Child Model - 2
    MODEL: 1 - Child Model - 1
    ```
    
    will result in 2 rows of result:
    ```
    MODEL: 1 - Child Models: [3, 1] # encountered first in result, all children rows combined
    MODEL: 2 - Child Models: [2]
    ```
    
    The main model will never duplicate in the result

Given sample Models like following:

```python
--8<-- "../docs_src/queries/docs007.py"
```

To order by main model field just provide a field name

#### Django style
```python
toys = await Toy.objects.select_related("owner").order_by("name").all()
assert [x.name.replace("Toy ", "") for x in toys] == [
    str(x + 1) for x in range(6)
]
assert toys[0].owner == zeus
assert toys[1].owner == aphrodite
```

#### Python style
```python
toys = await Toy.objects.select_related("owner").order_by(Toy.name.asc()).all()
assert [x.name.replace("Toy ", "") for x in toys] == [
    str(x + 1) for x in range(6)
]
assert toys[0].owner == zeus
assert toys[1].owner == aphrodite
```


To sort on nested models separate field names with dunder '__'.

You can sort this way across all relation types -> `ForeignKey`, reverse virtual FK
and `ManyToMany` fields.

#### Django style
```python
toys = await Toy.objects.select_related("owner").order_by("owner__name").all()
assert toys[0].owner.name == toys[1].owner.name == "Aphrodite"
assert toys[2].owner.name == toys[3].owner.name == "Hermes"
assert toys[4].owner.name == toys[5].owner.name == "Zeus"
```

#### Python style
```python
toys = await Toy.objects.select_related("owner").order_by(Toy.owner.name.asc()).all()
assert toys[0].owner.name == toys[1].owner.name == "Aphrodite"
assert toys[2].owner.name == toys[3].owner.name == "Hermes"
assert toys[4].owner.name == toys[5].owner.name == "Zeus"
```

To sort in descending order provide a hyphen in front of the field name

#### Django style
```python
owner = (
    await Owner.objects.select_related("toys")
        .order_by("-toys__name")
        .filter(name="Zeus")
        .get()
)
assert owner.toys[0].name == "Toy 4"
assert owner.toys[1].name == "Toy 1"
```

#### Python style
```python
owner = (
    await Owner.objects.select_related("toys")
        .order_by(Owner.toys.name.desc())
        .filter(Owner.name == "Zeus")
        .get()
)
assert owner.toys[0].name == "Toy 4"
assert owner.toys[1].name == "Toy 1"
```

!!!note 
    All methods that do not return the rows explicitly returns a QuerySet instance so
    you can chain them together

    So operations like `filter()`, `select_related()`, `limit()` and `offset()` etc. can be chained.
    
    Something like `Track.objects.select_related("album").filter(album__name="Malibu").offset(1).limit(1).all()`

#### Controlling NULL placement with `nulls_ordering`

Both `.asc()` and `.desc()` accept an optional `nulls_ordering` argument so you can
decide whether `NULL` values come first or last in the result:

```python
import ormar

# nulls go to the end
await Song.objects.order_by(
    Song.sort_order.asc(nulls_ordering=ormar.NullsOrdering.LAST)
).all()

# nulls come first
await Owner.objects.order_by(
    Owner.toys.name.desc(nulls_ordering=ormar.NullsOrdering.FIRST)
).all()
```

!!!note
    On PostgreSQL and SQLite (3.30+) this emits the standard `NULLS FIRST` /
    `NULLS LAST` clause. MySQL does not support that syntax, so `ormar`
    emulates it by prepending an `IS NULL` / `IS NOT NULL` sort key — the
    result set is unchanged, only the ordering.

### Default sorting in ormar

Since order of rows in a database is not guaranteed, `ormar` **always** issues an `order by` sql clause to each (part of) query even if you do not provide order yourself. 

When querying the database with given model by default the `Model` is ordered by the `primary_key`
column ascending. If you wish to change the default behaviour you can do it by providing `orders_by`
parameter to `ormar_config`.

!!!tip
    To read more about models sort order visit [models](../models/index.md#model-sort-order) section of documentation

By default the relations follow the same ordering, but you can modify the order in which related models are loaded during query by providing `orders_by` and `related_orders_by`
parameters to relations.

!!!tip
    To read more about models sort order visit [relations](../relations/index.md#relationship-default-sort-order) section of documentation

Order in which order_by clauses are applied is as follows:

  * Explicitly passed `order_by()` calls in query
  * Relation passed `orders_by` and `related_orders_by` if exists
  * Model's `ormar_config` object `orders_by`
  * Model's `primary_key` column ascending (fallback, used if none of above provided)

**Order from only one source is applied to each `Model` (so that you can always overwrite it in a single query).**

That means that if you provide explicit `order_by` for a model in a query, the `Relation` and `Model` sort orders are skipped.

If you provide a `Relation` one, the `Model` sort is skipped.

Finally, if you provide one for `Model` the default one by `primary_key` is skipped.

### QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey`
returns the list of related models.

But at the same time it exposes subset of QuerySet API, so you can filter, create,
select related etc related models directly from parent model.

#### order_by

Works exactly the same as [order_by](./#order_by) function above but allows you to sort related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section


[querysetproxy]: ../relations/queryset-proxy.md
