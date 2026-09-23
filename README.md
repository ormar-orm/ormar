# ormar
<p>
<a href="https://pypi.org/project/ormar">
    <img src="https://img.shields.io/pypi/v/ormar.svg" alt="Pypi version">
</a>
<a href="https://pypi.org/project/ormar">
    <img src="https://img.shields.io/pypi/pyversions/ormar.svg" alt="Pypi version">
</a>
<a href="https://github.com/collerek/ormar/actions/workflows/test-package.yml">
<img src="https://github.com/collerek/ormar/actions/workflows/test-package.yml/badge.svg?branch=master" alt="Test package" height="20">
</a> 
<a href="https://codecov.io/gh/collerek/ormar">
    <img src="https://codecov.io/gh/collerek/ormar/branch/master/graph/badge.svg" alt="Coverage">
</a>
<a href="https://qlty.sh/gh/collerek/projects/ormar"><img src="https://qlty.sh/gh/collerek/projects/ormar/maintainability.svg" alt="Maintainability" /></a>
<a href="https://pepy.tech/project/ormar">
<img src="https://pepy.tech/badge/ormar"></a>
</p>

### Overview

The `ormar` package is an async mini ORM for Python, with support for **Postgres,
MySQL**, and **SQLite**.

The main benefits of using `ormar` are:

*  getting an **async ORM that can be used with async frameworks** (fastapi, starlette etc.)
*  getting just **one model to maintain** - you don't have to maintain pydantic and other orm models (sqlalchemy, peewee, gino etc.)

The goal was to create a simple ORM that can be **used directly (as request and response models) with [`fastapi`][fastapi]** that bases it's data validation on pydantic.

Ormar - apart from the obvious "ORM" in name - gets its name from _ormar_ in Swedish which means _snakes_, and _ormar_ in Croatian which means _cabinet_.

And what's a better name for python ORM than snakes cabinet :)

**If you like ormar remember to star the repository in [github](https://github.com/collerek/ormar)!**

The bigger community we build, the easier it will be to catch bugs and attract contributors ;)

### Documentation

Check out the [documentation][documentation] for details.

**Note that for brevity most of the documentation snippets omit the creation of the database
and scheduling the execution of functions for asynchronous run.**

If you want more real life examples than in the documentation you can see the [tests][tests] folder,
since they actually have to create and connect to a database in most of the tests.

Yet remember that those are - well - tests and not all solutions are suitable to be used in real life applications.

### Part of the `fastapi` ecosystem

As part of the fastapi ecosystem `ormar` is supported in libraries that somehow work with databases.

As of now `ormar` is supported by:

*  [`fastapi-crudrouter`](https://github.com/awtkns/fastapi-crudrouter)
*  [`fastapi-pagination`](https://github.com/uriyyo/fastapi-pagination)

If you maintain or use a different library and would like it to support `ormar` let us know how we can help.

### Dependencies

Ormar is built with:

* [`sqlalchemy core`][sqlalchemy-core] for query building.
* [`sqlalchemy async`][sqlalchemy-async] for cross-database async support.
* [`pydantic`][pydantic] for data validation.

### License

`ormar` is built as open-sorce software and will remain completely free (MIT license).

As I write open-source code to solve everyday problems in my work or to promote and build strong python
community you can say thank you and buy me a coffee or sponsor me with a monthly amount to help ensure my work remains free and maintained.

<a aria-label="Sponsor collerek" href="https://github.com/sponsors/collerek" style="text-decoration: none; color: #c9d1d9 !important;">
<div style="
    background-color: #21262d;
    border-color: #30363d;
    box-shadow:  0 0 transparent, 0 0 transparent;
    color: #c9d1d9 !important;
    border: 1px solid;
    border-radius: 6px;
    cursor: pointer;
    display: inline-block;
    font-size: 14px;
    padding: 10px;
    line-height: 0px;
    height: 40px;
">
<span style="color: #c9d1d9 !important;">Sponsor - Github Sponsors</span>
</div>
</a>

### Migrating from `sqlalchemy` and existing databases

If you currently use `sqlalchemy` and would like to switch to `ormar` check out the auto-translation
tool that can help you with translating existing sqlalchemy orm models so you do not have to do it manually.

**Beta** versions available at github: [`sqlalchemy-to-ormar`](https://github.com/collerek/sqlalchemy-to-ormar)
or simply `pip install sqlalchemy-to-ormar`

`sqlalchemy-to-ormar` can be used in pair with `sqlacodegen` to auto-map/ generate `ormar` models from existing database, even if you don't use `sqlalchemy` for your project.

### Migrations & Database creation

Because ormar is built on SQLAlchemy core, you can use [`alembic`][alembic] to provide
database migrations (and you really should for production code).

For tests and basic applications the `sqlalchemy` is more than enough:
```python
# note this is just a partial snippet full working example below
# 1. Imports
import sqlalchemy
import ormar
from ormar import DatabaseConnection

# 2. Initialization
DATABASE_URL = "sqlite+aiosqlite:///db.sqlite"
base_ormar_config = ormar.OrmarConfig(
    metadata=sqlalchemy.MetaData(),
    database=DatabaseConnection(DATABASE_URL),
)

# Define models here

# 3. Database creation and tables creation
engine = sqlalchemy.create_engine(DATABASE_URL.replace('+aiosqlite', ''))
base_ormar_config.metadata.create_all(engine)
```

For a sample configuration of alembic and more information regarding migrations and
database creation visit [migrations][migrations] documentation section.

### Package versions
**ormar is still under development:**
We recommend pinning any dependencies (with i.e. `ormar~=0.9.1`)

`ormar` also follows the release numeration that breaking changes bump the major number,
while other changes and fixes bump minor number, so with the latter you should be safe to
update, yet always read the [releases][releases] docs before.
`example: (0.5.2 -> 0.6.0 - breaking, 0.5.2 -> 0.5.3 - non breaking)`.

### Asynchronous Python

Note that `ormar` is an asynchronous ORM, which means that you have to `await` the calls to
the methods, that are scheduled for execution in an event loop. Python has a builtin module
[`asyncio`][asyncio] that allows you to do just that.

Note that most "normal" python interpreters do not allow execution of `await`
outside of a function (because you actually schedule this function for delayed execution
and don't get the result immediately).

In a modern web framework (like `fastapi`), the framework will handle this for you, but if
you plan to do this on your own you need to perform this manually like described in the
quick start below.

### Quick Start

Note that you can find the same script in examples folder on github.

```python
from typing import Optional

import ormar
import pydantic
import sqlalchemy

DATABASE_URL = "sqlite+aiosqlite:///db.sqlite"


# note that this step is optional -> all ormar cares is an individual
# OrmarConfig for each of the models, but this way you do not
# have to repeat the same parameters if you use only one database
base_ormar_config = ormar.OrmarConfig(
    database=ormar.DatabaseConnection(DATABASE_URL),
    metadata=sqlalchemy.MetaData(),
)

# Note that all type hints are optional
# below is a perfectly valid model declaration
# class Author(ormar.Model):
#     ormar_config = base_ormar_config.copy(tablename="authors")
#
#     id = ormar.Integer(primary_key=True) # <= notice no field types
#     name = ormar.String(max_length=100)


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


# note - normally import should be at the beginning of the file
import asyncio


async def setup_database():
    """Create all tables in the database."""
    if not base_ormar_config.database.is_connected:
        await base_ormar_config.database.connect()
    async with base_ormar_config.database.engine.begin() as conn:
        await conn.run_sync(base_ormar_config.metadata.drop_all)
        await conn.run_sync(base_ormar_config.metadata.create_all)
    if base_ormar_config.database.is_connected:
        await base_ormar_config.database.disconnect()


# create the database
# note that in production you should use migrations
# note that this is not required if you connect to existing database
# just to be sure we clear the db before
asyncio.run(setup_database())


# all functions below are divided into functionality categories
# note how all functions are defined with async - hence can use await AND needs to
# be awaited on their own
async def create():
    # Create some records to work with through QuerySet.create method.
    # Note that queryset is exposed on each Model's class as objects
    tolkien = await Author.objects.create(name="J.R.R. Tolkien")
    await Book.objects.create(author=tolkien, title="The Hobbit", year=1937)
    await Book.objects.create(author=tolkien, title="The Lord of the Rings", year=1955)
    await Book.objects.create(author=tolkien, title="The Silmarillion", year=1977)

    # alternative creation of object divided into 2 steps
    sapkowski = Author(name="Andrzej Sapkowski")
    # do some stuff
    await sapkowski.save()

    # or save() after initialization
    await Book(author=sapkowski, title="The Witcher", year=1990).save()
    await Book(author=sapkowski, title="The Tower of Fools", year=2002).save()

    # to read more about inserting data into the database
    # visit: https://collerek.github.io/ormar/queries/create/


async def read():
    # Fetch an instance, without loading a foreign key relationship on it.
    # Django style
    book = await Book.objects.get(title="The Hobbit")
    # or python style
    book = await Book.objects.get(Book.title == "The Hobbit")
    book2 = await Book.objects.first()

    # first() fetch the instance with lower primary key value
    assert book == book2

    # you can access all fields on loaded model
    assert book.title == "The Hobbit"
    assert book.year == 1937

    # when no condition is passed to get()
    # it behaves as last() based on primary key column
    book3 = await Book.objects.get()
    assert book3.title == "The Tower of Fools"

    # When you have a relation, ormar always defines a related model for you
    # even when all you loaded is a foreign key value like in this example
    assert isinstance(book.author, Author)
    # primary key is populated from foreign key stored in books table
    assert book.author.pk == 1
    # since the related model was not loaded all other fields are None
    assert book.author.name is None

    # Load the relationship from the database when you already have the related model
    # alternatively see joins section below
    await book.author.load()
    assert book.author.name == "J.R.R. Tolkien"

    # get all rows for given model
    authors = await Author.objects.all()
    assert len(authors) == 2

    # to read more about reading data from the database
    # visit: https://collerek.github.io/ormar/queries/read/


async def update():
    # read existing row from db
    tolkien = await Author.objects.get(name="J.R.R. Tolkien")
    assert tolkien.name == "J.R.R. Tolkien"
    tolkien_id = tolkien.id

    # change the selected property
    tolkien.name = "John Ronald Reuel Tolkien"
    # call update on a model instance
    await tolkien.update()

    # confirm that object was updated
    tolkien = await Author.objects.get(name="John Ronald Reuel Tolkien")
    assert tolkien.name == "John Ronald Reuel Tolkien"
    assert tolkien.id == tolkien_id

    # alternatively update data without loading
    await Author.objects.filter(name__contains="Tolkien").update(name="J.R.R. Tolkien")

    # to read more about updating data in the database
    # visit: https://collerek.github.io/ormar/queries/update/


async def delete():
    silmarillion = await Book.objects.get(year=1977)
    # call delete() on instance
    await silmarillion.delete()

    # alternatively delete without loading
    await Book.objects.delete(title="The Tower of Fools")

    # note that when there is no record ormar raises NoMatch exception
    try:
        await Book.objects.get(year=1977)
    except ormar.NoMatch:
        print("No book from 1977!")

    # to read more about deleting data from the database
    # visit: https://collerek.github.io/ormar/queries/delete/

    # note that despite the fact that record no longer exists in database
    # the object above is still accessible and you can use it (and i.e. save()) again.
    tolkien = silmarillion.author
    await Book.objects.create(author=tolkien, title="The Silmarillion", year=1977)


async def joins():
    # Tho join two models use select_related

    # Django style
    book = await Book.objects.select_related("author").get(title="The Hobbit")
    # Python style
    book = await Book.objects.select_related(Book.author).get(
        Book.title == "The Hobbit"
    )

    # now the author is already prefetched
    assert book.author.name == "J.R.R. Tolkien"

    # By default you also get a second side of the relation
    # constructed as lowercase source model name +'s' (books in this case)
    # you can also provide custom name with parameter related_name

    # Django style
    author = await Author.objects.select_related("books").all(name="J.R.R. Tolkien")
    # Python style
    author = await Author.objects.select_related(Author.books).all(
        Author.name == "J.R.R. Tolkien"
    )
    assert len(author[0].books) == 3

    # for reverse and many to many relations you can also prefetch_related
    # that executes a separate query for each of related models

    # Django style
    author = await Author.objects.prefetch_related("books").get(name="J.R.R. Tolkien")
    # Python style
    author = await Author.objects.prefetch_related(Author.books).get(
        Author.name == "J.R.R. Tolkien"
    )
    assert len(author.books) == 3

    # to read more about relations
    # visit: https://collerek.github.io/ormar/relations/

    # to read more about joins and subqueries
    # visit: https://collerek.github.io/ormar/queries/joins-and-subqueries/


async def filter_and_sort():
    # to filter the query you can use filter() or pass key-value pars to
    # get(), all() etc.
    # to use special methods or access related model fields use double
    # underscore like to filter by the name of the author use author__name
    # Django style
    books = await Book.objects.all(author__name="J.R.R. Tolkien")
    # python style
    books = await Book.objects.all(Book.author.name == "J.R.R. Tolkien")
    assert len(books) == 3

    # filter can accept special methods also separated with double underscore
    # to issue sql query ` where authors.name like "%tolkien%"` that is not
    # case sensitive (hence small t in Tolkien)
    # Django style
    books = await Book.objects.filter(author__name__icontains="tolkien").all()
    # python style
    books = await Book.objects.filter(Book.author.name.icontains("tolkien")).all()
    assert len(books) == 3

    # to sort use order_by() function of queryset
    # to sort decreasing use hyphen before the field name
    # same as with filter you can use double underscores to access related fields
    # Django style
    books = (
        await Book.objects.filter(author__name__icontains="tolkien")
        .order_by("-year")
        .all()
    )
    # python style
    books = (
        await Book.objects.filter(Book.author.name.icontains("tolkien"))
        .order_by(Book.year.desc())
        .all()
    )
    assert len(books) == 3
    assert books[0].title == "The Silmarillion"
    assert books[2].title == "The Hobbit"

    # to read more about filtering and ordering
    # visit: https://collerek.github.io/ormar/queries/filter-and-sort/


async def subset_of_columns():
    # to exclude some columns from loading when querying the database
    # you can use fields() method
    hobbit = await Book.objects.fields(["title"]).get(title="The Hobbit")
    # note that fields not included in fields are empty (set to None)
    assert hobbit.year is None
    assert hobbit.author is None

    # selected field is there
    assert hobbit.title == "The Hobbit"

    # alternatively you can provide columns you want to exclude
    hobbit = await Book.objects.exclude_fields(["year"]).get(title="The Hobbit")
    # year is still not set
    assert hobbit.year is None
    # but author is back
    assert hobbit.author is not None

    # also you cannot exclude primary key column - it's always there
    # even if you EXPLICITLY exclude it it will be there

    # note that each model have a shortcut for primary_key column which is pk
    # and you can filter/access/set the values by this alias like below
    assert hobbit.pk is not None

    # note that you cannot exclude fields that are not nullable
    # (required) in model definition
    try:
        await Book.objects.exclude_fields(["title"]).get(title="The Hobbit")
    except pydantic.ValidationError:
        print("Cannot exclude non nullable field title")

    # to read more about selecting subset of columns
    # visit: https://collerek.github.io/ormar/queries/select-columns/


async def pagination():
    # to limit number of returned rows use limit()
    books = await Book.objects.limit(1).all()
    assert len(books) == 1
    assert books[0].title == "The Hobbit"

    # to offset number of returned rows use offset()
    books = await Book.objects.limit(1).offset(1).all()
    assert len(books) == 1
    assert books[0].title == "The Lord of the Rings"

    # alternatively use paginate that combines both
    books = await Book.objects.paginate(page=2, page_size=2).all()
    assert len(books) == 2
    # note that we removed one book of Sapkowski in delete()
    # and recreated The Silmarillion - by default when no order_by is set
    # ordering sorts by primary_key column
    assert books[0].title == "The Witcher"
    assert books[1].title == "The Silmarillion"

    # to read more about pagination and number of rows
    # visit: https://collerek.github.io/ormar/queries/pagination-and-rows-number/


async def aggregations():
    # count:
    assert 2 == await Author.objects.count()

    # exists
    assert await Book.objects.filter(title="The Hobbit").exists()

    # maximum
    assert 1990 == await Book.objects.max(columns=["year"])

    # minimum
    assert 1937 == await Book.objects.min(columns=["year"])

    # average
    assert 1964.75 == await Book.objects.avg(columns=["year"])

    # sum
    assert 7859 == await Book.objects.sum(columns=["year"])

    # to read more about aggregated functions
    # visit: https://collerek.github.io/ormar/queries/aggregations/


async def raw_data():
    # extract raw data in a form of dicts or tuples
    # note that this skips the validation(!) as models are
    # not created from parsed data

    # get list of objects as dicts
    assert await Book.objects.values() == [
        {"id": 1, "author": 1, "title": "The Hobbit", "year": 1937},
        {"id": 2, "author": 1, "title": "The Lord of the Rings", "year": 1955},
        {"id": 4, "author": 2, "title": "The Witcher", "year": 1990},
        {"id": 5, "author": 1, "title": "The Silmarillion", "year": 1977},
    ]

    # get list of objects as tuples
    assert await Book.objects.values_list() == [
        (1, 1, "The Hobbit", 1937),
        (2, 1, "The Lord of the Rings", 1955),
        (4, 2, "The Witcher", 1990),
        (5, 1, "The Silmarillion", 1977),
    ]

    # filter data - note how you always get a list
    assert await Book.objects.filter(title="The Hobbit").values() == [
        {"id": 1, "author": 1, "title": "The Hobbit", "year": 1937}
    ]

    # select only wanted fields
    assert await Book.objects.filter(title="The Hobbit").values(["id", "title"]) == [
        {"id": 1, "title": "The Hobbit"}
    ]

    # if you select only one column you could flatten it with values_list
    assert await Book.objects.values_list("title", flatten=True) == [
        "The Hobbit",
        "The Lord of the Rings",
        "The Witcher",
        "The Silmarillion",
    ]

    # to read more about extracting raw values
    # visit: https://collerek.github.io/ormar/queries/aggregations/


async def with_connect(function):
    # note that for any other backend than sqlite you actually need to
    # connect to the database to perform db operations
    async with base_ormar_config.database:
        await function()

    # note that if you use framework like `fastapi` you shouldn't connect
    # in your endpoints but have a global connection pool
    # check https://collerek.github.io/ormar/fastapi/ and section with db connection


async def cleanup_database():
    """Drop all tables from the database."""
    if not base_ormar_config.database.is_connected:
        await base_ormar_config.database.connect()
    async with base_ormar_config.database.engine.begin() as conn:
        await conn.run_sync(base_ormar_config.metadata.drop_all)
    if base_ormar_config.database.is_connected:
        await base_ormar_config.database.disconnect()


# gather and execute all functions
# note that normally you use gather() function to run several functions
# concurrently but we actually modify the data and we rely on the order of functions
for func in [
    create,
    read,
    update,
    delete,
    joins,
    filter_and_sort,
    subset_of_columns,
    pagination,
    aggregations,
    raw_data,
]:
    print(f"Executing: {func.__name__}")
    asyncio.run(with_connect(func))

# drop the database tables
asyncio.run(cleanup_database())
```

## Ormar Specification

### QuerySet methods

*  `create(**kwargs): -> Model`
*  `get(*args, **kwargs): -> Model`
*  `get_or_none(*args, **kwargs): -> Optional[Model]`
*  `get_or_create(_defaults: Optional[dict[str, Any]] = None, *args, **kwargs) -> tuple[Model, bool]`
*  `first(*args, **kwargs): -> Model`
*  `update(each: bool = False, **kwargs) -> int`
*  `update_or_create(**kwargs) -> Model`
*  `bulk_create(objects: list[Model]) -> None`
*  `bulk_update(objects: list[Model], columns: list[str] = None) -> None`
*  `delete(*args, each: bool = False, **kwargs) -> int`
*  `all(*args, **kwargs) -> list[Optional[Model]]`
*  `iterate(*args, **kwargs) -> AsyncGenerator[Model]`
*  `filter(*args, **kwargs) -> QuerySet`
*  `exclude(*args, **kwargs) -> QuerySet`
*  `select_related(related: Union[list, str]) -> QuerySet`
*  `prefetch_related(related: Union[list, str]) -> QuerySet`
*  `limit(limit_count: int) -> QuerySet`
*  `offset(offset: int) -> QuerySet`
*  `count(distinct: bool = True) -> int`
*  `exists() -> bool`
*  `max(columns: list[str]) -> Any`
*  `min(columns: list[str]) -> Any`
*  `avg(columns: list[str]) -> Any`
*  `sum(columns: list[str]) -> Any`
*  `annotate(**aggregates) -> QuerySet` / `having(**filters) -> QuerySet` - per-parent `Count`/`Sum`/`Avg`/`Min`/`Max` annotations and filtering on them
*  `fields(columns: Union[list, str, set, dict]) -> QuerySet`
*  `exclude_fields(columns: Union[list, str, set, dict]) -> QuerySet`
*  `order_by(columns:Union[list, str]) -> QuerySet`
*  `values(fields: Union[list, str, set, dict])`
*  `values_list(fields: Union[list, str, set, dict])`


#### Relation types

*  One to many  - with `ForeignKey(to: Model)`
*  Many to many - with `ManyToMany(to: Model, Optional[through]: Model)`

#### Model fields types

Available Model Fields (with required args - optional ones in docs):

* `String(max_length)`
* `Text()`
* `Boolean()`
* `Integer()`
* `Float()`
* `Date()`
* `Time()`
* `DateTime()`
* `JSON()`
* `BigInteger()`
* `SmallInteger()`
* `Decimal(scale, precision)`
* `UUID()`
* `LargeBinary(max_length)`
* `Enum(enum_class)`
* `Enum` like Field - by passing `choices` to any other Field type
* `EncryptedString` - by passing `encrypt_secret` and `encrypt_backend`
* `ForeignKey(to)`
* `ManyToMany(to)`

### Available fields options
The following keyword arguments are supported on all field types.

* `primary_key: bool`
* `nullable: bool`
* `default: Any`
* `server_default: Any`
* `on_update: Any`
* `index: bool`
* `unique: bool`
* `choices: typing.Sequence`
* `name: str`

All fields are required unless one of the following is set:

* `nullable` - Creates a nullable column. Sets the default to `False`. Read the fields common parameters for details.
* `sql_nullable` - Used to set different setting for pydantic and the database. Sets the default to `nullable` value. Read the fields common parameters for details.
* `default` - Set a default value for the field. **Not available for relation fields**
* `server_default` - Set a default value for the field on server side (like sqlalchemy's `func.now()`). **Not available for relation fields**
* `primary key` with `autoincrement` - When a column is set to primary key and autoincrement is set on this column.
  Autoincrement is set by default on int primary keys.

### Available signals

Signals allow to trigger your function for a given event on a given Model.

* `pre_save`
* `post_save`
* `pre_update`
* `post_update`
* `pre_delete`
* `post_delete`
* `pre_relation_add`
* `post_relation_add`
* `pre_relation_remove`
* `post_relation_remove`
* `post_bulk_update`


[sqlalchemy-core]: https://docs.sqlalchemy.org/en/latest/core/
[sqlalchemy-async]: https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
[pydantic]: https://pydantic-docs.helpmanual.io/
[encode/orm]: https://github.com/encode/orm/
[alembic]: https://alembic.sqlalchemy.org/en/latest/
[fastapi]: https://fastapi.tiangolo.com/
[documentation]: https://collerek.github.io/ormar/
[migrations]: https://collerek.github.io/ormar/models/migrations/
[asyncio]: https://docs.python.org/3/library/asyncio.html
[releases]: https://collerek.github.io/ormar/releases/
[tests]: https://github.com/collerek/ormar/tree/master/tests
