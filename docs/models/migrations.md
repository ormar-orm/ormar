# Migrations

## Database Initialization

Note that all examples assume that you already have a database.

If that is not the case and you need to create your tables, that's super easy as `ormar` is using sqlalchemy for underlying table construction.

All you have to do is call `create_all()` like in the example below.

```python
import sqlalchemy
# get your database url in sqlalchemy format - same as used with DatabaseConnection in Model definition
engine = sqlalchemy.create_engine("sqlite:///test.db")
# note that this has to be the same metadata that is used in ormar Models definition
metadata.create_all(engine)
```

You can also create single tables, sqlalchemy tables are exposed in `ormar.ormar_config` object.

```python
import sqlalchemy
# get your database url in sqlalchemy format - same as used with DatabaseConnection in Model definition
engine = sqlalchemy.create_engine("sqlite:///test.db")
# Artist is an ormar model from previous examples
Artist.ormar_config.table.create(engine)
```

!!!warning
    You need to create the tables only once, so use a python console for that or remove the script from your production code after first use.


## Alembic usage

Likewise as with tables, since we base tables on sqlalchemy for migrations please use [alembic][alembic].

### Initialization

Use command line to reproduce this minimalistic example.

```bash
alembic init alembic
alembic revision --autogenerate -m "made some changes"
alembic upgrade head
```

### Where does `metadata` come from?

Ormar models are built on SQLAlchemy Core. Each model stores its table on a SQLAlchemy `MetaData` instance that you provide through `OrmarConfig`. Alembic needs that same `MetaData` object so it can discover which tables exist and autogenerate migrations.

The usual pattern is to create one shared `MetaData` object and pass it to every model:

```python
# my_project/models.py
import sqlalchemy
import ormar
from ormar import DatabaseConnection

database = DatabaseConnection("sqlite+aiosqlite:///db.sqlite")
metadata = sqlalchemy.MetaData()


class Author(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="authors",
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Book(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="books",
    )

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    author: Author = ormar.ForeignKey(Author)
```

You can then expose `metadata` from the module that defines your models:

```python
# env.py
from my_project.models import metadata

target_metadata = metadata
```

If you prefer, you can also grab the metadata from any model:

```python
target_metadata = Author.ormar_config.metadata
```

The important part is that every model uses the **same** `sqlalchemy.MetaData()` instance. If models live in different files or apps, import them all before Alembic inspects the metadata (see [Multiple apps with models](#multiple-apps-with-models) below).

### Complete project layout

Below is a minimal but complete layout that works with `alembic revision --autogenerate`.

```
my_project/
├── alembic/
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
├── my_project/
│   ├── __init__.py
│   └── models.py
├── alembic.ini
└── db.sqlite
```

`my_project/models.py`:

```python
import sqlalchemy
import ormar
from ormar import DatabaseConnection

database = DatabaseConnection("sqlite+aiosqlite:///db.sqlite")
metadata = sqlalchemy.MetaData()


class Author(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="authors",
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Book(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="books",
    )

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    author: Author = ormar.ForeignKey(Author)
```

`alembic.ini` (only the parts you typically need to change):

```ini
[alembic]
script_location = %(here)s/alembic

prepend_sys_path = .

sqlalchemy.url = sqlite:///db.sqlite
```

`alembic/env.py`:

```python
from logging.config import fileConfig
from pathlib import Path
import sys

from sqlalchemy import engine_from_config, pool
from alembic import context

# Add the project root to sys.path so `my_project` can be imported.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import the shared metadata (and models, so the metaclass registers the tables).
from my_project.models import metadata

target_metadata = metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # Required if you use ormar.UUID().
        user_module_prefix="sa.",
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # Required if you use ormar.UUID().
            user_module_prefix="sa.",
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

Then generate and run your first migration:

```bash
alembic revision --autogenerate -m "initial"
alembic upgrade head
```

### Multiple apps with models

If your models are split across several packages, the metadata must still be shared and every model must be imported before Alembic reads the metadata. A common layout:

```
my_project/
├── alembic/
│   └── env.py
├── my_project/
│   ├── __init__.py
│   ├── database.py
│   ├── models/
│   │   ├── __init__.py
│   │   ├── authors.py
│   │   └── books.py
│   ├── authors/
│   │   └── models.py
│   └── books/
│       └── models.py
└── alembic.ini
```

`my_project/database.py`:

```python
import sqlalchemy
from ormar import DatabaseConnection

database = DatabaseConnection("sqlite+aiosqlite:///db.sqlite")
metadata = sqlalchemy.MetaData()
```

`my_project/authors/models.py`:

```python
import ormar
from my_project.database import database, metadata


class Author(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="authors",
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
```

`my_project/books/models.py`:

```python
import ormar
from my_project.authors.models import Author
from my_project.database import database, metadata


class Book(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="books",
    )

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    author: Author = ormar.ForeignKey(Author)
```

`my_project/models/__init__.py` acts as a central import point:

```python
# Import every model so the metaclass registers its table on the shared metadata.
from my_project.authors.models import Author
from my_project.books.models import Book

# Re-export the shared metadata so env.py can import it from one place.
from my_project.database import metadata

__all__ = ["Author", "Book", "metadata"]
```

Then in `alembic/env.py`:

```python
from my_project.models import metadata, Author, Book  # noqa: F401

target_metadata = metadata
```

Importing the models is required: if a model class is never defined/imported, its table is never attached to `metadata` and Alembic will not see it.

### Detecting column type changes (`compare_type`)

Alembic's `--autogenerate` does **not** compare column types by default — it only
notices added/dropped columns and changes to nullability/server defaults. Swapping
`ormar.DateTime()` for `ormar.DateTime(timezone=True)`, widening `ormar.String(max_length=...)`,
or changing numeric precision will therefore produce an empty migration unless you opt in.

Pass `compare_type=True` to `context.configure(...)` in both `run_migrations_offline`
and `run_migrations_online`:

```python
context.configure(
    connection=connection,
    target_metadata=target_metadata,
    user_module_prefix='sa.',
    compare_type=True,
)
```

Ormar already forwards `timezone` (and other column kwargs) to the underlying SQLAlchemy
column, so once `compare_type=True` is set, type-only changes show up in autogenerated
revisions like any other change.

!!!info
    See the Alembic docs on
    [comparing types](https://alembic.sqlalchemy.org/en/latest/autogenerate.html#comparing-types)
    for the full list of caveats (some dialect-specific type swaps still need a custom
    `compare_type` callable).

### Excluding tables

You can also include/exclude specific tables with `include_object` parameter passed to `context.configure`. That should be a function returning `True/False` for given objects.

A sample function excluding tables starting with `data_` in name unless it's 'data_jobs':
```python
def include_object(object, name, type_, reflected, compare_to):
    if name and name.startswith('data_') and name not in ['data_jobs']:
        return False

    return True
```

!!!note
    Function parameters for `include_objects` (you can change the name) are required and defined in alembic
    to check what they do check the [alembic][alembic] documentation

And you pass it into context like (both in online and offline):
```python
context.configure(
    url=config.get_main_option("sqlalchemy.url"),
    target_metadata=target_metadata,
    literal_binds=True,
    dialect_opts={"paramstyle": "named"},
    user_module_prefix="sa.",
    include_object=include_object,
)
```

!!!info
    You can read more about table creation, altering and migrations in [sqlalchemy table creation][sqlalchemy table creation] documentation.

[fields]: ./fields.md
[relations]: ./relations/index.md
[queries]: ./queries.md
[pydantic]: https://pydantic-docs.helpmanual.io/
[sqlalchemy-core]: https://docs.sqlalchemy.org/en/latest/core/
[sqlalchemy-metadata]: https://docs.sqlalchemy.org/en/13/core/metadata.html
[sqlalchemy-async]: https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
[sqlalchemy connection string]: https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls
[sqlalchemy table creation]: https://docs.sqlalchemy.org/en/13/core/metadata.html#creating-and-dropping-database-tables
[alembic]: https://alembic.sqlalchemy.org/en/latest/tutorial.html
[save status]:  ../models/index/#model-save-status
[Internals]:  #internals
