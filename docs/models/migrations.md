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

The usual pattern is to create one shared `MetaData` object and pass it to every model in its `OrmarConfig`. You can then expose that metadata from any model:

```python
target_metadata = Author.ormar_config.metadata
```

or export the shared `metadata` instance from your models module and import it into `env.py`.

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
--8<-- "../docs_src/models/docs019.py"
```

`alembic.ini`:

```ini
[alembic]
script_location = %(here)s/alembic

sqlalchemy.url = sqlite:///%(here)s/db.sqlite

# Logging configuration (required by fileConfig in env.py)
[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
```

!!! note "Async vs. sync database drivers"
    Ormar communicates with the database using an asynchronous driver (`sqlite+aiosqlite`, `asyncpg`, `asyncmy`), whereas Alembic's default `env.py` runs synchronously. The two connection URLs intentionally point to the same database through different driver protocols:

    | Database | Ormar (`DatabaseConnection`) | Alembic (`sqlalchemy.url`) |
    |---|---|---|
    | SQLite | `sqlite+aiosqlite:///db.sqlite` | `sqlite:///%(here)s/db.sqlite` |
    | PostgreSQL | `postgresql+asyncpg://...` | `postgresql+psycopg2://...` |
    | MySQL | `mysql+asyncmy://...` | `mysql+pymysql://...` |

    For SQLite, `%(here)s` makes Alembic's path relative to `alembic.ini`, while the relative path in ormar's URL resolves against the directory the application is started from. The two point at the same file only when you run the application from the project root, so use an absolute path in ormar if you start it from anywhere else.

    If you prefer to use async drivers throughout, you can initialize Alembic with `alembic init -t async`.

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
from my_project.models import metadata  # noqa: E402

target_metadata = metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # Required if you use ormar.UUID(); must match alembic's
        # sqlalchemy_module_prefix option (default "sa.").
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
            # Required if you use ormar.UUID(); must match alembic's
            # sqlalchemy_module_prefix option (default "sa.").
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
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
├── my_project/
│   ├── __init__.py
│   ├── database.py
│   ├── models/
│   │   └── __init__.py
│   ├── authors/
│   │   ├── __init__.py
│   │   └── models.py
│   └── books/
│       ├── __init__.py
│       └── models.py
└── alembic.ini
```

`alembic.ini` and `alembic/env.py` are the same as in the [complete project layout](#complete-project-layout) above, except for the metadata import shown at the end of this section.

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

Then in `alembic/env.py`, replace the metadata import with:

```python
# Import the models explicitly, not only metadata, so their tables are registered
# even if the import later changes to `from my_project.database import metadata`.
from my_project.models import Author, Book, metadata  # noqa: E402, F401

target_metadata = metadata
```

Importing the models is required: if a model class is never defined/imported, its table is never attached to `metadata` and Alembic will not see it.

!!!warning
    Do not import `metadata` only from `my_project.database`. Alembic does not raise an error when it gets an empty `metadata`: on a new database it generates an empty migration, and on an existing database it generates a migration that **drops every table**.

### Detecting column type changes (`compare_type`)

Alembic's `--autogenerate` does **not** compare column types by default — it only
notices added/dropped columns and changes to nullability. Swapping
`ormar.DateTime()` for `ormar.DateTime(timezone=True)`, widening `ormar.String(max_length=...)`,
or changing numeric precision will therefore produce an empty migration unless you opt in.

Pass `compare_type=True` to `context.configure(...)` in both `run_migrations_offline`
and `run_migrations_online`:

```python
context.configure(
    connection=connection,
    target_metadata=target_metadata,
    user_module_prefix="sa.",
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

### Column defaults (`compare_server_default`)

Since 0.31.0 a simple literal `default` (`bool`, `int`, `float`, `Decimal`, or `str` on a
`String` field) is also the column's `DEFAULT` in the database, see
[default](../fields/common-parameters.md#default). A migration that adds such a column now
contains `server_default=...`, so existing rows get the default instead of `NULL`:

```python
# is_active: bool = ormar.Boolean(default=False)
op.add_column('users', sa.Column('is_active', sa.Boolean(), server_default=sa.text('false'), nullable=True))
```

Alembic does not compare server defaults of existing columns unless you pass
`compare_server_default=True` to `context.configure(...)`. Without it, columns created before
0.31.0 keep no `DEFAULT` and autogenerate shows no change; with it, the next autogenerated
revision adds the `DEFAULT` to those columns (it does not change existing rows). Rows that are
already `NULL` need an explicit backfill, e.g.
`op.execute("UPDATE users SET is_active = false WHERE is_active IS NULL")`.

Against SQLite Alembic renders the `Boolean` default as `sa.text('(false)')`, which works the
same.

!!!warning
    With `compare_server_default=True` Alembic's PostgreSQL comparison raises an error for a
    `String` default containing `'` (Alembic quotes it without escaping), and SQLite or MySQL may
    report changes for some `String` defaults that did not change. Leave
    `compare_server_default` off, or pass a custom `compare_server_default` callable that skips
    those columns.

!!!note
    SQLite cannot change the default of an existing column with `ALTER TABLE`, so pass
    `render_as_batch=True` to `context.configure(...)` there: Alembic then recreates the table.

!!!note
    On MySQL the reflected default of a `Boolean` (`'0'` / `'1'`) and of a negative number
    differs from the one in the model, so with `compare_server_default=True` those columns show
    up as changed in every autogenerated revision. Either remove those operations from the
    revision, or pass a custom `compare_server_default` callable that returns `False` for them,
    see the Alembic docs on
    [comparing server defaults](https://alembic.sqlalchemy.org/en/latest/autogenerate.html#comparing-server-defaults).

### NOT NULL literal defaults (`not_null_defaults`)

With `OrmarConfig(not_null_defaults=True)` fields with a simple literal default become
`NOT NULL`, see [nullable](../fields/common-parameters.md#not-null-literal-defaults-not_null_defaults).
New columns work right away, as the column `DEFAULT` fills the existing rows. For existing
columns the autogenerated revision contains `alter_column(..., nullable=False)`, which fails
while any row is still `NULL`, so backfill those rows first:

```python
def upgrade() -> None:
    op.execute("UPDATE users SET is_active = false WHERE is_active IS NULL")
    with op.batch_alter_table("users") as batch_op:
        batch_op.alter_column(
            "is_active",
            existing_type=sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        )
```

`batch_alter_table` is needed on SQLite, which cannot change a column with `ALTER TABLE`
(set `render_as_batch=True` in `context.configure(...)` to have autogenerate emit it); on
PostgreSQL and MySQL it runs a plain `ALTER TABLE`. To keep a field nullable instead, set
`nullable=True` on it.

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

## Views

Models declared with `OrmarConfig(view=True)` (see [views](views.md)) are kept out of
`ormar_config.metadata`, so `metadata.create_all()` and Alembic's autogenerate never
create or drop them. Create views in a migration yourself:

```python
def upgrade():
    op.execute(
        "CREATE VIEW customer_totals AS "
        "SELECT customer AS customer_id, SUM(amount) AS total "
        "FROM purchases GROUP BY customer"
    )


def downgrade():
    op.execute("DROP VIEW customer_totals")
```

