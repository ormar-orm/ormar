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

```python
alembic init alembic
alembic revision --autogenerate -m "made some changes"
alembic upgrade head
```

### Sample env.py file

A quick example of alembic migrations should be something similar to:

When you have application structure like:

```
-> app
    -> alembic (initialized folder - so run alembic init alembic inside app folder)
    -> models (here are the models)
      -> __init__.py
      -> my_models.py
```

Your `env.py` file (in alembic folder) can look something like:

```python
from logging.config import fileConfig
from sqlalchemy import create_engine

from alembic import context
import sys, os

# add app folder to system path (alternative is running it from parent folder with python -m ...)
myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../../')

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# add your model's MetaData object here (the one used in ormar)
# for 'autogenerate' support
from app.models.my_models import metadata
target_metadata = metadata


# set your url here or import from settings
# note that by default url is in saved sqlachemy.url variable in alembic.ini file
URL = "sqlite:///test.db"


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        # if you use UUID field set also this param
        # the prefix has to match sqlalchemy import name in alembic
        # that can be set by sqlalchemy_module_prefix option (default 'sa.')
        user_module_prefix='sa.'
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = create_engine(URL)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # if you use UUID field set also this param
            # the prefix has to match sqlalchemy import name in alembic
            # that can be set by sqlalchemy_module_prefix option (default 'sa.')
            user_module_prefix='sa.'
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

```

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
        url=URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        user_module_prefix='sa.',
        include_object=include_object
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
