# Internals

Apart from special parameters defined in the `Model` during definition (tablename, metadata etc.) the `Model` provides you with useful internals.

## Pydantic Model

All `Model` classes inherit from `pydantic.BaseModel` so you can access all normal attributes of pydantic models.

For example to list pydantic model fields you can:

```Python hl_lines="21"
--8<-- "../docs_src/models/docs003.py"
```

!!!tip
    Note how the primary key `id` field is optional as `Integer` primary key by default has `autoincrement` set to `True`.

!!!info
    For more options visit official [pydantic][pydantic] documentation.

## Sqlalchemy Table

To access auto created sqlalchemy table you can use `Model.ormar_config.table` parameter

For example to list table columns you can:

```Python hl_lines="25"
--8<-- "../docs_src/models/docs004.py"
```

!!!tip
    You can access the table primary key by `Course.ormar_config.pk`: its field names
    with `Course.ormar_config.pk.fields`, database column names with
    `Course.ormar_config.pk.column_aliases()` and SQLAlchemy columns with
    `Course.ormar_config.pk.columns()`

!!!info
    For more options visit official [sqlalchemy-metadata][sqlalchemy-metadata] documentation.

## Fields Definition

To access ormar `Fields` you can use `Model.ormar_config.model_fields` parameter

For example to list table model fields you can:

```Python hl_lines="23"
--8<-- "../docs_src/models/docs005.py"
```

!!!info
    Note that fields stored on a model are `classes` not `instances`.
    
    So if you print just model fields you will get:
    
    `{'id': <class 'ormar.fields.model_fields.Integer'>, `
    
      `'name': <class 'ormar.fields.model_fields.String'>, `
      
      `'completed': <class 'ormar.fields.model_fields.Boolean'>}`


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
[save status]:  ../models/#model-save-status
[Internals]:  #internals
