# Models

## Defining models

By defining an ormar Model you get corresponding **Pydantic model** as well as **Sqlalchemy table** for free.
They are being managed in the background and you do not have to create them on your own.

### Model Class

To build an ormar model you simply need to inherit a `ormar.Model` class.

```Python hl_lines="10"
--8<-- "../docs_src/models/docs001.py"
```

### Defining Fields

Next assign one or more of the [Fields][fields] as a class level variables.

#### Basic Field Types

Each table **has to** have a primary key column, which you specify by setting `primary_key=True` on selected field.

Only one primary key column is allowed.

```Python hl_lines="16-18"
--8<-- "../docs_src/models/docs001.py"
```

!!! warning 
    Not assigning `primary_key` column or assigning more than one column per `Model` will raise `ModelDefinitionError`
    exception.

By default if you assign primary key to `Integer` field, the `autoincrement` option is set to true.

You can disable by passing `autoincrement=False`.

```Python 
id: int = ormar.Integer(primary_key=True, autoincrement=False)
```

#### Non Database Fields

Note that if you need a normal pydantic field in your model (used to store value on model or pass around some value) you can define a 
field like usual in pydantic.

Fields created like this are added to the `pydantic` model fields -> so are subject to validation according to `Field` type, 
also appear in `model_dump()` and `model_dump_json()` result. 

The difference is that **those fields are not saved in the database**. So they won't be included in underlying sqlalchemy `columns`, 
or `table` variables (check [Internals][Internals] section below to see how you can access those if you need).

Subsequently, pydantic fields won't be included in migrations or any database operation (like `save`, `update` etc.)

Fields like those can be passed around into payload in `fastapi` request and will be returned in `fastapi` response 
(of course only if you set their value somewhere in your code as the value is **not** fetched from the db. 
If you pass a value in `fastapi` `request` and return the same instance that `fastapi` constructs for you in `request_model`
you should get back exactly same value in `response`.).

!!!warning
    pydantic fields have to be always **Optional** and it cannot be changed (otherwise db load validation would fail)

```Python hl_lines="20"
--8<-- "../docs_src/models/docs014.py"
```

If you set pydantic field with `default` parameter and do not pass actual value in request you will always get default value.
Since it can be a function you can set `default=datetime.datetime.now` and get current timestamp each time you call an endpoint etc.

#### Non Database Fields in Fastapi

!!!note
    Note, that both pydantic and calculated_fields decorated field can be included/excluded in both `model_dump()` and `fastapi`
    response with `include`/`exclude` and `response_model_include`/`response_model_exclude` accordingly.

```python
# <==part of related code removed for clarity==>
base_ormar_config = ormar.OrmarConfig(
    database=DatabaseConnection(DATABASE_URL),
    metadata=sqlalchemy.MetaData(),
)


class User(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users2")

    id: int = ormar.Integer(primary_key=True)
    email: str = ormar.String(max_length=255, nullable=False)
    password: str = ormar.String(max_length=255)
    first_name: str = ormar.String(max_length=255)
    last_name: str = ormar.String(max_length=255)
    category: str = ormar.String(max_length=255, nullable=True)
    timestamp: datetime.datetime = pydantic.Field(
        default=datetime.datetime.now
    )

# <==part of related code removed for clarity==>
app = FastAPI()

@app.post("/users/")
async def create_user(user: User):
    return await user.save()

# <==part of related code removed for clarity==>

def test_excluding_fields_in_endpoints():
    client = TestClient(app)
    with client as client:
        timestamp = datetime.datetime.now()

        user = {
            "email": "test@domain.com",
            "password": "^*^%A*DA*IAAA",
            "first_name": "John",
            "last_name": "Doe",
            "timestamp": str(timestamp),
        }
        response = client.post("/users/", json=user)
        assert list(response.json().keys()) == [
            "id",
            "email",
            "first_name",
            "last_name",
            "category",
            "timestamp",
        ]
        # returned is the same timestamp
        assert response.json().get("timestamp") == str(timestamp).replace(" ", "T")


# <==part of related code removed for clarity==>
```

#### Fields names vs Column names

By default names of the fields will be used for both the underlying `pydantic` model and `sqlalchemy` table.

If for whatever reason you prefer to change the name in the database but keep the name in the model you can do this 
with specifying `name` parameter during Field declaration

Here you have a sample model with changed names
```Python hl_lines="19-22"
--8<-- "../docs_src/models/docs008.py"
```

Note that you can also change the ForeignKey column name
```Python hl_lines="36"
--8<-- "../docs_src/models/docs009.py"
```

But for now you cannot change the ManyToMany column names as they go through other Model anyway.
```Python hl_lines="44"
--8<-- "../docs_src/models/docs010.py"
```

### Overwriting the default QuerySet

If you want to customize the queries run by ormar you can define your own queryset class (that extends the ormar `QuerySet`) in your model class, default one is simply the `QuerySet`

You can provide a new class in `ormar_config` of your class as `queryset_class` parameter.

```python
import ormar
from ormar.queryset.queryset import QuerySet
from fastapi import HTTPException


class MyQuerySetClass(QuerySet):
    
    async def first_or_404(self, *args, **kwargs):
        entity = await self.get_or_none(*args, **kwargs) 
        if entity is None:
            # in fastapi or starlette
            raise HTTPException(404)

        
class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(
        queryset_class=MyQuerySetClass,
        tablename="book",
    )
    
    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=32)


# when book not found, raise `404` in your view.
book = await Book.objects.first_or_404(name="123")

```

### Type Hints

Note that for better IDE support and mypy checks you can provide type hints.

```Python hl_lines="16-18"
--8<-- "../docs_src/models/docs001.py"
```

Note that type hints are **optional** so perfectly valid `ormar` code can look like this:

```Python hl_lines="16-18"
--8<-- "../docs_src/models/docs012.py"
```

!!!warning
    Even if you use type hints **`ormar` does not use them to construct `pydantic` fields!**
    
    Type hints are there only to support static checkers and linting, 
    `ormar` construct annotations used by `pydantic` from own fields.



### Dependencies

Since ormar depends on [SQLAlchemy async][sqlalchemy-async] and [`sqlalchemy-core`][sqlalchemy-core] for database connection
and table creation you need to assign each `Model` with two special parameters.

#### Database Connection

One is `DatabaseConnection` instance created with your database url in [sqlalchemy connection string][sqlalchemy connection string] format (with async driver).

Created instance needs to be passed to every `Model` with `ormar_config` object `database` parameter.

```Python hl_lines="4 6 12"
--8<-- "../docs_src/models/docs001.py"
```

!!! tip
    You need to create the `DatabaseConnection` instance **only once** and use it for all models.
    You can create several ones if you want to use multiple databases.

#### Sqlalchemy

Second dependency is sqlalchemy `MetaData` instance.

Created instance needs to be passed to every `Model` with `ormar_config` object `metadata` parameter.

```Python hl_lines="1 7 13"
--8<-- "../docs_src/models/docs001.py"
```

!!! tip
    You need to create the `MetaData` instance **only once** and use it for all models. 
    You can create several ones if you want to use multiple databases.

#### Best practice

Note that `ormar` expects the field with name `ormar_config` that is an instance of `OrmarConfig` class.
To ease the config management, the `OrmarConfig` class provide `copy` method.
So instead of providing the same parameters over and over again for all models
you should create a base object and use its copy in all models.

```Python hl_lines="10-13 20 29"
--8<-- "../docs_src/models/docs013.py"
```

### Table Names

By default table name is created from Model class name as lowercase name plus 's'.

You can overwrite this parameter by providing `ormar_config` object's `tablename` argument.

```Python hl_lines="14-16"
--8<-- "../docs_src/models/docs002.py"
```

### Schemas

You can place a model in a non-default database schema by passing `schema=` to
`OrmarConfig` (or `OrmarConfig.copy()`). This lets you have multiple tables that
share a name as long as they live in different schemas, and lets you logically
group tables by domain.

```python
base = ormar.OrmarConfig(database=..., metadata=sqlalchemy.MetaData())

class HrEmployee(ormar.Model):
    ormar_config = base.copy(tablename="employees", schema="hr")
    id: int = ormar.Integer(primary_key=True)

class ItEmployee(ormar.Model):
    ormar_config = base.copy(tablename="employees", schema="it")
    id: int = ormar.Integer(primary_key=True)
```

`copy()` inherits the parent config's schema by default; pass `schema=None`
explicitly to opt out, or pass a different value to override.

#### Dialect support

| Dialect    | Status                  | Notes |
|------------|-------------------------|-------|
| PostgreSQL | Supported               | True schemas. Cross-schema foreign keys work. You must `CREATE SCHEMA` before `metadata.create_all()`. |
| MySQL      | Supported with caveats  | `schema` maps to a database name (qualified `db.tbl`). Cross-database FKs require both tables to be InnoDB. `DEFERRABLE`/`MATCH` are not allowed. You must `CREATE DATABASE` before `metadata.create_all()`. |
| SQLite     | Not supported           | SQLite forbids foreign keys whose parent and child live in different attached databases. Ormar raises `ModelDefinitionError` if it sees a cross-schema FK targeting SQLite. |

!!! warning
    `metadata.create_all()` does **not** create the schemas themselves. You must
    issue `CREATE SCHEMA <name>` (PostgreSQL) or `CREATE DATABASE <name>` (MySQL)
    before tables can be created in those schemas.

### Constraints

On a model level you can also set model-wise constraints on sql columns.

Right now only `IndexColumns`, `UniqueColumns` and `CheckColumns` constraints are supported. 

!!!note
    Note that both constraints should be used only if you want to set a name on constraint or want to set the index on multiple columns, otherwise `index` and `unique` properties on ormar fields are preferred.

!!!tip
    To read more about columns constraints like `primary_key`, `unique`, `ForeignKey` etc. visit [fields][fields].

#### UniqueColumns

You can set this parameter by providing `ormar_config` object `constraints` argument.

```Python hl_lines="14-17"
--8<-- "../docs_src/models/docs006.py"
```

!!!note
    Note that constraints are meant for combination of columns that should be unique. 
    To set one column as unique use [`unique`](../fields/common-parameters.md#unique) common parameter. 
    Of course you can set many columns as unique with this param but each of them will be checked separately.

#### IndexColumns

You can set this parameter by providing `ormar_config` object `constraints` argument.

```Python hl_lines="14-17"
--8<-- "../docs_src/models/docs017.py"
```

!!!note
    Note that constraints are meant for combination of columns that should be in the index. 
    To set one column index use [`unique`](../fields/common-parameters.md#index) common parameter. 
    Of course, you can set many columns as indexes with this param but each of them will be a separate index.

#### CheckColumns

You can set this parameter by providing `ormar_config` object `constraints` argument.

```Python hl_lines="16-21"
--8<-- "../docs_src/models/docs018.py"
```

!!!note
    Note that some databases do not actively support check constraints (such as MySQL).


### Pydantic configuration

As each `ormar.Model` is also a `pydantic` model, you might want to tweak the settings of the pydantic configuration.

The way to do this in pydantic is to adjust the settings on the `model_config` dictionary provided to your model, and it works exactly the same for ormar models.

So in order to set your own preferences you need to provide not only the `ormar_config` class but also the `model_config = ConfigDict()` class to your model.

!!!note
    To read more about available settings visit the [pydantic](https://pydantic-docs.helpmanual.io/usage/model_config/) config page.

Note that if you do not provide your own configuration, ormar will do it for you.
The default config provided is as follows:

```python
model_config = ConfigDict(validate_assignment=True, ser_json_bytes="base64")
```

So to overwrite setting or provide your own a sample model can look like following:
```Python hl_lines="17"
--8<-- "../docs_src/models/docs016.py"
```

### Extra fields in models

By default `ormar` forbids you to pass extra fields to Model.

If you try to do so the `ModelError` will be raised.

Since the extra fields cannot be saved in the database the default to disallow such fields seems a feasible option.

On the contrary in `pydantic` the default option is to ignore such extra fields, therefore `ormar` provides an `ormar_config.extra` setting to behave in the same way.

To ignore extra fields passed to `ormar` set this setting to `Extra.ignore` instead of default `Extra.forbid`.

Note that `ormar` does not allow accepting extra fields, you can only ignore them or forbid them (raise exception if present)

```python
from ormar import Extra, OrmarConfig

class Child(ormar.Model):
    ormar_config = OrmarConfig(
        tablename="children",
        extra=Extra.ignore  # set extra setting to prevent exceptions on extra fields presence
    )

    id: int = ormar.Integer(name="child_id", primary_key=True)
    first_name: str = ormar.String(name="fname", max_length=100)
    last_name: str = ormar.String(name="lname", max_length=100)
```

To set the same setting on all model check the [best practices]("../models/index/#best-practice") and `base_ormar_config` concept.

## Model sort order

When querying the database with given model by default the Model is ordered by the `primary_key`
column ascending. If you wish to change the default behaviour you can do it by providing `orders_by`
parameter to model `ormar_config` object.

Sample default ordering (not specified - so by primary key):
```python
base_ormar_config = ormar.OrmarConfig(
    database=DatabaseConnection(DATABASE_URL),
    metadata=sqlalchemy.MetaData(),
)


# default sort by column id ascending
class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(
        tablename="authors",
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
```
Modified
```python hl_lines="9"
base_ormar_config = ormar.OrmarConfig(
    database=DatabaseConnection(DATABASE_URL),
    metadata=sqlalchemy.MetaData(),
)

# now default sort by name descending
class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(
        orders_by = ["-name"],
        tablename="authors",
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
```

## Model Initialization

There are two ways to create and persist the `Model` instance in the database.

If you plan to modify the instance in the later execution of your program you can initiate your `Model` as a normal class and later await a `save()` call.  

```Python hl_lines="30-31"
--8<-- "../docs_src/models/docs007.py"
```

If you want to initiate your `Model` and at the same time save in in the database use a QuerySet's method `create()`.

For creating multiple objects at once a `bulk_create()` QuerySet's method is available.

Each model has a `QuerySet` initialised as `objects` parameter 

```Python hl_lines="33"
--8<-- "../docs_src/models/docs007.py"
```

!!!info
    To read more about `QuerySets` (including bulk operations) and available methods visit [queries][queries]

## `Model` save status

Each model instance is a separate python object and they do not know anything about each other.

```python
track1 = await Track.objects.get(name='The Bird')
track2 = await Track.objects.get(name='The Bird')
assert track1 == track2 # True

track1.name = 'The Bird2'
await track1.save()
assert track1.name == track2.name # False
# track2 does not update and knows nothing about track1
```

The objects itself have a saved status, which is set as following:

*  Model is saved after `save/update/load/upsert` method on model
*  Model is saved after `create/get/first/all/get_or_create/update_or_create` method
*  Model is saved when passed to `bulk_update` and `bulk_create`
*  Model is saved after `adding/removing` `ManyToMany` related objects (through model instance auto saved/deleted)
*  Model is **not** saved after change of any own field (including `pk` as `Model.pk` alias)
*  Model is **not** saved after adding/removing `ForeignKey` related object (fk column not saved)
*  Model is **not** saved after instantiation with `__init__` (w/o `QuerySet.create` or before calling `save`)

You can check if model is saved with `ModelInstance.saved` property

[fields]: ../fields/field-types.md
[relations]: ../relations/index.md
[queries]: ../queries/index.md
[pydantic]: https://pydantic-docs.helpmanual.io/
[sqlalchemy-core]: https://docs.sqlalchemy.org/en/latest/core/
[sqlalchemy-metadata]: https://docs.sqlalchemy.org/en/13/core/metadata.html
[sqlalchemy-async]: https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
[sqlalchemy connection string]: https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls
[sqlalchemy table creation]: https://docs.sqlalchemy.org/en/13/core/metadata.html#creating-and-dropping-database-tables
[alembic]: https://alembic.sqlalchemy.org/en/latest/tutorial.html
[save status]:  ../models/index/#model-save-status
[Internals]:  ../models/internals.md
