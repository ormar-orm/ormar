# Insert data into database

Following methods allow you to insert data into the database.

* `create(on_conflict: Optional[Literal["ignore"]] = None, **kwargs) -> Optional[Model]`
* `get_or_create(_defaults: Optional[dict[str, Any]] = None, **kwargs) -> tuple[Model, bool]`
* `update_or_create(**kwargs) -> Model`
* `bulk_create(objects: list[Model], on_conflict=None, conflict_target=None, update_fields=None) -> None`


* `Model`
      * `Model.save()` method
      * `Model.upsert()` method
      * `Model.save_related()` method


* `QuerysetProxy`
      * `QuerysetProxy.create(**kwargs)` method
      * `QuerysetProxy.get_or_create(_defaults: Optional[dict[str, Any]] = None, **kwargs)` method
      * `QuerysetProxy.update_or_create(**kwargs)` method

## create

`create(**kwargs): -> Model`

Creates the model instance, saves it in a database and returns the updates model
(with pk populated if not passed and autoincrement is set).

The allowed kwargs are `Model` fields names and proper value types.

```python
class Album(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="album"
    )


    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
```

```python
malibu = await Album.objects.create(name="Malibu")
await Track.objects.create(album=malibu, title="The Bird", position=1)
```

The alternative is a split creation and persistence of the `Model`.

```python
malibu = Album(name="Malibu")
await malibu.save()
```

!!!tip 
    Check other `Model` methods in [models][models]

## get_or_create

`get_or_create(_defaults: Optional[dict[str, Any]] = None, **kwargs) -> tuple[Model, bool]`

Combination of create and get methods.

Tries to get a row meeting the criteria and if `NoMatch` exception is raised it creates
a new one with given kwargs and _defaults.

When `_defaults` dictionary is provided the values set in `_defaults` will **always** be set, including overwriting explicitly provided values. 
i.e. `get_or_create(_defaults: {"title": "I win"}, title="never used")` will always use "I win" as title whether you provide your own value in kwargs or not. 

```python
class Album(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="album"
    )

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
    Despite being a equivalent row from database the `album` and `album2` in
    example above are 2 different python objects!
    Updating one of them will not refresh the second one until you explicitly load() the
    fresh data from db.

!!!note 
    Note that if you want to create a new object you either have to pass pk column
    value or pk column has to be set as autoincrement

## update_or_create

`update_or_create(**kwargs) -> Model`

Updates the model, or in case there is no match in database creates a new one.

```Python hl_lines="44-52"
--8<-- "../docs_src/queries/docs003.py"
```

!!!note 
    Note that if you want to create a new object you either have to pass pk column
    value or pk column has to be set as autoincrement

## bulk_create

`bulk_create(objects: list["Model"]) -> None`

Allows you to create multiple objects at once.

A valid list of `Model` objects needs to be passed.

```python hl_lines="30-37"
--8<-- "../docs_src/queries/docs004.py"
```

!!!note
    Objects are inserted with one `INSERT` per distinct set of provided fields.
    If some objects set a value that others leave out (e.g. an explicit primary key
    or a field with `server_default`), they are inserted in separate statements
    inside one transaction, so generated primary keys may not follow the order
    of the passed list. If you need that order, split such objects into separate
    `bulk_create()` calls.

## Conflict handling

`create()` and `bulk_create()` can skip or update rows that conflict with existing
ones on a unique key, in the same statement as the insert, instead of failing with an
`IntegrityError`.

```python
class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50, unique=True)
    score: int = ormar.Integer(default=0)
    updated: bool = ormar.Boolean(default=False, on_update=True)
```

### Skip conflicting rows

```python
tag = await Tag.objects.create(name="python", on_conflict="ignore")
# a Tag when the row was inserted, None when a row with the same name
# (or with the same primary key) already exists
```

`on_conflict="ignore"` reacts to a conflict on any unique key (primary key, unique
column or unique constraint). Concurrent `create(on_conflict="ignore")` calls insert the
row once, the others get `None`, and no error is raised, so an open transaction stays
usable. `pre_save` is sent, `post_save` only when the row was inserted.

`None` does not load the existing row: use `get()` for it. The model built from the
passed kwargs is still added to the in-memory relations of models it was given, like
after a failed insert.

```python
await Tag.objects.bulk_create(
    [Tag(name="python"), Tag(name="sql")], on_conflict="ignore"
)
```

### Update conflicting rows

```python
await Tag.objects.bulk_create(
    [Tag(name="python", score=5), Tag(name="rust", score=1)],
    on_conflict="update",
    conflict_target=["name"],
    update_fields=["score"],
)
```

* `conflict_target` - fields of the unique key the rows conflict on: the primary key
  by default. A foreign key stored in several columns stands for all of its columns.
* `update_fields` - fields set from the inserted row: by default all inserted fields
  except the target and the primary key.
* Fields with `on_update` that are not in `update_fields` get their `on_update`
  value, as in `bulk_update()`.

`conflict_target` and `update_fields` are only allowed with `on_conflict="update"`.
When several objects of one call conflict on the same key, the last one wins.

`bulk_create()` sends no signals, sets no primary keys and marks all passed objects as
saved, the skipped ones too.

### Backends

| | PostgreSQL, SQLite | MySQL |
|---|---|---|
| `create(on_conflict="ignore")` | `INSERT ... ON CONFLICT DO NOTHING RETURNING` | plain `INSERT`, a duplicate key error is caught |
| `bulk_create(on_conflict="ignore")` | `ON CONFLICT DO NOTHING` | `ON DUPLICATE KEY UPDATE` of the primary key to itself |
| `bulk_create(on_conflict="update")` | `ON CONFLICT (target) DO UPDATE` | `ON DUPLICATE KEY UPDATE`, one row per statement when an `on_update` field is set |

!!!warning
    MySQL has no conflict target: `ON DUPLICATE KEY UPDATE` reacts to a conflict on
    **any** unique key of the table, so with several unique keys a row can update an
    existing row that conflicts on a different key than `conflict_target`. The target is
    used only on PostgreSQL and SQLite, which reject it when it does not match a unique
    key or constraint. MariaDB is not tested.

!!!note
    SQLite supports `ON CONFLICT` with `RETURNING` from version 3.35.

!!!note
    `on_conflict` is a reserved keyword argument of `create()`. To create a model with
    a field named `on_conflict` use `Model(on_conflict=...).save()`.

`QuerysetProxy.create()` accepts `on_conflict="ignore"` too, except for many to many
relations.

## Model methods

Each model instance have a set of methods to `save`, `update` or `load` itself.

###save

You can create new models by using `QuerySet.create()` method or by initializing your model as a normal pydantic model 
and later calling `save()` method.

!!!tip
    Read more about `save()` method in [models-save][models-save]

###upsert

It's a proxy to either `save()` or `update(**kwargs)` methods of a Model.
If the pk is not set the `save()` method will be called.

!!!tip
    Read more about `upsert()` method in [models-upsert][models-upsert]

###save_related

Method goes through all relations of the `Model` on which the method is called, 
and calls `upsert()` method on each model that is **not** saved. 

!!!tip
    Read more about `save_related()` method in [models-save-related][models-save-related]

## QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey` returns the list of related models.

But at the same time it exposes subset of QuerySet API, so you can filter, create, select related etc related models directly from parent model.

### create

Works exactly the same as [create](#create) function above but allows you to create related objects
from other side of the relation.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section


### get_or_create

Works exactly the same as [get_or_create](#get_or_create) function above but allows you to query or create related objects
from other side of the relation.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section


### update_or_create

Works exactly the same as [update_or_create](#update_or_create) function above but allows you to update or create related objects
from other side of the relation.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

[models]: ../models/methods.md
[models-save]: ../models/methods.md#save
[models-upsert]: ../models/methods.md#upsert
[models-save-related]: ../models/methods.md#save_related
[querysetproxy]: ../relations/queryset-proxy.md
