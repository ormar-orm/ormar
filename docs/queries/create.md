# Insert data into database

Following methods allow you to insert data into the database.

* `create(on_conflict=None, conflict_target=None, update_fields=None, update_values=None, **kwargs) -> Optional[Model]`
* `get_or_create(_defaults: Optional[dict[str, Any]] = None, **kwargs) -> tuple[Model, bool]`
* `update_or_create(**kwargs) -> Model`
* `bulk_create(objects: list[Model], on_conflict=None, conflict_target=None, update_fields=None, update_values=None) -> None`


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

If a concurrent caller inserts the matching row first, the failed insert is caught and the row is fetched again. Inside `database.transaction()` the insert runs in a savepoint, so a failed insert doesn't abort the transaction.

!!!warning
    On MySQL with the default `REPEATABLE READ` isolation, the retried lookup inside a transaction reads the snapshot taken at the transaction's first read, so it can't see the row a concurrent transaction committed later and the `IntegrityError` is raised instead.

## update_or_create

`update_or_create(**kwargs) -> Model`

Updates the model, or in case there is no match in database creates a new one.

```Python hl_lines="44-52"
--8<-- "../docs_src/queries/docs003.py"
```

!!!note 
    Note that if you want to create a new object you either have to pass pk column
    value or pk column has to be set as autoincrement

!!!tip
    `update_or_create()` looks the row up and then updates or inserts it in a separate
    statement, so concurrent calls can raise `IntegrityError` or overwrite each other.
    For an atomic insert or update by a unique key use
    [`create(on_conflict="update")`](#update-conflicting-rows).

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
device = await Device.objects.create(
    endpoint_hash=endpoint_hash,
    keys=keys,
    on_conflict="update",
    conflict_target=["endpoint_hash"],
    update_fields=["keys"],
)
# the stored row: a new device, or the existing one with new keys

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
  except the target and the primary key, none when `update_values` are given.
* `update_values` - values or column expressions set on conflict, see
  [below](#expressions-on-conflict).
* Fields with `on_update` that are in neither `update_fields` nor `update_values` get
  their `on_update` value, as in `bulk_update()`.

`conflict_target`, `update_fields` and `update_values` are only allowed with
`on_conflict="update"`.
When several objects of one call conflict on the same key, the last one wins.

`create(on_conflict="update")` inserts or updates the row in one statement and returns
the stored row, so fields left out of `update_fields` hold the values of the existing
row. Every field of the target needs a value (an autoincrement primary key that is not
passed cannot conflict). It sends `pre_save` and `post_save`, whether the row was
inserted or updated.

`bulk_create()` sends no signals, sets no primary keys and marks all passed objects as
saved, the skipped ones too.

### Expressions on conflict

`update_values` sets fields of the conflicting row to a value or to a
[column expression](update.md#column-expressions) computed by the database, in the same
statement as the insert, so concurrent calls never lose an update. In it `F("field")`
(or `Model.field`) is the column of the **existing** row and `ormar.Inserted("field")`
the value the insert tried to write:

```python
from ormar import Coalesce, F, Greatest, Inserted

# count the completions: 1 for a new row, then 2, 3, ...
level = await TrackLevel.objects.create(
    track=track,
    level=3,
    completed=1,
    on_conflict="update",
    conflict_target=["track", "level"],
    update_values={"completed": F("completed") + 1},
)

# never move the read marker back, keep the first join date
member = await Member.objects.create(
    chat=chat,
    user=user,
    read_id=message_id,
    joined=today,
    on_conflict="update",
    conflict_target=["chat", "user"],
    update_values={
        "read_id": Greatest(F("read_id"), Inserted("read_id")),
        "joined": Coalesce(F("joined"), Inserted("joined")),
    },
)

# the same expressions for every conflicting row of a bulk insert
await Score.objects.bulk_create(
    scores,
    on_conflict="update",
    conflict_target=["player"],
    update_values={"points": F("points") + Inserted("points"), "source": "merge"},
)
```

| | PostgreSQL, SQLite | MySQL |
|---|---|---|
| `F("x")` | `table.x` (the existing row) | `table.x` (the existing row) |
| `Inserted("x")` | `excluded.x` | `VALUES(x)` |

* When `update_values` are given, `update_fields` default to **none**: otherwise the
  inserted `completed=1` would be stored instead of the incremented value. List
  fields that should take the inserted value in `update_fields` next to
  `update_values`; a field in both is rejected.
* The rules of [expressions in `update()`](update.md#column-expressions) apply: an
  expression reads non-relation fields of the inserted model only (an upsert joins no
  tables, so no relation paths), sets no relation and reads no field set by the same
  upsert (`update_fields`, other `update_values`, fields with `on_update`); reading the
  field it sets is fine, and `Inserted()` can read any field. The literal goes on the
  right side. Plain values (`"source": "merge"`) are validated like `update()` values.
* `Inserted()` is only accepted in `update_values`; in filters and `update()` it
  raises `QueryDefinitionError`, as do unknown fields.
* The statement is cached per shape of the expressions: `F("completed") + 1` and
  `F("completed") + 5` run the same SQL with a different bound value.
* On MySQL a multi-row `bulk_create()` stores the literals of `update_values` in
  session user variables (`@ormar_u<n>_<k>`) together with the `on_update` values,
  see the warning below.

### Backends

| | PostgreSQL, SQLite | MySQL |
|---|---|---|
| `create(on_conflict="ignore")` | `INSERT ... ON CONFLICT DO NOTHING RETURNING` | plain `INSERT`, a duplicate key error is caught |
| `create(on_conflict="update")` | `INSERT ... ON CONFLICT (target) DO UPDATE ... RETURNING` | `ON DUPLICATE KEY UPDATE`, then a select by the target values |
| `bulk_create(on_conflict="ignore")` | `ON CONFLICT DO NOTHING` | `ON DUPLICATE KEY UPDATE` of the primary key to itself |
| `bulk_create(on_conflict="update")` | `ON CONFLICT (target) DO UPDATE` | `ON DUPLICATE KEY UPDATE`; `on_update` values and `update_values` literals are set once in user variables (`SET @ormar_on_update_<n>`, `@ormar_u<n>`) and read by all rows |

!!!warning
    MySQL has no conflict target: `ON DUPLICATE KEY UPDATE` reacts to a conflict on
    **any** unique key of the table, so with several unique keys a row can update an
    existing row that conflicts on a different key than `conflict_target`. The target is
    used only on PostgreSQL and SQLite, which reject it when it does not match a unique
    key or constraint. `create(on_conflict="update")` reads the row back by the target
    values and raises `NoMatch` when none matches: the conflict was on another key (that
    row is updated already), or the database stored a target value differently (floats,
    datetimes with fractional seconds). MariaDB is not tested.

!!!warning
    On MySQL a multi-row `bulk_create(on_conflict="update")` of a model with `on_update`
    fields or with literals in `update_values` stores those values in session user
    variables (`@ormar_on_update_<n>`, `@ormar_u<n>`) right before the insert. Do not run such calls concurrently
    on one connection (e.g. with `asyncio.gather()` inside one `transaction()`): one
    call can overwrite the values of the other. Separate connections are not affected.

!!!note
    SQLite supports `ON CONFLICT` with `RETURNING` from version 3.35.

!!!note
    `on_conflict`, `conflict_target`, `update_fields` and `update_values` are reserved
    keyword arguments of `create()`. To create a model with a field of such a name use
    `Model(on_conflict=...).save()`.

`QuerysetProxy.create()` accepts `on_conflict` too, except for many to many
relations. Through a reverse foreign key the relation is always updated, so a
conflicting row of another owner moves to the one it was created through.

`create(on_conflict="update")` with nothing to update (only the target fields passed)
works as an atomic get or create by a unique key: the conflicting row is returned
unchanged.

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
