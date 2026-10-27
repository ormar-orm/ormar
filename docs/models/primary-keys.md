# Primary keys

Every ormar model has a primary key. It's described by `Model.ormar_config.pk`
and its value is available as `instance.pk`.

## Single column key

Mark one field with `primary_key=True`. Integer keys autoincrement by default.

```python
class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="albums")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


album = await Album.objects.create(name="Malibu")
album.pk  # 1
```

String, UUID and other natural keys work the same way. Their value is set by you
or by a `default`:

```python
class Country(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="countries")

    iso_code: str = ormar.String(primary_key=True, max_length=2)
    name: str = ormar.String(max_length=100)
```

## Composite key

Mark several fields with `primary_key=True`. The key is made of those fields in
declaration order (fields inherited from an abstract parent or mixin come first),
and the table gets one `PRIMARY KEY (tenant_id, number)` constraint.

```python
class Order(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="orders")

    tenant_id: int = ormar.Integer(primary_key=True)
    number: int = ormar.Integer(primary_key=True)
    note: Optional[str] = ormar.String(max_length=200, nullable=True)


order = await Order.objects.create(tenant_id=1, number=7)
order.pk  # (1, 7)
order.pk = (1, 8)  # sets tenant_id and number
```

* The value of a composite key is a tuple in key order. It is `None` while any part
  is `None`, and `0` or `""` are valid parts.
* No part of a composite key autoincrements. Integer parts don't get the default
  autoincrement, and passing `autoincrement=True` raises `ModelDefinitionError`.
  All parts must be set before `save()` (a server default on a part counts as set).
* Foreign keys can be part of the key (`ForeignKey(..., primary_key=True)`, see
  [foreign keys in the primary key](../relations/foreign-key.md#foreign-keys-in-the-primary-key)).
* Foreign keys **to** models with a composite key are stored in one column per key
  part (see [foreign keys to several columns](../relations/foreign-key.md#foreign-keys-to-several-columns)).
  Many-to-many relations to models with a composite key are not supported yet and
  raise `ModelDefinitionError`.

## Querying by primary key

`pk` can be used in filters, `get()`, `exclude()` and `order_by()` of any model:

```python
await Album.objects.get(pk=1)
await Album.objects.filter(pk__in=[1, 2]).all()
await Album.objects.filter(pk__gt=10).all()
await Album.objects.order_by("-pk").all()

await Order.objects.get(pk=(1, 7))
await Order.objects.get(pk={"tenant_id": 1, "number": 7})
await Order.objects.filter(pk__in=[(1, 7), (2, 3)]).all()
await Order.objects.exclude(pk=(1, 7)).all()
await Order.objects.order_by("pk").all()  # by tenant_id, then number
```

`pk` lookups also work inside `ormar.or_()` / `ormar.and_()` groups and through
relations (`Book.objects.filter(author__pk=1)`). On a composite key `pk=` and
`pk__in=` compile to a row-value comparison,
`(tenant_id, number) IN ((1, 7), (2, 3))`, and only those two lookups are available.
A composite key value with a `None` part raises `QueryDefinitionError`. A key value
that doesn't match the key (wrong number of parts, unknown field names) raises
`QueryDefinitionError`. Python-style filters on the key (`Album.pk == 1`) work for
single column keys only.

## Changing a key and saving

ormar remembers the key a model was loaded or last saved with. `update()`,
`delete()` and `load()` target that row even after you change the key, so changing
a key and calling `update()` moves the row to the new key:

```python
album = await Album.objects.get(pk=1)
album.id = 5
await album.update()  # UPDATE albums SET id=5, ... WHERE id = 1
```

## Insert or update with `upsert()`

`upsert()` decides between an insert and an update:

* a model loaded from the database or already saved is updated,
* a model without a key is inserted,
* a model that was never saved but has its key set (a uuid default, a natural or a
  composite key) is looked up by key first: it's updated if the row exists and
  inserted otherwise.

```python
await Country(iso_code="PL", name="Poland").upsert()  # inserts
await Country(iso_code="PL", name="Polska").upsert()  # finds the row, updates
```

## Inspecting the key

```python
key = Order.ormar_config.pk
key.fields  # ("tenant_id", "number"), field names
key.column_aliases()  # database column names
key.columns()  # SQLAlchemy columns
key.is_composite  # True
```
