# Selecting subset of columns

To select only chosen columns of your model you can use following functions.

* `fields(columns: Union[list, str, set, dict]) -> QuerySet`
* `exclude_fields(columns: Union[list, str, set, dict]) -> QuerySet`
* `flatten_fields(columns: Union[list, str, set, tuple, dict, FieldAccessor]) -> QuerySet`


* `QuerysetProxy`
    * `QuerysetProxy.fields(columns: Union[list, str, set, dict])` method
    * `QuerysetProxy.exclude_fields(columns: Union[list, str, set, dict])` method

## fields

`fields(columns: Union[list, str, set, dict]) -> QuerySet`

With `fields()` you can select subset of model columns to limit the data load.

!!!note 
    Note that `fields()` and `exclude_fields()` works both for main models (on
    normal queries like `get`, `all` etc.)
    as well as `select_related` and `prefetch_related` models (with nested notation).

Given a sample data like following:

```python
--8<-- "../docs_src/select_columns/docs001.py"
```

You can select specified fields by passing a `str, list[str], set[str] or dict` with
nested definition.

To include related models use
notation `{related_name}__{column}[__{optional_next} etc.]`.

```python hl_lines="1-6"
all_cars = await (
    Car.objects
    .select_related('manufacturer')
    .fields(['id', 'name', 'manufacturer__name'])
    .all()
)
for car in all_cars:
    # excluded columns will yield None
    assert all(getattr(car, x) is None for x in ['year', 'gearbox_type', 'gears', 'aircon_type'])
    # included column on related models will be available, pk column is always included
    # even if you do not include it in fields list
    assert car.manufacturer.name == 'Toyota'
    # also in the nested related models - you cannot exclude pk - it's always auto added
    assert car.manufacturer.founded is None
```

`fields()` can be called several times, building up the columns to select.

If you include related models into `select_related()` call but you won't specify columns
for those models in fields

- implies a list of all fields for those nested models.

```python hl_lines="1-7"
all_cars = await (
    Car.objects
    .select_related('manufacturer')
    .fields('id')
    .fields(['name'])
    .all()
)
# all fields from company model are selected
assert all_cars[0].manufacturer.name == 'Toyota'
assert all_cars[0].manufacturer.founded == 1937
```

!!!warning 
    Mandatory fields cannot be excluded as it will raise `ValidationError`, to
    exclude a field it has to be nullable.

    The `values()` method can be used to exclude mandatory fields, though data will
    be returned as a `dict`.

You cannot exclude mandatory model columns - `manufacturer__name` in this example.

```python
await (
    Car.objects
    .select_related('manufacturer')
    .fields(['id', 'name', 'manufacturer__founded'])
    .all()
)
# will raise pydantic ValidationError as company.name is required
```

!!!tip 
    Pk column cannot be excluded - it's always auto added even if not explicitly
    included.

You can also pass fields to include as dictionary or set.

To mark a field as included in a dictionary use it's name as key and ellipsis as value.

To traverse nested models use nested dictionaries.

To include fields at last level instead of nested dictionary a set can be used.

To include whole nested model specify model related field name and ellipsis.

Below you can see examples that are equivalent:

```python
# 1. like in example above
await (
    Car.objects
    .select_related('manufacturer')
    .fields(['id', 'name', 'manufacturer__name'])
    .all()
)

# 2. to mark a field as required use ellipsis
await (
    Car.objects
    .select_related('manufacturer')
    .fields({'id': ...,
             'name': ...,
             'manufacturer': {
                 'name': ...
                }
             })
    .all()
)

# 3. to include whole nested model use ellipsis
await (
    Car.objects
    .select_related('manufacturer')
    .fields({'id': ...,
             'name': ...,
             'manufacturer': ...
             })
    .all()
)

# 4. to specify fields at last nesting level 
# you can also use set - equivalent to 2. above
await (
    Car.objects
    .select_related('manufacturer')
    .fields({'id': ...,
             'name': ...,
             'manufacturer': {'name'}
             })
    .all()
)

# 5. of course set can have multiple fields
await (
    Car.objects
    .select_related('manufacturer')
    .fields({'id': ...,
             'name': ...,
             'manufacturer': {'name', 'founded'}
             })
    .all()
)

# 6. you can include all nested fields, 
# but it will be equivalent of 3. above which is shorter
await (
    Car.objects
    .select_related('manufacturer')
    .fields({'id': ...,
             'name': ...,
             'manufacturer': {'id', 'name', 'founded'}
             })
    .all()
)

```

!!!note 
    All methods that do not return the rows explicitly returns a QuerySet instance so
    you can chain them together

    So operations like `filter()`, `select_related()`, `limit()` and `offset()` etc. can be chained.
    
    Something like `Track.objects.select_related("album").filter(album__name="Malibu").offset(1).limit(1).all()`

## exclude_fields

`exclude_fields(columns: Union[list, str, set, dict]) -> QuerySet`

With `exclude_fields()` you can select subset of model columns that will be excluded to
limit the data load.

It's the opposite of `fields()` method so check documentation above to see what options
are available.

Especially check above how you can pass also nested dictionaries and sets as a mask to
exclude fields from whole hierarchy.

!!!note 
    Note that `fields()` and `exclude_fields()` works both for main models (on
    normal queries like `get`, `all` etc.)
    as well as `select_related` and `prefetch_related` models (with nested notation).

Below you can find few simple examples:

```python
--8<-- "../docs_src/select_columns/docs001.py"
```

```python
# select manufacturer but only name,
# to include related models use notation {model_name}__{column}
all_cars = await (
    Car.objects
    .select_related('manufacturer')
    .exclude_fields([
        'year',
        'gearbox_type',
        'gears',
        'aircon_type',
        'company__founded'
    ])
    .all()
)
for car in all_cars:
    # excluded columns will yield None
    assert all(getattr(car, x) is None
               for x in [
                   'year',
                   'gearbox_type',
                   'gears',
                   'aircon_type'
               ])
    # included column on related models will be available,
    # pk column is always included
    # even if you do not include it in fields list
    assert car.manufacturer.name == 'Toyota'
    # also in the nested related models,
    # you cannot exclude pk - it's always auto added
    assert car.manufacturer.founded is None

# fields() can be called several times,
# building up the columns to select
# models included in select_related 
# but with no columns in fields list implies all fields
all_cars = await (
    Car.objects
    .select_related('manufacturer')
    .exclude_fields('year')
    .exclude_fields(['gear', 'gearbox_type'])
    .all()
)
# all fields from company model are selected
assert all_cars[0].manufacturer.name == 'Toyota'
assert all_cars[0].manufacturer.founded == 1937

# cannot exclude mandatory model columns,
# company__name in this example - note usage of dict/set this time
await (
    Car.objects
    .select_related('manufacturer')
    .exclude_fields([{'company': {'name'}}])
    .all()
)
# will raise pydantic ValidationError as company.name is required

```

!!!warning 
    Mandatory fields cannot be excluded as it will raise `ValidationError`, to
    exclude a field it has to be nullable.

    The `values()` method can be used to exclude mandatory fields, though data will
    be returned as a `dict`.

!!!tip 
    Pk column cannot be excluded - it's always auto added even if explicitly
    excluded.

!!!note 
    All methods that do not return the rows explicitly returns a QuerySet instance so
    you can chain them together

    So operations like `filter()`, `select_related()`, `limit()` and `offset()` etc. can be chained.
    
    Something like `Track.object.select_related("album").filter(album__name="Malibu").offset(1).limit(1).all()`


## flatten_fields

`flatten_fields(columns: Union[list, str, set, tuple, dict, FieldAccessor]) -> QuerySet`

With `flatten_fields()` you can render selected related models as their primary
key value on `model_dump()` instead of the default nested dict. This is useful
when your API clients expect `{"manufacturer": 1}` rather than
`{"manufacturer": {"id": 1, "name": "Toyota", ...}}`.

Accepts the same input forms as `fields()` / `exclude_fields()` (string, list,
set, tuple, dict-with-Ellipsis) plus `FieldAccessor` / list of accessors.
Works across foreign keys, many-to-many, and reverse relations.

```python hl_lines="1-6"
all_cars = await (
    Car.objects
    .select_related('manufacturer')
    .flatten_fields('manufacturer')
    .all()
)
assert all_cars[0].model_dump() == {
    'id': 1,
    'name': 'Corolla',
    'manufacturer': 1,  # flattened from nested dict to pk value
}
```

The same can be written in nested-dict form:

```python
Car.objects.flatten_fields({'manufacturer': ...})
```

Or with a `FieldAccessor`:

```python
Car.objects.flatten_fields(Car.manufacturer)
```

Deeply nested relations use `__`:

```python hl_lines="1-6"
cars = await (
    Car.objects
    .select_related('manufacturer__hq')
    .flatten_fields('manufacturer__hq')
    .all()
)
assert cars[0].model_dump()['manufacturer']['hq'] == 7  # just the hq pk
```

Lists of pks for many-to-many and reverse relations:

```python
posts = await Post.objects.flatten_fields('categories').all()
posts[0].model_dump()['categories']  # [1, 2, 3]
```

!!!note
    Relations listed in `flatten_fields()` are **auto-loaded** — single-valued
    foreign keys are added to `select_related()`, many-to-many and reverse
    relations to `prefetch_related()`. You don't have to load them yourself.

### flatten_all on model_dump

`model.model_dump(flatten_all=True)` collapses every related model at every depth
to its primary key in one shot. `model.model_dump(flatten_fields=...)` accepts
the same input forms as the queryset method and works even on models not loaded
via a queryset.

```python
car.model_dump(flatten_all=True)
# {"id": 1, "name": "Corolla", "manufacturer": 1, "lead_manager": 2}

car.model_dump(flatten_fields={'manufacturer': ...})
# {"id": 1, "name": "Corolla", "manufacturer": 1, "lead_manager": {...}}
```

### Validation rules

Flatten directives conflict with sub-field selection on the flattened relation
— you can't attach children to a scalar pk. The following raise
`QueryDefinitionError`:

* `flatten_fields('manufacturer')` combined with `fields({'manufacturer': {'name'}})`
* `flatten_fields('manufacturer')` combined with `exclude_fields({'manufacturer': {'name'}})`
* `flatten_fields(['manufacturer', 'manufacturer__hq'])` — the deeper path is unreachable
* `flatten_fields('name')` on a non-relation column
* `flatten_fields('manufacturer__nonexistent')` — unknown relation
* `model_dump(flatten_all=True, exclude_primary_keys=True)` — directly contradictory

Whole-relation `include`/`exclude` (e.g. `fields({'manufacturer'})`) is fine
alongside a flatten directive.

Filtering *through* a flattened relation still works — the join is generated
for the filter, and the rendered output is just the pk:

```python
await (
    Car.objects
    .filter(manufacturer__hq__city='Tokyo')
    .flatten_fields('manufacturer')
    .all()
)
# join visits hq for the filter; manufacturer still rendered as its pk
```

## QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey`
returns the list of related models.

But at the same time it exposes subset of QuerySet API, so you can filter, create,
select related etc related models directly from parent model.

### fields

Works exactly the same as [fields](./#fields) function above but allows you to select columns from related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### exclude_fields

Works exactly the same as [exclude_fields](./#exclude_fields) function above but allows you to select columns from related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### flatten_fields

Works exactly the same as [flatten_fields](./#flatten_fields) function above
but applied to the related-side queryset.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section


[querysetproxy]: ../relations/queryset-proxy.md
