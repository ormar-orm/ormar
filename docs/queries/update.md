# Update data in database

Following methods and functions allow updating existing data in the database.

* `update(each: bool = False, **kwargs) -> int`
* `update_or_create(**kwargs) -> Model`
* `bulk_update(objects: list[Model], columns: list[str] = None) -> None`


* `Model`
    * `Model.update()` method
    * `Model.upsert()` method
    * `Model.save_related()` method


* `QuerysetProxy`
    * `QuerysetProxy.update_or_create(**kwargs)` method

## update

`update(each: bool = False, **kwargs) -> int`

QuerySet level update is used to update multiple records with the same value at once.

You either have to filter the QuerySet first or provide a `each=True` flag to update
whole table.

If you do not provide this flag or a filter a `QueryDefinitionError` will be raised.

Return number of rows updated.

Values are validated and converted like in `create()`: a foreign key accepts the
related model, a dictionary with its pk or the pk itself (`None` clears it), and a
JSON field accepts a value to dump or a JSON string.

```Python hl_lines="46-48"
--8<-- "../docs_src/queries/docs002.py"
```

!!!warning 
    Queryset needs to be filtered before updating to prevent accidental overwrite.

    To update whole database table `each=True` needs to be provided as a safety switch

## update_or_create

`update_or_create(**kwargs) -> Model`

Updates the model, or in case there is no match in database creates a new one.

```Python hl_lines="44-52"
--8<-- "../docs_src/queries/docs003.py"
```

!!!note 
    Note that if you want to create a new object you either have to pass pk column
    value or pk column has to be set as autoincrement

## bulk_update

`bulk_update(objects: list["Model"], columns: list[str] = None) -> None`

Allows to update multiple instance at once.

All `Models` passed need to have primary key column populated.

You can also select which fields to update by passing `columns` list as a list of string
names.

```python hl_lines="8"
# continuing the example from bulk_create
# update objects
for todo in todoes:
    todo.completed = False

# perform update of all objects at once
# objects need to have pk column set, otherwise exception is raised
await ToDo.objects.bulk_update(todoes)

completed = await ToDo.objects.filter(completed=False).all()
assert len(completed) == 3
```

## Model methods

Each model instance have a set of methods to `save`, `update` or `load` itself.

###update

You can update models by updating your model attributes (fields) and calling `update()` method.

If you try to update a model without a primary key set a `ModelPersistenceError` exception will be thrown.


!!!tip
    Read more about `update()` method in [models-update](../models/methods.md#update)

###upsert

It's a proxy to either `save()` or `update(**kwargs)` methods of a Model.
If the pk is set the `update()` method will be called.

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

### update_or_create

Works exactly the same as [update_or_create](./#update_or_create) function above but allows you to update or create related objects
from other side of the relation.

!!!tip
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

[querysetproxy]: ../relations/queryset-proxy.md
[models-upsert]: ../models/methods.md#upsert
[models-save-related]: ../models/methods.md#save_related
