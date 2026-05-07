# Model methods

!!!tip
    Main interaction with the databases is exposed through a `QuerySet` object exposed on 
    each model as `Model.objects` similar to the django orm.

    To read more about **quering, joining tables, excluding fields etc. visit [queries][queries] section.**

Each model instance have a set of methods to `save`, `update` or `load` itself.

Available methods are described below.

## `pydantic` methods

Note that each `ormar.Model` is also a `pydantic.BaseModel`, so all `pydantic` methods are also available on a model,
especially `model_dump()` and `model_dump_json()` methods that can also accept `exclude`, `include` and other parameters.

To read more check [pydantic][pydantic] documentation

## model_construct()

`model_construct` is a raw equivalent of `__init__` method used for construction of new instances.

The difference is that `model_construct` skips validations, so it should be used when you know that data is correct and can be trusted.
The benefit of using construct is the speed of execution due to skipped validation.

!!!note 
    Note that in contrast to `pydantic.model_construct` method - the `ormar` equivalent will also process the nested related models.

!!!warning
    Bear in mind that due to skipped validation the `construct` method does not perform any conversions, checks etc. 
    So it's your responsibility to provide that data that is valid and can be consumed by the database.
    
    The only two things that construct still performs are:

    *  Providing a `default` value for not set fields
    *  Initialize nested ormar models if you pass a dictionary or a primary key value

## model_dump()

`model_dump` is a method inherited from `pydantic`, yet `ormar` adds its own parameters and has some nuances when working with default values,
therefore it's listed here for clarity.

`model_dump` as the name suggests export data from model tree to dictionary.

Explanation of model_dump parameters:

### include (`ormar` modified)

`include: Union[set, dict] = None`

Set or dictionary of field names to include in returned dictionary.

Note that `pydantic` has an uncommon pattern of including/ excluding fields in lists (so also nested models) by an index.
And if you want to exclude the field in all children you need to pass a `__all__` key to dictionary. 

You cannot exclude nested models in `set`s in `pydantic` but you can in `ormar` 
(by adding double underscore on relation name i.e. to exclude name of category for a book you can use `exclude={"book__category__name"}`)

`ormar` does not support by index exclusion/ inclusions and accepts a simplified and more user-friendly notation.

To check how you can include/exclude fields, including nested fields check out [fields](../queries/select-columns.md#fields) section that has an explanation and a lot of samples.

!!!note
    The fact that in `ormar` you can exclude nested models in sets, you can exclude from a whole model tree in `response_model_exclude` and `response_model_include` in fastapi!

### exclude (`ormar` modified)

`exclude: Union[set, dict] = None`

Set or dictionary of field names to exclude in returned dictionary.

Note that `pydantic` has an uncommon pattern of including/ excluding fields in lists (so also nested models) by an index.
And if you want to exclude the field in all children you need to pass a `__all__` key to dictionary. 

You cannot exclude nested models in `set`s in `pydantic` but you can in `ormar` 
(by adding double underscore on relation name i.e. to exclude name of category for a book you cen use `exclude={"book__category__name"}`)

`ormar` does not support by index exclusion/ inclusions and accepts a simplified and more user-friendly notation.

To check how you can include/exclude fields, including nested fields check out [fields](../queries/select-columns.md#fields) section that has an explanation and a lot of samples.

!!!note
    The fact that in `ormar` you can exclude nested models in sets, you can exclude from a whole model tree in `response_model_exclude` and `response_model_include` in fastapi!

### exclude_unset

`exclude_unset: bool = False`

Flag indicates whether fields which were not explicitly set when creating the model should be excluded from the returned dictionary.

!!!warning
    Note that after you save data into database each field has its own value -> either provided by you, default, or `None`.
    
    That means that when you load the data from database, **all** fields are set, and this flag basically stop working! 

```python
class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, default="Test")
    visibility: bool = ormar.Boolean(default=True)


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    price: float = ormar.Float(default=9.99)
    categories: list[Category] = ormar.ManyToMany(Category)

category = Category(name="Test 2")
assert category.model_dump() == {'id': None, 'items': [], 'name': 'Test 2',
                           'visibility': True}
assert category.model_dump(exclude_unset=True) == {'items': [], 'name': 'Test 2'}

await category.save()
category2 = await Category.objects.get()
assert category2.model_dump() == {'id': 1, 'items': [], 'name': 'Test 2',
                            'visibility': True}
# NOTE how after loading from db all fields are set explicitly
# as this is what happens when you populate a model from db
assert category2.model_dump(exclude_unset=True) == {'id': 1, 'items': [],
                                              'name': 'Test 2', 'visibility': True}
```

### exclude_defaults

`exclude_defaults: bool = False`

Flag indicates are equal to their default values (whether set or otherwise) should be excluded from the returned dictionary

```python
class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, default="Test")
    visibility: bool = ormar.Boolean(default=True)

class Item(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    price: float = ormar.Float(default=9.99)
    categories: list[Category] = ormar.ManyToMany(Category)
    
category = Category()
# note that Integer pk is by default autoincrement so optional
assert category.model_dump() == {'id': None, 'items': [], 'name': 'Test', 'visibility': True}
assert category.model_dump(exclude_defaults=True) == {'items': []}

# save and reload the data
await category.save()
category2 = await Category.objects.get()

assert category2.model_dump() == {'id': 1, 'items': [], 'name': 'Test', 'visibility': True}
assert category2.model_dump(exclude_defaults=True) == {'id': 1, 'items': []}
```

### exclude_none

`exclude_none: bool = False`

Flag indicates whether fields which are equal to `None` should be excluded from the returned dictionary.

```python
class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, default="Test", nullable=True)
    visibility: bool = ormar.Boolean(default=True)


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    price: float = ormar.Float(default=9.99)
    categories: list[Category] = ormar.ManyToMany(Category)


category = Category(name=None)
assert category.model_dump() == {'id': None, 'items': [], 'name': None,
                           'visibility': True}
# note the id is not set yet so None and excluded
assert category.model_dump(exclude_none=True) == {'items': [], 'visibility': True}

await category.save()
category2 = await Category.objects.get()
assert category2.model_dump() == {'id': 1, 'items': [], 'name': None,
                            'visibility': True}
assert category2.model_dump(exclude_none=True) == {'id': 1, 'items': [],
                                             'visibility': True}

```

### exclude_primary_keys (`ormar` only)

`exclude_primary_keys: bool = False`

Setting flag to `True` will exclude all primary key columns in a tree, including nested models.

```python
class Item(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)

item1 = Item(id=1, name="Test Item")
assert item1.model_dump() == {"id": 1, "name": "Test Item"}
assert item1.model_dump(exclude_primary_keys=True) == {"name": "Test Item"}
```

### exclude_through_models (`ormar` only)

`exclude_through_models: bool = False`

`Through` models are auto added for every `ManyToMany` relation, and they hold additional parameters on linking model/table.

Setting the `exclude_through_models=True` will exclude all through models, including Through models of submodels.

```python
class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    categories: list[Category] = ormar.ManyToMany(Category)

# tree defining the models
item_dict = {
            "name": "test",
            "categories": [{"name": "test cat"}, {"name": "test cat2"}],
        }
# save whole tree
await Item(**item_dict).save_related(follow=True, save_all=True)

# get the saved values
item = await Item.objects.select_related("categories").get()

# by default you can see the through models (itemcategory)
assert item.model_dump() == {'id': 1, 'name': 'test', 
                       'categories': [
                           {'id': 1, 'name': 'test cat', 
                            'itemcategory': {'id': 1, 'category': None, 'item': None}}, 
                           {'id': 2, 'name': 'test cat2', 
                            'itemcategory': {'id': 2, 'category': None, 'item': None}}
                       ]}

# you can exclude those fields/ models
assert item.model_dump(exclude_through_models=True) == {
                       'id': 1, 'name': 'test', 
                       'categories': [
                           {'id': 1, 'name': 'test cat'}, 
                           {'id': 2, 'name': 'test cat2'}
                       ]}
```

## model_dump_json()

`model_dump_json()` has exactly the same parameters as `model_dump()` so check above.

Of course the end result is a string with json representation and not a dictionary.

## get_pydantic()

`get_pydantic(include: Union[set, dict] = None, exclude: Union[set, dict] = None)`

This method allows you to generate `pydantic` models from your ormar models without you needing to retype all the fields.

Note that if you have nested models, it **will generate whole tree of pydantic models for you!** but in a way that prevents cyclic references issues.

Moreover, you can pass `exclude` and/or `include` parameters to keep only the fields that you want to, including in nested models.

That means that this way you can effortlessly create pydantic models for requests and responses in `fastapi`.

!!!Note
    To read more about possible excludes/includes and how to structure your exclude dictionary or set visit [fields](../queries/select-columns.md#fields) section of documentation

Given sample ormar models like follows:

```python
base_ormar_config = ormar.OrmarConfig(
    metadata=sqlalchemy.MetaData(),
    database=DatabaseConnection(DATABASE_URL),
)


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, default="test")
    category: Optional[Category] = ormar.ForeignKey(Category, nullable=True)
```

You can generate pydantic models out of it with a one simple call.

```python
PydanticCategory = Category.get_pydantic(include={"id", "name"})
```

Which will generate model equivalent of:

```python
class Category(BaseModel):
    id: Optional[int]
    name: Optional[str] = "test"
```

!!!warning
    Note that it's not a good practice to have several classes with same name in one module, as well as it would break `fastapi` docs.
    Thats's why ormar adds random 3 uppercase letters to the class name. In example above it means that in reality class would be named i.e. `Category_XIP(BaseModel)`.

To exclude or include nested fields you can use dict or double underscores.

```python
# both calls are equivalent
PydanticCategory = Category.get_pydantic(include={"id", "items__id"})
PydanticCategory = Category.get_pydantic(include={"id": ..., "items": {"id"}})
```

and results in a generated structure as follows:
```python
class Item(BaseModel):
    id: Optional[int]
    
class Category(BaseModel):
    id: Optional[int]
    items: Optional[list[Item]]
```

Of course, you can use also deeply nested structures and ormar will generate it's pydantic equivalent for you (in a way that exclude loops).

Note how `Item` model above does not have a reference to `Category` although in ormar the relation is bidirectional (and `ormar.Item` has `categories` field).

!!!warning
    Note that the generated pydantic model will inherit all **field** validators from the original `ormar` model, that includes the ormar choices validator as well as validators defined with `pydantic.validator` decorator.
    
    But, at the same time all root validators present on `ormar` models will **NOT** be copied to the generated pydantic model. Since root validator can operate on all fields and a user can exclude some fields during generation of pydantic model it's not safe to copy those validators.
    If required, you need to redefine/ manually copy them to generated pydantic model.

Methods decorated with `@pydantic.computed_field` are also carried over to the generated model and follow the same `include` / `exclude` rules as regular fields, so they appear in the model's schema and `model_dump()` output (e.g. in `fastapi` responses).

## load()

By default, when you query a table without prefetching related models, the ormar will still construct
your related models, but populate them only with the pk value. You can load the related model by calling `load()` method.

`load()` can also be used to refresh the model from the database (if it was changed by some other process). 

```python
track = await Track.objects.get(name='The Bird')
track.album.pk # will return malibu album pk (1)
track.album.name # will return None

# you need to actually load the data first
await track.album.load()
track.album.name # will return 'Malibu'
```

## load_all()

`load_all(follow: bool = False, exclude: Union[list, str, set, dict] = None) -> Model`

Method works like `load()` but also goes through all relations of the `Model` on which the method is called, 
and reloads them from database.

By default, the `load_all` method loads only models that are directly related (one step away) to the model on which the method is called.

But you can specify the `follow=True` parameter to traverse through nested models and load all of them in the relation tree.

!!!warning
    To avoid circular updates with `follow=True` set, `load_all` keeps a set of already visited Models, 
    and won't perform nested `loads` on Models that were already visited.
    
    So if you have a diamond or circular relations types you need to perform the loads in a manual way.
    
    ```python
    # in example like this the second Street (coming from City) won't be load_all, so ZipCode won't be reloaded
    Street -> District -> City -> Street -> ZipCode
    ```

Method accepts also optional exclude parameter that works exactly the same as exclude_fields method in `QuerySet`.
That way you can remove fields from related models being refreshed or skip whole related models.

Method performs one database query so it's more efficient than nested calls to `load()` and `all()` on related models.

!!!tip
    To read more about `exclude` read [exclude_fields][exclude_fields]

!!!warning
    All relations are cleared on `load_all()`, so if you exclude some nested models they will be empty after call.

## save()

`save() -> self`

You can create new models by using `QuerySet.create()` method or by initializing your model as a normal pydantic model 
and later calling `save()` method.

`save()` can also be used to persist changes that you made to the model, but only if the primary key is not set or the model does not exist in database.

The `save()` method does not check if the model exists in db, so if it does you will get a integrity error from your selected db backend if trying to save model with already existing primary key. 

```python
track = Track(name='The Bird')
await track.save() # will persist the model in database

track = await Track.objects.get(name='The Bird')
await track.save() # will raise integrity error as pk is populated
```

## update()

`update(_columns: list[str] = None, **kwargs) -> self`

You can update models by using `QuerySet.update()` method or by updating your model attributes (fields) and calling `update()` method.

If you try to update a model without a primary key set a `ModelPersistenceError` exception will be thrown.

To persist a newly created model use `save()` or `upsert(**kwargs)` methods.

```python
track = await Track.objects.get(name='The Bird')
await track.update(name='The Bird Strikes Again')
```

To update only selected columns from model into the database provide a list of columns that should be updated to `_columns` argument.

In example:

```python hl_lines="16"
class Movie(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, nullable=False, name="title")
    year: int = ormar.Integer()
    profit: float = ormar.Float()

terminator = await Movie(name='Terminator', year=1984, profit=0.078).save()

terminator.name = "Terminator 2"
terminator.year = 1991
terminator.profit = 0.520

# update only name
await terminator.update(_columns=["name"])

# note that terminator instance was not reloaded so
assert terminator.year == 1991

# but once you load the data from db you see it was not updated
await terminator.load()
assert terminator.year == 1984
```

!!!warning
    Note that `update()` does not refresh the instance of the Model, so if you change more columns than you pass in `_columns` list your Model instance will have different values than the database!

## upsert()

`upsert(**kwargs) -> self`

It's a proxy to either `save()` or `update(**kwargs)` methods described above.

If the primary key is set -> the `update` method will be called.

If the pk is not set the `save()` method will be called.

```python
track = Track(name='The Bird')
await track.upsert() # will call save as the pk is empty

track = await Track.objects.get(name='The Bird')
await track.upsert(name='The Bird Strikes Again') # will call update as pk is already populated
```


## delete()

You can delete models by using `QuerySet.delete()` method or by using your model and calling `delete()` method.

```python
track = await Track.objects.get(name='The Bird')
await track.delete() # will delete the model from database
```

!!!tip
    Note that that `track` object stays the same, only record in the database is removed.

## save_related()

`save_related(follow: bool = False, save_all: bool = False, exclude=Optional[Union[set, dict]]) -> None`

Method goes through all relations of the `Model` on which the method is called, 
and calls `upsert()` method on each model that is **not** saved. 

To understand when a model is saved check [save status][save status] section above.

By default the `save_related` method saved only models that are directly related (one step away) to the model on which the method is called.

But you can specify the `follow=True` parameter to traverse through nested models and save all of them in the relation tree.

By default save_related saves only model that has not `saved` status, meaning that they were modified in current scope.

If you want to force saving all of the related methods use `save_all=True` flag, which will upsert all related models, regardless of their save status.

If you want to skip saving some of the relations you can pass `exclude` parameter. 

`Exclude` can be a set of own model relations,
or it can be a dictionary that can also contain nested items. 

!!!note
    Note that `exclude` parameter in `save_related` accepts only relation fields names, so
    if you pass any other fields they will be saved anyway

!!!note
    To read more about the structure of possible values passed to `exclude` check `Queryset.fields` method documentation.

!!!warning
    To avoid circular updates with `follow=True` set, `save_related` keeps a set of already visited Models on each branch of relation tree, 
    and won't perform nested `save_related` on Models that were already visited.
    
    So if you have circular relations types you need to perform the updates in a manual way.

Note that with `save_all=True` and `follow=True` you can use `save_related()` to save whole relation tree at once.

Example:

```python
class Department(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    department_name: str = ormar.String(max_length=100)


class Course(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    course_name: str = ormar.String(max_length=100)
    completed: bool = ormar.Boolean()
    department: Optional[Department] = ormar.ForeignKey(Department)


class Student(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    courses = ormar.ManyToMany(Course)

to_save = {
            "department_name": "Ormar",
            "courses": [
                {"course_name": "basic1",
                 "completed": True,
                 "students": [
                     {"name": "Jack"},
                     {"name": "Abi"}
                 ]},
                {"course_name": "basic2",
                 "completed": True,
                 "students": [
                     {"name": "Kate"},
                     {"name": "Miranda"}
                 ]
                 },
            ],
        }
# initialize whole tree
department = Department(**to_save)

# save all at once (one after another)
await department.save_related(follow=True, save_all=True)

department_check = await Department.objects.select_all(follow=True).get()

to_exclude = {
    "id": ...,
    "courses": {
        "id": ...,
        "students": {"id", "studentcourse"}
    }
}
# after excluding ids and through models you get exact same payload used to
# construct whole tree
assert department_check.model_dump(exclude=to_exclude) == to_save

```


!!!warning
    `save_related()` iterates all relations and all models and upserts() them one by one,
    so it will save all models but might not be optimal in regard of number of database queries.

[fields]: ../fields.md
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
[Internals]:  #internals
[exclude_fields]: ../queries/select-columns.md#exclude_fields
