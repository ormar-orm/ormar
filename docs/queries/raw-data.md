# Return raw data

Following methods allow you to execute a query but instead of returning ormar models those will return list of dicts or tuples.

* `values(fields = None, exclude_through = False) -> list[dict]`
* `values_list(fields = None, exclude_through = False, flatten = False) -> list`


* `QuerysetProxy`
    * `QuerysetProxy.values(fields = None, exclude_through = False)` method
    * `QuerysetProxy.values_list(fields = None, exclude_through= False, flatten = False)` method

!!!danger
    Note that `values` and `values_list` skips parsing the result to ormar models so skips also the validation of the result!

!!!warning
    Note that each entry in a result list is one to one reflection of a query result row. 
    Since rows are not parsed if you have one-to-many or many-to-many relation expect 
    duplicated columns values in result entries if one parent row have multiple related rows. 


## values

`values(fields: Union[list, str, set, dict] = None, exclude_through: bool = False) -> list[dict]`

Return a list of dictionaries representing the values of the columns coming from the database.

You can select a subset of fields with fields parameter, that accepts the same set of parameters as `fields()` method.

Note that passing fields to `values(fields)` is actually a shortcut for calling `fields(fields).values()`.

!!!tip
    To read more about what you can pass to fields and how to select nested models fields read [selecting columns](./select-columns.md#fields) docs

You can limit the number of rows by providing conditions in `filter()` and `exclude()`, but note that even if only one row (or no rows!) match your criteria you will return a list in response.

Example:

```python
# declared models

class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=40)
    sort_order: int = ormar.Integer(nullable=True)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=200)
    category: Optional[Category] = ormar.ForeignKey(Category)

# sample data
news = await Category(name="News", sort_order=0).save()
await Post(name="Ormar strikes again!", category=news).save()
await Post(name="Why don't you use ormar yet?", category=news).save()
await Post(name="Check this out, ormar now for free", category=news).save()
```

Access Post models:

```python
posts = await Post.objects.values()
assert posts == [
    {"id": 1, "name": "Ormar strikes again!", "category": 1},
    {"id": 2, "name": "Why don't you use ormar yet?", "category": 1},
    {"id": 3, "name": "Check this out, ormar now for free", "category": 1},
]
```

To select also related models use `select_related` or `prefetch_related`.

Note how nested models columns will be prefixed with full relation path coming from the main model (the one used in a query).


```python
# declare models

class User(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Role(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    users: list[User] = ormar.ManyToMany(User)

# sample data
creator = await User(name="Anonymous").save()
admin = await Role(name="admin").save()
editor = await Role(name="editor").save()
await creator.roles.add(admin)
await creator.roles.add(editor)
```

Select user with roles

```python
user = await User.objects.select_related("roles").values()
# note nested prefixes: roleuser and roles
assert user == [
    {
        "id": 1,
        "name": "Anonymous",
        "roleuser__id": 1,
        "roleuser__role": 1,
        "roleuser__user": 1,
        "roles__id": 1,
        "roles__name": "admin",
    },
    {
        "id": 1,
        "name": "Anonymous",
        "roleuser__id": 2,
        "roleuser__role": 2,
        "roleuser__user": 1,
        "roles__id": 2,
        "roles__name": "editor",
    },
]
```

!!!note
    Note how role to users relation is a `ManyToMany` relation so by default you also get through model columns.

Combine select related and fields to select only 3 fields.

!!!note
    `values()` and `values_list()` project only the columns referenced in
    `fields()`. Related models pulled into the query (either via
    `select_related()` directly, or implicitly by a filter like
    `filter(category__id=1)`) but never named in `fields()` are dropped
    from the result - including their through models for many-to-many
    relations.

    This differs from `fields()` on a regular ORM-loading query (`get`,
    `all`, etc.), where unmentioned related models in `select_related()`
    are still loaded in full so model construction does not lose
    mandatory columns.

!!!note
    Note that in contrary to other queryset methods here you can exclude the
    in-between models but keep the end columns, which does not make sense
    when parsing the raw data into models.

    So in relation category -> category_x_post -> post -> user you can exclude
    category_x_post and post models but can keep the user one. (in ormar model
    context that is not possible as if you would exclude through and post model
    there would be no way to reach user model from category model).

```python
user = (
        await Role.objects.select_related("users__categories")
        .filter(name="admin")
        .fields({"name": ..., "users": {"name": ..., "categories": {"name"}}})
        .exclude_fields("roleuser")
        .values()
    )
assert user == [
    {
        "name": "admin",
        "users__name": "Anonymous",
        "users__categories__name": "News",
    }
]
```

If you have multiple ManyToMany models in your query you would have to exclude each through model manually.

To avoid this burden `ormar` provides you with `exclude_through=False` parameter. 
If you set this flag to True **all through models will be fully excluded**.


```python
# equivalent to query above, note lack of exclude_fields call
user = (
    await Role.objects.select_related("users__categories")
    .filter(name="admin")
    .fields({"name": ..., "users": {"name": ..., "categories": {"name"}}})
    .values(exclude_through=True)
)
assert user == [
    {
        "name": "admin",
        "users__name": "Anonymous",
        "users__categories__name": "News",
    }
]
```

## values_list

`values_list(fields: Union[list, str, set, dict] = None, flatten: bool = False, exclude_through: bool = False) -> list`

Return a list of tuples representing the values of the columns coming from the database.

You can select a subset of fields with fields parameter, that accepts the same set of parameters as `fields()` method.

Note that passing fields to `values_list(fields)` is actually a shortcut for calling `fields(fields).values_list()`.

!!!tip
    To read more about what you can pass to fields and how to select nested models fields read [selecting columns](./select-columns.md#fields) docs

If you select only one column/field you can pass `flatten=True` which will return you a list of values instead of list of one element tuples.

!!!warning
    Setting `flatten=True` if more than one (or none which means all) fields are selected will raise `QueryDefinitionError` exception.

You can limit the number of rows by providing conditions in `filter()` and `exclude()`, but note that even if only one row (or no rows!) match your criteria you will return a list in response.

Example:

```python
# declared models

class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=40)
    sort_order: int = ormar.Integer(nullable=True)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=200)
    category: Optional[Category] = ormar.ForeignKey(Category)

# sample data
news = await Category(name="News", sort_order=0).save()
await Post(name="Ormar strikes again!", category=news).save()
await Post(name="Why don't you use ormar yet?", category=news).save()
await Post(name="Check this out, ormar now for free", category=news).save()
```

Access Post models:

```python
posts = await Post.objects.values_list()
# note how columns refer to id, name and category (fk)
assert posts == [
    (1, "Ormar strikes again!", 1),
    (2, "Why don't you use ormar yet?", 1),
    (3, "Check this out, ormar now for free", 1),
]
```

To select also related models use `select_related` or `prefetch_related`.

Let's complicate the relation and modify the previously mentioned Category model to refer to User model.

```python
class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=40)
    sort_order: int = ormar.Integer(nullable=True)
    # new column below
    created_by: Optional[User] = ormar.ForeignKey(User, related_name="categories")

```

Now create the sample data with link to user.

```python
creator = await User(name="Anonymous").save()
admin = await Role(name="admin").save()
editor = await Role(name="editor").save()
await creator.roles.add(admin)
await creator.roles.add(editor)
news = await Category(name="News", sort_order=0, created_by=creator).save()
```

Combine select related and fields to select only 3 fields.

!!!note
    `values()` and `values_list()` project only the columns referenced in
    `fields()`. Related models pulled into the query (either via
    `select_related()` directly, or implicitly by a filter like
    `filter(category__id=1)`) but never named in `fields()` are dropped
    from the result - including their through models for many-to-many
    relations.

    This differs from `fields()` on a regular ORM-loading query (`get`,
    `all`, etc.), where unmentioned related models in `select_related()`
    are still loaded in full so model construction does not lose
    mandatory columns.

!!!note
    Note that in contrary to other queryset methods here you can exclude the
    in-between models but keep the end columns, which does not make sense
    when parsing the raw data into models.

    So in relation category -> category_x_post -> post -> user you can exclude
    category_x_post and post models but can keep the user one. (in ormar model
    context that is not possible as if you would exclude through and post model
    there would be no way to reach user model from category model).

```python
user = (
        await Role.objects.select_related("users__categories")
        .filter(name="admin")
        .fields({"name": ..., "users": {"name": ..., "categories": {"name"}}})
        .exclude_fields("roleuser")
        .values_list()
    )
assert user == [("admin", "Anonymous", "News")]
```

If you have multiple ManyToMany models in your query you would have to exclude each through model manually.

To avoid this burden `ormar` provides you with `exclude_through=False` parameter. 
If you set this flag to True **all through models will be fully excluded**.

```python
# equivalent to query above, note lack of exclude_fields call
user = (
        await Role.objects.select_related("users__categories")
        .filter(name="admin")
        .fields({"name": ..., "users": {"name": ..., "categories": {"name"}}})
        .values_list(exclude_through=True)
    )
assert user == [("admin", "Anonymous", "News")]
```

Use flatten to get list of values.

```python
# using flatten with more than one field will raise exception!
await Role.objects.fields({"name", "id"}).values_list(flatten=True)

# proper usage
roles = await Role.objects.fields("name").values_list(flatten=True)
assert roles == ["admin", "editor"]
```

## QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey`
returns the list of related models.

But at the same time it exposes subset of QuerySet API, so you can filter, create,
select related etc related models directly from parent model.

!!!warning
    Because using `values` and `values_list` skips parsing of the models and validation, in contrast to all other read methods in querysetproxy those 2 **does not clear currently loaded related models** and **does not overwrite the currently loaded models** with result of own call!

### values

Works exactly the same as [values](./#values) function above but allows you to fetch related
objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

### values_list

Works exactly the same as [values_list](./#values_list) function above but allows
you to query or create related objects from other side of the relation.

!!!tip 
    To read more about `QuerysetProxy` visit [querysetproxy][querysetproxy] section

[querysetproxy]: ../relations/queryset-proxy.md
