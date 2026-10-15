# ManyToMany

`ManyToMany(to, through)` has required parameters `to` and optional `through` that takes target and relation `Model` classes.  

Sqlalchemy column and Type are automatically taken from target `Model`.

* Sqlalchemy column: class of a target `Model` primary key column  
* Type (used for pydantic): type of a target `Model` 

## Defining Models

```Python hl_lines="35"
--8<-- "../docs_src/relations/docs002.py"
```

Create sample data:
```Python
guido = await Author.objects.create(first_name="Guido", last_name="Van Rossum")
post = await Post.objects.create(title="Hello, M2M", author=guido)
news = await Category.objects.create(name="News")
```

## Reverse relation

`ForeignKey` fields are automatically registering reverse side of the relation.

By default it's child (source) `Model` name + s, like `posts` in snippet below: 

```python hl_lines="25-26"
class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=40)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    categories: Optional[list[Category]] = ormar.ManyToMany(Category)

# create some sample data
post = await Post.objects.create(title="Hello, M2M")
news = await Category.objects.create(name="News")
await post.categories.add(news)
    
# now you can query and access from both sides:
post_check = Post.objects.select_related("categories").get()
assert post_check.categories[0] == news

# query through auto registered reverse side
category_check = Category.objects.select_related("posts").get()
assert category_check.posts[0] == post
```

Reverse relation exposes API to manage related objects also from parent side.

### related_name

By default, the related_name is generated in the same way as for the `ForeignKey` relation (class.name.lower()+'s'), 
but in the same way you can overwrite this name by providing `related_name` parameter like below:

```Python
categories: Optional[Union[Category, list[Category]]] = ormar.ManyToMany(
        Category, through=PostCategory, related_name="new_categories"
    )
```

!!!warning
    When you provide multiple relations to the same model `ormar` can no longer auto generate
    the `related_name` for you. Therefore, in that situation you **have to** provide `related_name`
    for all but one (one can be default and generated) or all related fields.


### Skipping reverse relation

If you are sure you don't want the reverse relation you can use `skip_reverse=True` 
flag of the `ManyToMany`.

If you set `skip_reverse` flag internally the field is still registered on the other 
side of the relationship so you can:

  * `filter` by related models fields from reverse model
  * `order_by` by related models fields from reverse model 
  
But you cannot:

  * access the related field from reverse model with `related_name`
  * even if you `select_related` from reverse side of the model the returned models won't be populated in reversed instance (the join is not prevented so you still can `filter` and `order_by` over the relation)
  * the relation won't be populated in `model_dump()` and `json()`
  * you cannot pass the nested related objects when populating from dictionary or json (also through `fastapi`). It will be either ignored or error will be raised depending on `extra` setting in pydantic `Config`.

Example:

```python
class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=40)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    categories: Optional[list[Category]] = ormar.ManyToMany(Category, skip_reverse=True)

# create some sample data
post = await Post.objects.create(title="Hello, M2M")
news = await Category.objects.create(name="News")
await post.categories.add(news)

assert post.categories[0] == news  # ok
assert news.posts  # Attribute error!

# but still can use in order_by
categories = (
    await Category.objects.select_related("posts").order_by("posts__title").all()
)
assert categories[0].first_name == "Test"

# note that posts are not populated for author even if explicitly
# included in select_related - note no posts in model_dump()
assert news.model_dump(exclude={"id"}) == {"name": "News"}

# still can filter through fields of related model
categories = await Category.objects.filter(posts__title="Hello, M2M").all()
assert categories[0].name == "News"
assert len(categories) == 1
```


## Through Model

Optionally if you want to add additional fields you can explicitly create and pass
the through model class.

```Python hl_lines="20-25 33"
--8<-- "../docs_src/relations/docs004.py"
```

!!!warning
    Note that even of you do not provide through model it's going to be created for you automatically and 
    still has to be included in example in `alembic` migrations. 

!!!tip
      Note that you need to provide `through` model if you want to 
      customize the `Through` model name or the database table name of this model.

If you do not provide the Through field it will be generated for you. 

The default naming convention is:

*  for class name it's union of both classes name (parent+other) so in example above 
   it would be `PostCategory`
*  for table name it similar but with underscore in between and s in the end of class 
   lowercase name, in example above would be `posts_categorys`
 
### Customizing Through relation names

By default `Through` model relation names default to related model name in lowercase.

So in example like this:
```python
... # course declaration omitted
class Student(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    courses = ormar.ManyToMany(Course)

# will produce default Through model like follows (example simplified)
class StudentCourse(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="students_courses")

    id: int = ormar.Integer(primary_key=True)
    student = ormar.ForeignKey(Student) # default name
    course = ormar.ForeignKey(Course)  # default name
```

To customize the names of fields/relation in Through model now you can use new parameters to `ManyToMany`:

* `through_relation_name` - name of the field leading to the model in which `ManyToMany` is declared
* `through_reverse_relation_name` - name of the field leading to the model to which `ManyToMany` leads to

Example:

```python
... # course declaration omitted
base_ormar_config = ormar.OrmarConfig(
    database=DatabaseConnection("sqlite+aiosqlite:///db.sqlite"),
    metadata=sqlalchemy.MetaData(),
)


class Student(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    courses = ormar.ManyToMany(Course,
                               through_relation_name="student_id",
                               through_reverse_relation_name="course_id")

# will produce Through model like follows (example simplified)
class StudentCourse(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="student_courses")

    id: int = ormar.Integer(primary_key=True)
    student_id = ormar.ForeignKey(Student) # set by through_relation_name
    course_id = ormar.ForeignKey(Course)  # set by through_reverse_relation_name
```    

!!!note
    Note that explicitly declaring relations in Through model is forbidden, so even if you
    provide your own custom Through model you cannot change the names there and you need to use
    same `through_relation_name` and `through_reverse_relation_name` parameters.

## Overriding foreign key constraint names on the through model

Auto-generated foreign key constraint names on the through model can be
overridden with:

* `through_foreign_key_name` - name of the FK constraint on the column that
  references the model where `ManyToMany` is declared (the owner side).
* `through_reverse_foreign_key_name` - name of the FK constraint on the column
  that references the target model.

This is primarily useful for databases with short identifier limits (for
example MySQL's 64 character limit) where the auto-generated name would be
truncated.

```python
class Student(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    courses = ormar.ManyToMany(
        Course,
        through_foreign_key_name="fk_sc_student",
        through_reverse_foreign_key_name="fk_sc_course",
    )
```

## Making through relation columns non-nullable

By default the auto-generated foreign key columns on the through table are
nullable (matching SQLAlchemy's default for non primary-key columns). This can
be overridden per column with:

* `through_relation_nullable` - controls nullability of the column pointing to
  the model where `ManyToMany` is declared (the owner side). Defaults to `True`.
* `through_reverse_relation_nullable` - controls nullability of the column
  pointing to the target model. Defaults to `True`.

Set either (or both) to `False` when you want the database to enforce that a
through row always references both sides.

```python
class Student(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    courses = ormar.ManyToMany(
        Course,
        through_relation_nullable=False,
        through_reverse_relation_nullable=False,
    )
```

## Through Fields

The through field is auto added to the reverse side of the relation. 

The exposed field is named as lowercase `Through` class name.

The exposed field **explicitly has no relations loaded** as the relation is already populated in `ManyToMany` field,
so it's useful only when additional fields are provided on `Through` model.

In a sample model setup as following:

```Python hl_lines="20-25 33"
--8<-- "../docs_src/relations/docs004.py"
```

the through field can be used as a normal model field in most of the QuerySet operations.

Note that through field is attached only to related side of the query so:

```python
post = await Post.objects.select_related("categories").get()
# source model has no through field
assert post.postcategory is None
# related models have through field
assert post.categories[0].postcategory is not None

# same is applicable for reversed query
category = await Category.objects.select_related("posts").get()
assert category.postcategory is None
assert category.posts[0].postcategory is not None
```

Through field can be used for filtering the data.
```python
post = (
        await Post.objects.select_related("categories")
        .filter(postcategory__sort_order__gt=1)
        .get()
        )
```

!!!tip
    Note that despite that the actual instance is not populated on source model,
    in queries, order by statements etc you can access through model from both sides.
    So below query has exactly the same effect (note access through `categories`)
    
    ```python
    post = (
        await Post.objects.select_related("categories")
        .filter(categories__postcategory__sort_order__gt=1)
        .get()
        )
    ```

Through model can be used in order by queries.
```python
post = (
        await Post.objects.select_related("categories")
        .order_by("-postcategory__sort_order")
        .get()
    )
```

You can also select subset of the columns in a normal `QuerySet` way with `fields` 
and `exclude_fields`.

```python
post2 = (
        await Post.objects.select_related("categories")
        .exclude_fields("postcategory__param_name")
        .get()
        )
```

!!!warning
    Note that because through fields explicitly nullifies all relation fields, as relation
    is populated in ManyToMany field, you should not use the standard model methods like
    `save()` and `update()` before re-loading the field from database.

If you want to modify the through field in place remember to reload it from database.
Otherwise you will set relations to None so effectively make the field useless!

```python
# always reload the field before modification
await post2.categories[0].postcategory.load()
# only then update the field
await post2.categories[0].postcategory.update(sort_order=3)
```
Note that reloading the model effectively reloads the relations as `pk_only` models 
(only primary key is set) so they are not fully populated, but it's enough to preserve 
the relation on update.

!!!warning
    If you use i.e. `fastapi` the partially loaded related models on through field might cause
    `pydantic` validation errors (that's the primary reason why they are not populated by default).
    So either you need to exclude the related fields in your response, or fully load the related
    models. In example above it would mean:
    ```python
    await post2.categories[0].postcategory.post.load()
    await post2.categories[0].postcategory.category.load()
    ```
    Alternatively you can use `load_all()`:
    ```python
    await post2.categories[0].postcategory.load_all()
    ```

**Preferred way of update is through queryset proxy `update()` method**

```python
# filter the desired related model with through field and update only through field params
await post2.categories.filter(name='Test category').update(postcategory={"sort_order": 3})
```


## Relation methods

### add

`add(item: Model, **kwargs)`

Allows you to add model to ManyToMany relation. 

```python
# Add a category to a post.
await post.categories.add(news)
# or from the other end:
await news.posts.add(post)
```

!!!warning
    In all not `None` cases the primary key value for related model **has to exist in database**.
    
    Otherwise an IntegrityError will be raised by your database driver library.

If you declare your models with a Through model with additional fields, you can populate them
during adding child model to relation.

In order to do so, pass keyword arguments with field names and values to `add()` call.

Note that this works only for `ManyToMany` relations.

```python
post = await Post(title="Test post").save()
category = await Category(name="Test category").save()
# apart from model pass arguments referencing through model fields
await post.categories.add(category, sort_order=1, param_name='test')
```

!!!note
    Adding a child that is already linked does not create a second link - if you pass through
    model fields, the existing through model is updated with them, otherwise the call does nothing.

### remove

Removal of the related model one by one.

Removes also the relation in the database.

```python
await news.posts.remove(post)
```

### clear

Removal of all related models in one call.

Removes also the relation in the database.

```python
await news.posts.clear()
```

### QuerysetProxy

Reverse relation exposes QuerysetProxy API that allows you to query related model like you would issue a normal Query.

To read which methods of QuerySet are available read below [querysetproxy][querysetproxy]


[queries]: ./queries.md
[querysetproxy]: ./queryset-proxy.md
[get]: ./queries.md#get
[all]: ./queries.md#all
[create]: ./queries.md#create
[get_or_create]: ./queries.md#get_or_create
[update_or_create]: ./queries.md#update_or_create
[filter]: ./queries.md#filter
[exclude]: ./queries.md#exclude
[select_related]: ./queries.md#select_related
[prefetch_related]: ./queries.md#prefetch_related
[limit]: ./queries.md#limit
[offset]: ./queries.md#offset
[count]: ./queries.md#count
[exists]: ./queries.md#exists
[fields]: ./queries.md#fields
[exclude_fields]: ./queries.md#exclude_fields
[order_by]: ./queries.md#order_by
