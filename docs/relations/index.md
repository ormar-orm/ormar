# Relations

Currently `ormar` supports two types of relations:

* One-to-many (and many-to-one) with `ForeignKey` field
* One-to-one with `OneToOne` field (see [OneToOne](one-to-one.md))
* Many-to-many with `ManyToMany` field

Below you can find a very basic examples of definitions for each of those relations.

To read more about methods, possibilities, definition etc. please read the subsequent section of the documentation. 

## ForeignKey

To define many-to-one relation use `ForeignKey` field.

```Python hl_lines="27"
--8<-- "../docs_src/relations/docs003.py"
```

!!!tip
    To read more about one-to-many relations visit [foreign-keys][foreign-keys] section

## Reverse ForeignKey

The definition of one-to-many relation also uses `ForeignKey`, and it's registered for you automatically.

So in relation to example above.

```Python hl_lines="7-8"
class Department(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    # there is a virtual field here like follows
    courses: Optional[list[Course]] = ormar.ForeignKey(Course, virtual=True)
    # note that you DO NOT define it yourself, ormar does it for you.
```

!!!tip
    To read more about many-to-one relations (i.e changing the name of generated field) visit [foreign-keys][foreign-keys] section


!!!tip
    Reverse ForeignKey allows you to query the related models with [queryset-proxy][queryset-proxy].
    
    It allows you to use `await department.courses.all()` to fetch data related only to specific department etc. 

##ManyToMany

To define many-to-many relation use `ManyToMany` field.

```python hl_lines="19"
class Category(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="categories",
    )

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=40)

class Post(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
    )

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    categories: Optional[list[Category]] = ormar.ManyToMany(Category)
```


!!!tip
    To read more about many-to-many relations visit [many-to-many][many-to-many] section


!!!tip
    ManyToMany allows you to query the related models with [queryset-proxy][queryset-proxy].

    It allows you to use `await post.categories.all()` but also `await category.posts.all()` to fetch data related only to specific post, category etc.

## Through fields

As part of the `ManyToMany` relation you can define a through model, that can contain additional 
fields that you can use to filter, order etc. Fields defined like this are exposed on the reverse
side of the current query for m2m models. 

So if you query from model `A` to model `B`, only model `B` has through field exposed.
Which kind of make sense, since it's a one through model/field for each of related models.

```python hl_lines="12-21"
class Category(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="categories",
    )

    id = ormar.Integer(primary_key=True)
    name = ormar.String(max_length=40)

# you can specify additional fields on through model
class PostCategory(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
        tablename="posts_x_categories",
    )

    id: int = ormar.Integer(primary_key=True)
    sort_order: int = ormar.Integer(nullable=True)
    param_name: str = ormar.String(default="Name", max_length=200)


class Post(ormar.Model):
    ormar_config = ormar.OrmarConfig(
        database=database,
        metadata=metadata,
    )

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    categories = ormar.ManyToMany(Category, through=PostCategory)
```

!!!tip
    To read more about many-to-many relations and through fields visit [many-to-many][many-to-many] section


!!!tip
    ManyToMany allows you to query the related models with [queryset-proxy][queryset-proxy].
    
    It allows you to use `await post.categories.all()` but also `await category.posts.all()` to fetch data related only to specific post, category etc.

## Relationship default sort order

By default relations follow model default sort order so `primary_key` column ascending, or any sort order se in `ormar_config` object.

!!!tip
    To read more about models sort order visit [models](../models/index.md#model-sort-order) section of documentation

But you can modify the order in which related models are loaded during query by providing `orders_by` and `related_orders_by`
parameters to relations.

In relations you can sort only by directly related model columns or for `ManyToMany` 
columns also `Through` model columns `{through_field_name}__{column_name}`

Sample configuration might look like this:

```python hl_lines="23"
database = DatabaseConnection(DATABASE_URL)
metadata = sqlalchemy.MetaData()


base_ormar_config = ormar.OrmarConfig(
    database=database,
    metadata=metadata,
)


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Book(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    author: Optional[Author] = ormar.ForeignKey(
        Author, orders_by=["name"], related_orders_by=["-year"]
    )
    title: str = ormar.String(max_length=100)
    year: int = ormar.Integer(nullable=True)
    ranking: int = ormar.Integer(nullable=True)
```

Now calls:

`await Author.objects.select_related("books").get()` - the books will be sorted by the book year descending

`await Book.objects.select_related("author").all()` - the authors will be sorted by author name ascending

## Self-reference and postponed references

In order to create auto-relation or create two models that reference each other in at least two
different relations (remember the reverse side is auto-registered for you), you need to use
`ForwardRef` from `typing` module.

```python hl_lines="1 9 12"
PersonRef = ForwardRef("Person")


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    supervisor: PersonRef = ormar.ForeignKey(PersonRef, related_name="employees")


Person.update_forward_refs()
```

!!!tip
    To read more about self-reference and postponed relations visit [postponed-annotations][postponed-annotations] section


[foreign-keys]: ./foreign-key.md
[many-to-many]: ./many-to-many.md
[queryset-proxy]: ./queryset-proxy.md
[postponed-annotations]: ./postponed-annotations.md
