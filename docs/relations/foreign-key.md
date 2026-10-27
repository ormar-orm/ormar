# ForeignKey

`ForeignKey(to: Model, *, name: str = None, unique: bool = False, nullable: bool = True,
related_name: str = None, virtual: bool = False, onupdate: Union[ReferentialAction, str] = None,
ondelete: Union[ReferentialAction, str] = None, foreign_key_name: str = None, **kwargs: Any)`
has required parameters `to` that takes target `Model` class.  

Sqlalchemy column and type are automatically taken from target `Model`.

* Sqlalchemy column: class of a target `Model` primary key column  
* Type (used for pydantic): type of a target `Model` 

## Defining Models

To define a relation add `ForeignKey` field that points to related `Model`.

```Python hl_lines="31"
--8<-- "../docs_src/fields/docs003.py"
```

## Reverse Relation

`ForeignKey` fields are automatically registering reverse side of the relation.

By default it's child (source) `Model` name + s, like courses in snippet below: 

```Python hl_lines="34 41"
--8<-- "../docs_src/fields/docs001.py"
```

Reverse relation exposes API to manage related objects also from parent side.

### Skipping reverse relation

If you are sure you don't want the reverse relation you can use `skip_reverse=True`
flag of the `ForeignKey`.

If you set `skip_reverse` flag internally the field is still registered on the other
side of the relationship so you can:

* `filter` by related models fields from reverse model
* `order_by` by related models fields from reverse model
  
But you cannot:

* Access the related field from reverse model with `related_name`
* Even if you `select_related` from reverse side of the model the returned models won't be populated in reversed instance (the join is not prevented so you still can `filter` and `order_by` over the relation)
* The relation won't be populated in `model_dump()` and `model_dump_json()`
* You cannot pass the nested related objects when populating from dictionary or json (also through `fastapi`). It will be either ignored or error will be raised depending on `extra` setting in pydantic `Config`.

Example:

```python
class Author(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    first_name: str = ormar.String(max_length=80)
    last_name: str = ormar.String(max_length=80)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=200)
    author: Optional[Author] = ormar.ForeignKey(Author, skip_reverse=True)

# create sample data
author = Author(first_name="Test", last_name="Author")
post = Post(title="Test Post", author=author)

assert post.author == author  # ok
assert author.posts  # Attribute error!

# but still can use in order_by
authors = (
    await Author.objects.select_related("posts").order_by("posts__title").all()
)
assert authors[0].first_name == "Test"

# note that posts are not populated for author even if explicitly
# included in select_related - note no posts in model_dump()
assert author.model_dump(exclude={"id"}) == {"first_name": "Test", "last_name": "Author"}

# still can filter through fields of related model
authors = await Author.objects.filter(posts__title="Test Post").all()
assert authors[0].first_name == "Test"
assert len(authors) == 1
```


### add

Adding child model from parent side causes adding related model to currently loaded parent relation, 
as well as sets child's model foreign key value and updates the model.

```python
department = await Department(name="Science").save()
course = Course(name="Math", completed=False) # note - not saved

await department.courses.add(course)
assert course.pk is not None # child model was saved
# relation on child model is set and FK column saved in db
assert course.department == department
# relation on parent model is also set
assert department.courses[0] == course 
```

!!!warning
    If you want to add child model on related model the primary key value for parent model **has to exist in database**.
    
    Otherwise ormar will raise `RelationshipInstanceError` as it cannot set child's ForeignKey column value 
    if parent model has no primary key value.
    
    That means that in example above the department has to be saved before you can call `department.courses.add()`.

!!!note
    If the child already has a primary key value (i.e. a `uuid` pk with a default or an explicitly passed id
    on a non-autoincrement pk), `ormar` checks if the row exists in the database - if not the child is inserted,
    otherwise it's updated.

!!!warning
    This method will not work on `ManyToMany` relations - there, both sides of the relation have to be saved before adding to relation.
    

### remove

Removal of the related model one by one.

In reverse relation calling `remove()` does not remove the child model, but instead nulls it ForeignKey value.

```python
# continuing from above
await department.courses.remove(course)
assert len(department.courses) == 0
# course still exists and was saved in remove
assert course.pk is not None
assert course.department is None

# to remove child from db
await course.delete()
```

But if you want to clear the relation and delete the child at the same time you can issue:

```python
# this will not only clear the relation 
# but also delete related course from db
await department.courses.remove(course, keep_reversed=False)
```

### clear

Removal of all related models in one call.

Like with remove, by default, `clear()` nulls the ForeigKey column on child model (all, not matter if they are loaded or not).

```python
# nulls department column on all courses related to this department
await department.courses.clear()
```

If you want to remove the children altogether from the database, set `keep_reversed=False`

```python
# deletes from db all courses related to this department 
await department.courses.clear(keep_reversed=False)
```

## QuerysetProxy

Reverse relation exposes QuerysetProxy API that allows you to query related model like you would issue a normal Query.

To read which methods of QuerySet are available read below [querysetproxy][querysetproxy]

## related_name

You can overwrite related model field name by providing `related_name` parameter like below:

```Python hl_lines="28-30 36"
--8<-- "../docs_src/fields/docs002.py"
```

!!!tip
    The reverse relation on access returns list of `wekref.proxy` to avoid circular references.

!!!warning
    When you provide multiple relations to the same model `ormar` can no longer auto generate
    the `related_name` for you. Therefore, in that situation you **have to** provide `related_name`
    for all but one (one can be default and generated) or all related fields.

## Referential Actions

When an object referenced by a ForeignKey is changed (deleted or updated),
ormar will set the SQL constraint specified by the `ondelete` and `onupdate` argument.

The possible values for `ondelete` and `onupdate` are found in `ormar.ReferentialAction`:

!!!note
    Instead of `ormar.ReferentialAction`, you can directly pass string values to these two arguments, but this is not recommended because it will break the integrity.

### CASCADE

Whenever rows in the parent (referenced) table are deleted (or updated), the respective rows of the child (referencing) table with a matching foreign key column will be deleted (or updated) as well. This is called a cascade delete (or update).

### RESTRICT

A value cannot be updated or deleted when a row exists in a referencing or child table that references the value in the referenced table.

Similarly, a row cannot be deleted as long as there is a reference to it from a referencing or child table.

### SET_NULL

Set the ForeignKey to `None`; this is only possible if `nullable` is True.

### SET_DEFAULT

Set the ForeignKey to its default value; a `server_default` for the ForeignKey must be set.

!!!note
      Note that the `default` value is not allowed and you must do this through `server_default`, which you can read about in [this section][server_default].

### DO_NOTHING

Take `NO ACTION`; NO ACTION and RESTRICT are very much alike. The main difference between NO ACTION and RESTRICT is that with NO ACTION the referential integrity check is done after trying to alter the table. RESTRICT does the check before trying to execute the UPDATE or DELETE statement. Both referential actions act the same if the referential integrity check fails: the UPDATE or DELETE statement will result in an error.

## Overriding the foreign key constraint name

By default ormar generates the foreign key constraint name as
`fk_{source_table}_{target_table}_{target_pk}_{field_name}`. On databases with a
short identifier length limit (for example MySQL's 64 character limit) the
auto-generated name can be truncated or rejected. Pass `foreign_key_name` to use
a custom name instead:

```python
class Book(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="books")

    id: int = ormar.Integer(primary_key=True)
    author = ormar.ForeignKey(
        Author,
        foreign_key_name="fk_books_author",
    )
```

When used on an abstract base class, each subclass suffixes the name with its
own tablename to avoid constraint name collisions across sibling tables.

## Referencing a unique column (`to_field`)

By default a foreign key stores the related model's primary key. Legacy schemas often
reference another unique column instead; pass its field name as `to_field`. The field
must be the primary key, have `unique=True`, or be the only column of a
`UniqueColumns` constraint.

```python
class Country(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="countries")

    id: int = ormar.Integer(primary_key=True)
    iso_code: str = ormar.String(max_length=2, unique=True)


class Address(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="addresses")

    id: int = ormar.Integer(primary_key=True)
    country: Optional[Country] = ormar.ForeignKey(
        Country, to_field="iso_code", name="country_iso"
    )


await Address.objects.create(country="PL")  # by the iso_code value
address = await Address.objects.get()
address.country.iso_code  # "PL" - not loaded yet, pk unknown
await address.country.load()  # loads the country by iso_code
```

## Relations without a database constraint (`db_constraint`)

`db_constraint=False` creates the column without a `FOREIGN KEY` constraint (for
tables in another database, partitioned tables or schemas that don't declare their
foreign keys). Joins, filters and prefetching work the same. `ondelete`/`onupdate`
need a constraint, so they can't be combined with it.

```python
class Event(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="events")

    id: int = ormar.Integer(primary_key=True)
    user: Optional[User] = ormar.ForeignKey(User, db_constraint=False)
```

## Foreign keys in the primary key

A foreign key can be (part of) the primary key: link tables keyed by their foreign
keys, or a table sharing its primary key with another one. The key value holds the
referenced values, not the related models.

```python
class Enrollment(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="student_course")

    student: Student = ormar.ForeignKey(Student, primary_key=True, name="student_id")
    course: Course = ormar.ForeignKey(Course, primary_key=True, name="course_id")
    grade: Optional[int] = ormar.Integer(nullable=True)


enrollment = await Enrollment.objects.create(student=ann, course=math)
enrollment.pk  # (ann.id, math.id)
await Enrollment.objects.get(pk=(ann.id, math.id))
await Enrollment.objects.get(student=ann, course=math)  # the same row
```

Foreign keys in the primary key are not nullable. They can point only at models with
a single column primary key (or a single `to_field`) for now.

## Foreign keys to several columns

A foreign key to a model with a composite primary key, or to a set of columns that is
unique together (`to_field=[...]`, matching a `UniqueColumns` constraint), is stored in
one column per referenced column, with one composite `FOREIGN KEY` constraint. The
columns are named `<field>_<referenced column>` unless `columns=` maps each referenced
column (by its database name) to a column name. `name=` can't be used for them.

```python
class Order(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="orders")

    tenant_id: int = ormar.Integer(primary_key=True)
    number: int = ormar.Integer(primary_key=True, name="order_no")


class OrderLine(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="order_lines")

    id: int = ormar.Integer(primary_key=True)
    order: Optional[Order] = ormar.ForeignKey(
        Order, columns={"tenant_id": "ord_tenant", "order_no": "ord_no"}
    )
    # FOREIGN KEY (ord_tenant, ord_no) REFERENCES orders (tenant_id, order_no)


await OrderLine.objects.create(order=order)  # a model,
await OrderLine.objects.create(order=(1, 7))  # its key tuple
await OrderLine.objects.create(order={"tenant_id": 1, "number": 7})  # or a dict

await OrderLine.objects.filter(order=order).all()
await OrderLine.objects.filter(order__in=[(1, 7), (1, 8)]).all()
await OrderLine.objects.filter(order__isnull=True).all()
await OrderLine.objects.filter(order__note="first").all()  # through the relation
await Order.objects.prefetch_related("lines").get(pk=(1, 7))

await OrderLine.objects.values(["id", "order"])
# [{"id": 1, "order": {"tenant_id": 1, "number": 7}}]
await OrderLine.objects.values_list(["id", "order"])  # [(1, (1, 7))]
line.model_dump(flatten_fields={"order"})["order"]  # {"tenant_id": 1, "number": 7}
```

Only `=`, `__in` and `__isnull` lookups work on the relation itself; filter other
conditions through the related fields. A foreign key to several columns can't be part
of a primary key yet.

## Relation Setup

You have several ways to set-up a relationship connection.

### `Model` instance

The most obvious one is to pass a related `Model` instance to the constructor.

```Python hl_lines="35-36"
--8<-- "../docs_src/relations/docs001.py"
```

### Primary key value

You can setup the relation also with just the pk column value of the related model.

```Python hl_lines="38-39"
--8<-- "../docs_src/relations/docs001.py"
```

### Dictionary

Next option is with a dictionary of key-values of the related model.

You can build the dictionary yourself or get it from existing model with `model_dump()` method.

```Python hl_lines="41-42"
--8<-- "../docs_src/relations/docs001.py"
```

### None

Finally you can explicitly set it to None (default behavior if no value passed).

```Python hl_lines="44-45"
--8<-- "../docs_src/relations/docs001.py"
```

You can also clear a `ForeignKey` relation on an already-loaded model by
assigning `None` — the field must be `nullable` for this to persist. The
related model is unregistered from the instance immediately, so subsequent
reads return `None` without requiring a reload:

```python
track = await Track.objects.select_related("album").get(id=track_id)
assert track.album is not None

track.album = None
assert track.album is None  # in-memory state cleared

await track.update(_columns=["album"])  # persists NULL to the database
```

!!!note
    Assigning `None` to a non-nullable `ForeignKey` is ignored at the
    in-memory level. Make the field `nullable=True` (the default) if you
    need to unassign it.

!!!warning
    In all not None cases the primary key value for related model **has to exist in database**.
    
    Otherwise an IntegrityError will be raised by your database driver library.

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
[server_default]: ../fields/common-parameters.md#server-default
