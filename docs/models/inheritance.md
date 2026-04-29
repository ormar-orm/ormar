# Inheritance

Out of various types of ORM models inheritance `ormar` currently supports three of them:

* **Mixins**
* **Concrete table inheritance** (with parents set to `abstract=True`)
* **Proxy models** (with children set to `proxy=True`)

## Types of inheritance

The short summary of different types of inheritance:

* **Mixins [SUPPORTED]** - don't subclass `ormar.Model`, just define fields that are
  later used on different models (like `created_date` and `updated_date` on each model),
  only actual models create tables, but those fields from mixins are added
* **Concrete table inheritance [SUPPORTED]** - means that parent is marked as abstract
  and each child has its own table with columns from a parent and own child columns, kind
  of similar to Mixins but parent also is a Model
* **Proxy models [SUPPORTED]** - means that only parent has an actual table,
  children just add methods, modify settings etc. and share the parent's table
* **Single table inheritance [NOT SUPPORTED]** - means that only one table is created
  with fields that are combination/sum of the parent and all children models but child
  models use only subset of column in db (all parent and own ones, skipping the other
  children ones)
* **Multi/ Joined table inheritance [NOT SUPPORTED]** - means that part of the columns
  is saved on parent model and part is saved on child model that are connected to each
  other by kind of one to one relation and under the hood you operate on two models at
  once

## Mixins

To use Mixins just define a class that is not inheriting from an `ormar.Model` but is
defining `ormar.Fields` as class variables.

```python
base_ormar_config = ormar.OrmarConfig(
    database=DatabaseConnection(DATABASE_URL),
    metadata=sqlalchemy.MetaData(),
)


# a mixin defines the fields but is a normal python class 
class AuditMixin:
    created_by: str = ormar.String(max_length=100)
    updated_by: str = ormar.String(max_length=100, default="Sam")


class DateFieldsMixins:
    created_date: datetime.datetime = ormar.DateTime(default=datetime.datetime.now)
    updated_date: datetime.datetime = ormar.DateTime(default=datetime.datetime.now)


# a models can inherit from one or more mixins
class Category(ormar.Model, DateFieldsMixins, AuditMixin):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50, unique=True, index=True)
    code: int = ormar.Integer()
```

!!!tip 
    Note that Mixins are **not** models, so you still need to inherit
    from `ormar.Model` as well as define `ormar_config` field in the **final** model.

A Category class above will have four additional fields: `created_date`, `updated_date`,
`created_by` and `updated_by`.

There will be only one table created for model `Category` (`categories`), with `Category` class fields
combined with all `Mixins` fields.

Note that `Mixin` in class name is optional but is a good python practice.

## Concrete table inheritance

In concept concrete table inheritance is very similar to Mixins, but uses
actual `ormar.Models` as base classes.

!!!warning 
    Note that base classes have `abstract=True` set in `ormar_config` object, if you try
    to inherit from non abstract marked class `ModelDefinitionError` will be raised.

Since this abstract Model will never be initialized you can skip `metadata`
and `database` in it's `ormar_config` definition.

But if you provide it - it will be inherited, that way you do not have to
provide `metadata` and `database` in the final/concrete class

Note that you can always overwrite it in child/concrete class if you need to.

More over at least one of the classes in inheritance chain have to provide both `database` and `metadata` -
otherwise an error will be raised.

```python
# note that base classes have abstract=True
# since this model will never be initialized you can skip metadata and database
class AuditModel(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True)

    created_by: str = ormar.String(max_length=100)
    updated_by: str = ormar.String(max_length=100, default="Sam")


# but if you provide it it will be inherited - DRY (Don't Repeat Yourself) in action
class DateFieldsModel(ormar.Model):
    ormar_config = base_ormar_config.copy(
        abstract=True,
        metadata=metadata,
        database=db,
    )

    created_date: datetime.datetime = ormar.DateTime(default=datetime.datetime.now)
    updated_date: datetime.datetime = ormar.DateTime(default=datetime.datetime.now)


# that way you do not have to provide metadata and database in concrete class
class Category(DateFieldsModel, AuditModel):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50, unique=True, index=True)
    code: int = ormar.Integer()

```

The list of inherited options/settings is as follows: `metadata`, `database`
and `constraints`.

Of course apart from that all fields from base classes are combined and created in the
concrete table of the final Model.

!!!tip
    Note how you don't have to provide `abstarct=False` in the final class - it's the default setting
    that is not inherited.

## Redefining fields in subclasses

Note that you can redefine previously created fields like in normal python class
inheritance.

Whenever you define a field with same name and new definition it will completely replace
the previously defined one.

```python hl_lines="28"
# base class
class DateFieldsModel(ormar.Model):
    ormar_config = OrmarConfig(
        abstract=True,
        metadata=metadata,
        database=db,
        # note that UniqueColumns need sqlalchemy db columns names not the ormar ones
        constraints=[ormar.UniqueColumns("creation_date", "modification_date")]
    )

    created_date: datetime.datetime = ormar.DateTime(
        default=datetime.datetime.now, name="creation_date"
    )
    updated_date: datetime.datetime = ormar.DateTime(
        default=datetime.datetime.now, name="modification_date"
    )


class RedefinedField(DateFieldsModel):
    ormar_config = OrmarConfig(
        tablename="redefines",
        metadata=metadata,
        database=db,
    )

    id: int = ormar.Integer(primary_key=True)
    # here the created_date is replaced by the String field
    created_date: str = ormar.String(max_length=200, name="creation_date")


# you can verify that the final field is correctly declared and created
changed_field = RedefinedField.ormar_config.model_fields["created_date"]
assert changed_field.default is None
assert changed_field.alias == "creation_date"
assert any(x.name == "creation_date" for x in RedefinedField.ormar_config.table.columns)
assert isinstance(
    RedefinedField.ormar_config.table.columns["creation_date"].type,
    sqlalchemy.sql.sqltypes.String,
)
```

!!!warning 
    If you declare `UniqueColumns` constraint with column names, the final model **has to have**
    a column with the same name declared. Otherwise, the `ModelDefinitionError` will be raised.

    So in example above if you do not provide `name` for `created_date` in `RedefinedField` model
    ormar will complain.
    
    `created_date: str = ormar.String(max_length=200) # exception`
    
    `created_date: str = ormar.String(max_length=200, name="creation_date2") # exception`

## Proxy models

A proxy model is a child class that **shares its parent's table** and only adds
methods, computed fields, or a custom `queryset_class`. Use it when you want
multiple Python views over the same underlying table, for example to attach
domain methods to a generic record type.

To declare a proxy model, set `proxy=True` on the child's `OrmarConfig`:

```python
base_ormar_config = ormar.OrmarConfig(
    database=DatabaseConnection(DATABASE_URL),
    metadata=sqlalchemy.MetaData(),
)


class Human(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="humans")

    id: int = ormar.Integer(primary_key=True)
    first_name: str = ormar.String(max_length=50)
    last_name: str = ormar.String(max_length=50)


class User(Human):
    ormar_config = base_ormar_config.copy(proxy=True)

    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"
```

Both `Human.objects.all()` and `User.objects.all()` read from the same `humans`
table; the only difference is that `User` rows are returned as `User` instances
and gain the `full_name()` method.

### Constraints

* The base class must be a non-abstract `ormar.Model`. Proxying an
  `abstract=True` parent raises `ModelDefinitionError` — use concrete
  inheritance for that.
* A proxy model **cannot** declare new ormar fields. Doing so raises
  `ModelDefinitionError` because the schema is fixed by the parent.
* `proxy=True` and `abstract=True` are mutually exclusive on the same class.
* Proxy chains (e.g. `Admin(User)` with `proxy=True` where `User` itself is a
  proxy of `Human`) are allowed and resolve back to the root concrete table.
* All concrete ormar bases of a proxy must share the same table.

### Behavior notes

* Each proxy class has its own `SignalEmitter`, so a `pre_save` registered on
  `User` does not fire when a `Human` instance is saved (and vice versa).
  Set `emit_parent_signals=True` on the proxy's `OrmarConfig` to also dispatch
  the parent's `pre_save` / `post_save` / `pre_update` / `post_update` /
  `pre_delete` / `post_delete` handlers (with `sender=parent_cls`) on every
  save / update / delete via the proxy. The flag is opt-in to preserve the
  contract of existing handlers that assume the instance is the parent type.
* Reverse relations defined on related models still point at the original
  parent class — proxying does not create new reverse accessors.
* The proxy's `queryset_class` and `extra` settings can be overridden via
  `OrmarConfig.copy(proxy=True, queryset_class=...)` independently of the
  parent.

## Relations in inheritance

You can declare relations in every step of inheritance, so both in parent and child
classes. 

When you define a relation on a child model level it's either overwriting the relation 
defined in parent model (if the same field name is used), or is accessible only to this 
child if you define a new relation.

When inheriting relations, you always need to be aware of `related_name` parameter, that
has to be unique across a related model, when you define multiple child classes that inherit the
same relation.

If you do not provide `related_name` parameter ormar calculates it for you. This works
with inheritance as all child models have to have different class names, which are used
to calculate the default `related_name` (class.name.lower()+'s').

But, if you provide a `related_name` this name cannot be reused in all child models as
they would overwrite each other on the related model side.

Therefore, you have two options:

* redefine relation field in child models and manually provide different `related_name`
  parameters
* let this for `ormar` to handle -> auto adjusted related_name are: original
  related_name + "_" + child model **table** name

That might sound complicated but let's look at the following example:

### ForeignKey relations

```python
# normal model used in relation
class Person(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


# parent model - needs to be abstract
class Car(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True)

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    owner: Person = ormar.ForeignKey(Person)
    # note that we refer to the Person model again so we **have to** provide related_name
    co_owner: Person = ormar.ForeignKey(Person, related_name="coowned")
    created_date: datetime.datetime = ormar.DateTime(default=datetime.datetime.now)


class Truck(Car):
    ormar_config = base_ormar_config.copy()

    max_capacity: int = ormar.Integer()


class Bus(Car):
    ormar_config = base_ormar_config.copy(tablename="buses")

    max_persons: int = ormar.Integer()
```

Now when you will inspect the fields on Person model you will get:

```python
Person.ormar_config.model_fields
"""
{'id': <class 'ormar.fields.model_fields.Integer'>, 
'name': <class 'ormar.fields.model_fields.String'>, 
'trucks': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'coowned_trucks': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'buss': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'coowned_buses': <class 'ormar.fields.foreign_key.ForeignKey'>}
"""
```

Note how you have `trucks` and `buss` fields that leads to Truck and Bus class that
this Person owns. There were no `related_name` parameter so default names were used.

At the same time the co-owned cars need to be referenced by `coowned_trucks`
and `coowned_buses`. Ormar appended `_trucks` and `_buses` suffixes taken from child
model table names.

Seems fine, but the default name for owned trucks is ok (`trucks`) but the `buss` is
ugly, so how can we change it?

The solution is pretty simple - just redefine the field in Bus class and provide
different `related_name` parameter.

```python
# rest of the above example remains the same
class Bus(Car):
    ormar_config = base_ormar_config.copy(tablename="buses")

    # new field that changes the related_name
    owner: Person = ormar.ForeignKey(Person, related_name="buses")
    max_persons: int = ormar.Integer()
```

Now the columns looks much better.

```python
Person.ormar_config.model_fields
"""
{'id': <class 'ormar.fields.model_fields.Integer'>, 
'name': <class 'ormar.fields.model_fields.String'>, 
'trucks': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'coowned_trucks': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'buses': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'coowned_buses': <class 'ormar.fields.foreign_key.ForeignKey'>}
"""
```

!!!note 
    You could also provide `related_name` for the `owner` field, that way the proper suffixes
    would be added.

    `owner: Person = ormar.ForeignKey(Person, related_name="owned")` 

    and model fields for Person owned cars would become `owned_trucks` and `owned_buses`.

### ManyToMany relations

Similarly, you can inherit from Models that have ManyToMany relations declared but
there is one, but substantial difference - the Through model. 

Since the Through model will be able to hold additional fields, and now it links only two Tables 
(`from` and `to` ones), each child that inherits the m2m relation field has to have separate
Through model. 

Of course, you can overwrite the relation in each Child model, but that requires additional
code and undermines the point of the whole inheritance. `Ormar` will handle this for you if
you agree with default naming convention, which you can always manually overwrite in 
children if needed.

Again, let's look at the example to easier grasp the concepts. 

We will modify the previous example described above to use m2m relation for co_owners.

```python
# person remain the same as above
class Person(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)

# new through model between Person and Car2
class PersonsCar(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cars_x_persons")

# note how co_owners is now ManyToMany relation
class Car2(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True)

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)
    # note the related_name - needs to be unique across Person
    # model, regardless of how many different models leads to Person
    owner: Person = ormar.ForeignKey(Person, related_name="owned")
    co_owners: list[Person] = ormar.ManyToMany(
        Person, through=PersonsCar, related_name="coowned"
    )
    created_date: datetime.datetime = ormar.DateTime(default=datetime.datetime.now)


# child models define only additional Fields
class Truck2(Car2):
    ormar_config = base_ormar_config.copy(tablename="trucks2")

    max_capacity: int = ormar.Integer()


class Bus2(Car2):
    ormar_config = base_ormar_config.copy(tablename="buses2")

    max_persons: int = ormar.Integer()
```

`Ormar` automatically modifies related_name of the fields to include the **table** name 
of the children models. The default name is original related_name + '_' + child table name.

That way for class Truck2 the relation defined in 
`owner: Person = ormar.ForeignKey(Person, related_name="owned")` becomes `owned_trucks2`

You can verify the names by inspecting the list of fields present on `Person` model.

```python
Person.ormar_config.model_fields
{
# note how all relation fields need to be unique on Person
# regardless if autogenerated or manually overwritten
'id': <class 'ormar.fields.model_fields.Integer'>, 
'name': <class 'ormar.fields.model_fields.String'>, 
# note that we expanded on previous example so all 'old' fields are here
'trucks': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'coowned_trucks': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'buses': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'coowned_buses': <class 'ormar.fields.foreign_key.ForeignKey'>, 
# newly defined related fields
'owned_trucks2': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'coowned_trucks2': <class 'abc.ManyToMany'>, 
'owned_buses2': <class 'ormar.fields.foreign_key.ForeignKey'>, 
'coowned_buses2': <class 'abc.ManyToMany'>
}
```

But that's not all. It's kind of internal to `ormar` but affects the data structure in the database,
so let's examine the through models for both `Bus2` and `Truck2` models.

```python
Bus2.ormar_config.model_fields['co_owners'].through
<class 'abc.PersonsCarBus2'>
Bus2.ormar_config.model_fields['co_owners'].through.ormar_config.tablename
'cars_x_persons_buses2'

Truck2.ormar_config.model_fields['co_owners'].through
<class 'abc.PersonsCarTruck2'>
Truck2.ormar_config.model_fields['co_owners'].through.ormar_config.tablename
'cars_x_persons_trucks2'
```

As you can see above `ormar` cloned the Through model for each of the Child classes and added
Child **class** name at the end, while changing the table names of the cloned fields
the name of the **table** from the child is used.

Note that original model is not only not used, the table for this model is removed from metadata:

```python
Bus2.ormar_config.metadata.tables.keys()
dict_keys(['test_date_models', 'categories', 'subjects', 'persons', 'trucks', 'buses', 
           'cars_x_persons_trucks2', 'trucks2', 'cars_x_persons_buses2', 'buses2'])
```

So be aware that if you introduce inheritance along the way and convert a model into 
abstract parent model you may lose your data on through table if not careful.

!!!note
    Note that original table name and model name of the Through model is never used.
    Only the cloned models tables are created and used.

!!!warning
    Note that each subclass of the Model that has `ManyToMany` relation defined generates
    a new `Through` model, meaning also **new database table**.

    That means that each time you define a Child model you need to either manually create
    the table in the database, or run a migration (with alembic).
