# Signals

Signals are a mechanism to fire your piece of code (function / method) whenever given type of event happens in `ormar`.

To achieve this you need to register your receiver for a given type of signal for selected model(s).

## Defining receivers

Given a sample model like following:

```Python
import sqlalchemy

import ormar
from ormar import DatabaseConnection


base_ormar_config = ormar.OrmarConfig(
    database=DatabaseConnection("sqlite+aiosqlite:///db.sqlite"),
    metadata=sqlalchemy.MetaData(),
)


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    is_best_seller: bool = ormar.Boolean(default=False)
    play_count: int = ormar.Integer(default=0)
```

You can for example define a trigger that will set `album.is_best_seller` status if it will be played more than 50 times.

Import `pre_update` decorator, for list of currently available decorators/ signals check below.

```Python hl_lines="7"
--8<-- "../docs_src/signals/docs002.py"
```

Define your function. 

Note that each receiver function:

* has to be **callable**
* has to accept first **`sender`** argument that receives the class of sending object
* has to accept **`**kwargs`** argument as the parameters send in each `ormar.Signal` can change at any time so your function has to serve them.
* has to be **`async`** cause callbacks are gathered and awaited.

`pre_update` currently sends only one argument apart from `sender` and it's `instance` one.

Note how `pre_update` decorator accepts a `senders` argument that can be a single model or a list of models, 
for which you want to run the signal receiver. 

Currently there is no way to set signal for all models at once without explicitly passing them all into registration of receiver.

```Python hl_lines="31-34"
--8<-- "../docs_src/signals/docs002.py"
```

!!!note
    Note that receivers are defined on a class level -> so even if you connect/disconnect function through instance 
    it will run/ stop running for all operations on that `ormar.Model` class.

Note that our newly created function has instance and class of the instance so you can easily run database 
queries inside your receivers if you want to.

```Python hl_lines="44-51"
--8<-- "../docs_src/signals/docs002.py"
```

You can define same receiver for multiple models at once by passing a list of models to signal decorator.

```python
# define a dummy debug function
@pre_update([Album, Track])
async def before_update(sender, instance, **kwargs):
    print(f"{sender.get_name()}: {instance.model_dump_json()}: {kwargs}")
```

Of course, you can also create multiple functions for the same signal and model. Each of them will run at each signal.

```python
@pre_update(Album)
async def before_update(sender, instance, **kwargs):
    print(f"{sender.get_name()}: {instance.model_dump_json()}: {kwargs}")

@pre_update(Album)
async def before_update2(sender, instance, **kwargs):
    print(f'About to update {sender.get_name()} with pk: {instance.pk}')
```

Note that `ormar` decorators are the syntactic sugar, you can directly connect your function or method for given signal for
given model. Connect accept only one parameter - your `receiver` function / method.

```python hl_lines="11 13 16"
class AlbumAuditor:
    def __init__(self):
        self.event_type = "ALBUM_INSTANCE"

    async def before_save(self, sender, instance, **kwargs):
        await AuditLog(
            event_type=f"{self.event_type}_SAVE", event_log=instance.model_dump_json()
        ).save()

auditor = AlbumAuditor()
pre_save(Album)(auditor.before_save)
# call above has same result like the one below
Album.ormar_config.signals.pre_save.connect(auditor.before_save)
# signals are also exposed on instance
album = Album(name='Miami')
album.signals.pre_save.connect(auditor.before_save)
``` 

!!!warning
    Note that signals keep the reference to your receiver (not a `weakref`) so keep that in mind to avoid circular references.

## Disconnecting the receivers

To disconnect the receiver and stop it for running for given model you need to disconnect it.

```python hl_lines="7 10"

@pre_update(Album)
async def before_update(sender, instance, **kwargs):
    if instance.play_count > 50 and not instance.is_best_seller:
        instance.is_best_seller = True

# disconnect given function from signal for given Model
Album.ormar_config.signals.pre_save.disconnect(before_save)
# signals are also exposed on instance
album = Album(name='Miami')
album.signals.pre_save.disconnect(before_save)
``` 


## Available signals

!!!warning
    Note that signals are **not** send for:
    
    *  bulk operations (`QuerySet.bulk_create` and `QuerySet.bulk_update`) as they are designed for speed.
    
    *  queryset table level operations (`QuerySet.update` and `QuerySet.delete`) as they run on the underlying tables 
    (more like raw sql update/delete operations) and do not have specific instance.

### pre_save

`pre_save(sender: type["Model"], instance: "Model")`

Send for `Model.save()` and `Model.objects.create()` methods.

`sender` is a `ormar.Model` class and `instance` is the model to be saved.

### post_save

`post_save(sender: type["Model"], instance: "Model")`

Send for `Model.save()` and `Model.objects.create()` methods.

`sender` is a `ormar.Model` class and `instance` is the model that was saved.

Not sent when `create(on_conflict="ignore")` skipped a conflicting row (`pre_save` is still sent).

### pre_update

`pre_update(sender: type["Model"], instance: "Model")`

Send for `Model.update()` method.

`sender` is a `ormar.Model` class and `instance` is the model to be updated.

### post_update

`post_update(sender: type["Model"], instance: "Model")`

Send for `Model.update()` method.

`sender` is a `ormar.Model` class and `instance` is the model that was updated.

### pre_delete

`pre_delete(sender: type["Model"], instance: "Model")`

Send for `Model.save()` and `Model.objects.create()` methods.

`sender` is a `ormar.Model` class and `instance` is the model to be deleted.

### post_delete

`post_delete(sender: type["Model"], instance: "Model")`

Send for `Model.update()` method.

`sender` is a `ormar.Model` class and `instance` is the model that was deleted.

### pre_relation_add

`pre_relation_add(sender: type["Model"], instance: "Model", child: "Model", 
relation_name: str, passed_args: dict)`

Send for `Model.relation_name.add()` method for `ManyToMany` relations and reverse side of `ForeignKey` relation.

`sender` - sender class, `instance` - instance to which related model is added, `child` - model being added,
`relation_name` - name of the relation to which child is added, for add signals also `passed_kwargs` - dict of kwargs passed to `add()`

### post_relation_add

`post_relation_add(sender: type["Model"], instance: "Model", child: "Model", 
relation_name: str, passed_args: dict)`

Send for `Model.relation_name.add()` method for `ManyToMany` relations and reverse side of `ForeignKey` relation.

`sender` - sender class, `instance` - instance to which related model is added, `child` - model being added,
`relation_name` - name of the relation to which child is added, for add signals also `passed_kwargs` - dict of kwargs passed to `add()`

### pre_relation_remove

`pre_relation_remove(sender: type["Model"], instance: "Model", child: "Model", 
relation_name: str)`

Send for `Model.relation_name.remove()` method for `ManyToMany` relations and reverse side of `ForeignKey` relation.

`sender` - sender class, `instance` - instance to which related model is added, `child` - model being added,
`relation_name` - name of the relation to which child is added.

### post_relation_remove

`post_relation_remove(sender: type["Model"], instance: "Model", child: "Model", 
relation_name: str, passed_args: dict)`

Send for `Model.relation_name.remove()` method for `ManyToMany` relations and reverse side of `ForeignKey` relation.

`sender` - sender class, `instance` - instance to which related model is added, `child` - model being added,
`relation_name` - name of the relation to which child is added.

### post_bulk_update

`post_bulk_update(sender: type["Model"], instances: list["Model"], **kwargs)`, 
Send for `Model.objects.bulk_update(list[objects])` method.


## Defining your own signals

Note that you can create your own signals although you will have to send them manually in your code or subclass `ormar.Model`
and trigger your signals there.

Creating new signal is super easy. Following example will set a new signal with name your_custom_signal.

```python hl_lines="21"
import sqlalchemy

import ormar
from ormar import DatabaseConnection


base_ormar_config = ormar.OrmarConfig(
    database=DatabaseConnection("sqlite+aiosqlite:///db.sqlite"),
    metadata=sqlalchemy.MetaData(),
)


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    is_best_seller: bool = ormar.Boolean(default=False)
    play_count: int = ormar.Integer(default=0)

Album.ormar_config.signals.your_custom_signal = ormar.Signal()
Album.ormar_config.signals.your_custom_signal.connect(your_receiver_name)
```

Actually under the hood signal is a `SignalEmitter` instance that keeps a dictionary of know signals, and allows you
to access them as attributes. When you try to access a signal that does not exist `SignalEmitter` will create one for you.

So example above can be simplified to. The `Signal` will be created for you.

```
Album.ormar_config.signals.your_custom_signal.connect(your_receiver_name)
```

Now to trigger this signal you need to call send method of the Signal.

```python
await Album.ormar_config.signals.your_custom_signal.send(sender=Album)
```

Note that sender is the only required parameter and it should be ormar Model class.

Additional parameters have to be passed as keyword arguments.

```python
await Album.ormar_config.signals.your_custom_signal.send(sender=Album, my_param=True)
```

