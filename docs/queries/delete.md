# Delete data from database

Following methods allow you to delete data from the database.

* `delete(each: bool = False, **kwargs) -> int`


* `Model`
    * `Model.delete()` method


* `QuerysetProxy`
    * `QuerysetProxy.remove()` method
    * `QuerysetProxy.clear()` method

## delete

`delete(each: bool = False, **kwargs) -> int`

QuerySet level delete is used to delete multiple records at once.

You either have to filter the QuerySet first or provide a `each=True` flag to delete
whole table.

If you do not provide this flag or a filter a `QueryDefinitionError` will be raised.

Return number of rows deleted.

Filters may follow relations of any kind, the delete removes exactly the rows `all()`
of the same queryset returns:

```python
await Booking.objects.filter(training__instructor=instructor).delete()
```

```python hl_lines="44-48"
--8<-- "../docs_src/queries/docs005.py"
```

## Model methods

Each model instance have a set of methods to `save`, `update` or `load` itself.

### delete

You can delete model instance by calling `delete()` method on it.

!!!tip
    Read more about `delete()` method in [models methods](../models/methods.md#delete)

## QuerysetProxy methods

When access directly the related `ManyToMany` field as well as `ReverseForeignKey`
returns the list of related models.

But at the same time it exposes subset of QuerySet API, so you can filter, create,
select related etc related models directly from parent model.

### remove

Removal of the related model one by one.

Removes the relation in the database.

If you specify the keep_reversed flag to `False` `ormar` will also delete the related model from the database.

```python
class Album(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    is_best_seller: bool = ormar.Boolean(default=False)

class Track(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    title: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
    play_count: int = ormar.Integer(nullable=True)
```

```python
album = await Album(name="Malibu").save()
track1 = await Track(
    album=album, title="The Bird", position=1, play_count=30, 
).save()
# remove through proxy from reverse side of relation
await album.tracks.remove(track1, keep_reversed=False)

# the track was also deleted
tracks = await Track.objects.all()
assert len(tracks) == 0
```

### clear

Removal of all related models in one call.

Removes also the relation in the database.

If you specify the keep_reversed flag to `False` `ormar` will also delete the related model from the database.

```python
class Album(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    is_best_seller: bool = ormar.Boolean(default=False)

class Track(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    title: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
    play_count: int = ormar.Integer(nullable=True)
```

```python
album = await Album(name="Malibu").save()
track1 = await Track(
    album=album, 
    title="The Bird", 
    position=1, 
    play_count=30, 
).save()
track2 = await Track(
    album=album,
    title="Heart don't stand a chance",
    position=2,
    play_count=20,
).save()

# removes the relation only -> clears foreign keys on tracks
await album.tracks.clear()

# removes also the tracks
await album.tracks.clear(keep_reversed=False)
```

[querysetproxy]: ../relations/queryset-proxy.md
