# OneToOne

`OneToOne` is a foreign key where each related model has at most one model pointing
back at it. It takes the same arguments as [`ForeignKey`](foreign-key.md) (`name`,
`related_name`, `to_field`, `db_constraint`, `ondelete`, `primary_key`, ...).

```python
class User(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="users")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Profile(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="profiles")

    id: int = ormar.Integer(primary_key=True)
    bio: Optional[str] = ormar.String(max_length=200, nullable=True)
    user: Optional[User] = ormar.OneToOne(User)
```

* The foreign key column is `UNIQUE`, so the database rejects a second profile for
  the same user. A `OneToOne` that is the primary key (the shared primary key
  pattern) is unique already:

    ```python
    class Passport(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="passports")

        user: User = ormar.OneToOne(User, primary_key=True, related_name="passport")
        number: str = ormar.String(max_length=20)
    ```

* The reverse side holds **one** model or `None`, not a list. Its default name is the
  model name without a trailing `s` (`user.profile`).

```python
user = await User.objects.select_related("profile").get(name="Ann")
user.profile  # Profile or None
await User.objects.prefetch_related("profile").all()
await User.objects.filter(profile__bio__icontains="python").all()
user.model_dump()  # {"id": 1, "name": "Ann", "profile": {"id": 1, "bio": ...}}
```

* The reverse side has no queryset proxy (there is nothing to filter); load it with
  `select_related` / `prefetch_related`.
* Assigning on the reverse side (`user.profile = profile`) links both models in memory;
  save the profile to store the link.
