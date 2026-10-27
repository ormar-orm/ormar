# Request

You can use ormar Models in `fastapi` request `Body` parameters instead of pydantic models.

You can of course also mix `ormar.Model`s with `pydantic` ones if you need to.

One of the most common tasks in requests is excluding certain fields that you do not want to include in the payload you send to API.

This can be achieved in several ways in `ormar` so below you can review your options and select the one most suitable for your situation.

## Excluding fields in request

### Optional fields

Note that each field that is optional is not required, that means that Optional fields can be skipped both in response and in requests.

Field is not required if (any/many/all) of following:

* Field is marked with `nullable=True`
* Field has `default` value or function provided, i.e. `default="Test"`
* Field has a `server_default` value set
* Field is an `autoincrement=True` `primary_key` field (note that `ormar.Integer` `primary_key` is `autoincrement` by default)

Example:
```python
base_ormar_config = ormar.OrmarConfig(
    metadata=metadata
    database=database
)

class User(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    email: str = ormar.String(max_length=255)
    password: str = ormar.String(max_length=255)
    first_name: str = ormar.String(max_length=255, nullable=True)
    last_name: str = ormar.String(max_length=255)
    category: str = ormar.String(max_length=255, default="User")
```

In above example fields `id` (is an `autoincrement` `Integer`), `first_name` ( has `nullable=True`) and `category` (has `default`) are optional and can be skipped in response and model will still validate.

If the field is nullable you don't have to include it in payload during creation as well as in response, so given example above you can:

!!!Warning
    Note that although you do not have to pass the optional field, you still **can** do it.
    And if someone will pass a value it will be used later unless you take measures to prevent it.

```python
# note that app is an FastApi app
@app.post("/users/", response_model=User) # here we use ormar.Model in response
async def create_user(user: User):  # here we use ormar.Model in request parameter
    return await user.save()
```

That means that if you do not pass i.e. `first_name` in request it will validate correctly (as field is optional), `None` will be saved in the database.

### Generate `pydantic` model from `ormar.Model`

Since task of excluding fields is so common `ormar` has a special way to generate `pydantic` models from existing `ormar.Models` without you needing to retype all the fields. 

That method is `get_pydantic()` method available on all models classes.

```python
# generate a tree of models without password on User and without priority on nested Category
RequestUser = User.get_pydantic(exclude={"password": ..., "category": {"priority"}})
@app.post("/users3/", response_model=User) # here you can also use both ormar/pydantic
async def create_user3(user: RequestUser):  # use the generated model here
    # note how now user is pydantic and not ormar Model so you need to convert
    return await User(**user.model_dump()).save()
```

!!!Note
    To see more examples and read more visit [get_pydantic](../models/methods.md#get_pydantic) part of the documentation.

If you prefer to send and receive related models as their primary keys instead of nested objects, pass them in the `flatten_fields` parameter (or use `flatten_all=True`).

```python
ItemSchema = Item.get_pydantic(flatten_fields="category")
# request: {"name": "item", "category": 1}, response: {"id": 1, "name": "item", "category": 1}
```

To read more visit [relations as primary keys](../models/methods.md#relations-as-primary-keys).

!!!Warning
    The `get_pydantic` method generates all models in a tree of nested models according to an algorithm that allows to avoid loops in models (same algorithm that is used in `model_dump()`, `select_all()` etc.)
        
    That means that nested models won't have reference to parent model (by default ormar relation is bidirectional).
        
    Note also that if given model exists in a tree more than once it will be doubled in pydantic models (each occurrence will have separate own model). That way you can exclude/include different fields on different leafs of the tree.

#### Mypy and type checking

Note that assigning a function as a python type passes at runtime (as it's not checked) the static type checkers like mypy will complain.

Although result of the function call will always be the same for given model using a dynamically created type is not allowed.

Therefore you have two options:

First one is to simply add `# type: ignore` to skip the type checking

```python
RequestUser = User.get_pydantic(exclude={"password": ..., "category": {"priority"}})
@app.post("/users3/", response_model=User)
async def create_user3(user: RequestUser):  # type: ignore
    # note how now user is not ormar Model so you need to convert
    return await User(**user.model_dump()).save()
```

The second one is a little bit more hacky and utilizes a way in which fastapi extract function parameters.

You can overwrite the `__annotations__` entry for given param.

```python
RequestUser = User.get_pydantic(exclude={"password": ..., "category": {"priority"}})
# do not use the app decorator
async def create_user3(user: User):  # use ormar model here
    return await User(**user.model_dump()).save()
# overwrite the function annotations entry for user param with generated model 
create_user3.__annotations__["user"] = RequestUser
# manually call app functions (app.get, app.post etc.) and pass your function reference
app.post("/categories/", response_model=User)(create_user3)
```

Note that this will cause mypy to "think" that user is an ormar model but since in request it doesn't matter that much (you pass jsonized dict anyway and you need to convert before saving).

That still should work fine as generated model will be a subset of fields, so all needed fields will validate, and all not used fields will fail at runtime.

### Separate `pydantic` model

The final solution is to just create separate pydantic model manually. 
That works exactly the same as with normal fastapi application, so you can have different models for response and requests etc.

Sample:
```python
import pydantic

class UserCreate(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(from_attributes=True)

    email: str
    first_name: str
    last_name: str
    password: str


@app.post("/users3/", response_model=User) # use ormar model here (but of course you CAN use pydantic also here)
async def create_user3(user: UserCreate):  # use pydantic model here
    # note how now request param is a pydantic model and not the ormar one
    # so you need to parse/convert it to ormar before you can use database
    return await User(**user.model_dump()).save()
```
