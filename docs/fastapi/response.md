# Response

You can use ormar Models in `fastapi` response_model instead of pydantic models.

You can of course also mix `ormar.Model`s with `pydantic` ones if you need to.

One of the most common tasks in responses is excluding certain fields that you do not want to include in response data.

This can be achieved in several ways in `ormar` so below you can review your options and select the one most suitable for your situation.

## Excluding fields in response

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

```python
# note that app is an FastApi app
@app.post("/users/", response_model=User) # here we use ormar.Model in response
async def create_user(user: User):  # here we use ormar.Model in request parameter
    return await user.save()
```

That means that if you do not pass i.e. `first_name` in request it will validate correctly (as field is optional), save in the database and return the saved record without this field (which will also pass validation).

!!!Note
    Note that although you do not pass the **field value**, the **field itself** is still present in the `response_model` that means it **will be present in response data** and set to `None`.
        
    If you want to fully exclude the field from the result read on.

### FastApi `response_model_exclude`

Fastapi has `response_model_exclude` that accepts a set (or a list) of field names.

That has it's limitation as `ormar` and `pydantic` accepts also dictionaries in which you can set exclude/include columns also on nested models (more on this below)

!!!Warning
    Note that you cannot exclude required fields when using `response_model` as it will fail during validation.

```python
@app.post("/users/", response_model=User, response_model_exclude={"password"})
async def create_user(user: User):
    return await user.save()
```

Above endpoint can be queried like this:

```python
from starlette.testclient import TestClient

client = TestClient(app)

with client as client:
        # note there is no pk
        user = {
            "email": "test@domain.com",
            "password": "^*^%A*DA*IAAA",
            "first_name": "John",
            "last_name": "Doe",
        }
        response = client.post("/users/", json=user)
        # note that the excluded field is fully gone from response
        assert "password" not in response.json()
        # read the response and initialize model out of it
        created_user = User(**response.json())
        # note pk is populated by autoincrement
        assert created_user.pk is not None
        # note that password is missing in initialized model too
        assert created_user.password is None
```

!!!Note
    Note how in above example `password` field is fully gone from the response data. 
        
    Note that you can use this method only for non-required fields.

#### Nested models excludes

Despite the fact that `fastapi` allows passing only set of field names, so simple excludes, when using `response_model_exclude`, ormar is smarter.

In `ormar` you can exclude nested models using two types of notations.

One is a dictionary with nested fields that represents the model tree structure, and the second one is double underscore separated path of field names.

Assume for a second that our user's category is a separate model:

```python
base_ormar_config = ormar.OrmarConfig(
    metadata=metadata
    database=database
)

class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")
    
    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=255)    
    priority: int = ormar.Integer(nullable=True)


class User(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    email: str = ormar.String(max_length=255)
    password: str = ormar.String(max_length=255)
    first_name: str = ormar.String(max_length=255, nullable=True)
    last_name: str = ormar.String(max_length=255)
    category: Optional[Category] = ormar.ForeignKey(Category, related_name="categories")
```

If you want to exclude `priority` from category in your response, you can still use fastapi parameter.
```python
@app.post("/users/", response_model=User, response_model_exclude={"category__priority"})
async def create_user(user: User):
    return await user.save()
```

Note that you can go in deeper models with double underscore, and if you want to exclude multiple fields from nested model you need to prefix them with full path.
In example `response_model_exclude={"category__priority", "category__other_field", category__nested_model__nested_model_field}` etc.

!!!Note
    To read more about possible excludes and how to structure your exclude dictionary or set visit [fields](../queries/select-columns.md#fields) section of documentation

!!!Note
    Note that apart from `response_model_exclude` parameter `fastapi` supports also other parameters inherited from `pydantic`.
    All of them works also with ormar, but can have some nuances so best to read [dict](../models/methods.md#dict) part of the documentation.

### Exclude in `Model.model_dump()`

Alternatively you can just return a dict from `ormar.Model` and use . 

Like this you can also set exclude/include as dict and exclude fields on nested models too.

!!!Warning
    Not using a `response_model` will cause api documentation having no response example and schema since in theory response can have any format.

```python
@app.post("/users2/", response_model=User)
async def create_user2(user: User):
    user = await user.save()
    return user.model_dump(exclude={'password'})
    # could be also something like return user.model_dump(exclude={'category': {'priority'}}) to exclude category priority
```

!!!Note
    Note that above example will nullify the password field even if you pass it in request, but the **field will be still there** as it's part of the response schema, the value will be set to `None`.

If you want to fully exclude the field with this approach simply don't use `response_model` and exclude in Model's model_dump()

Alternatively you can just return a dict from ormar model. 
Like this you can also set exclude/include as dict and exclude fields on nested models.

!!!Note
    In theory you loose validation of response here but since you operate on `ormar.Models` the response data have already been validated after db query (as ormar model is pydantic model).

So if you skip `response_model` altogether you can do something like this:

```python
@app.post("/users4/") # note no response_model
async def create_user4(user: User):
    user = await user.save()
    return user.model_dump(exclude={'last_name'})
```

!!!Note
    Note that when you skip the response_model you can now **exclude also required fields** as the response is no longer validated after being returned.
        
    The cost of this solution is that you loose also api documentation as response schema in unknown from fastapi perspective. 

### Generate `pydantic` model from `ormar.Model`

Since task of excluding fields is so common `ormar` has a special way to generate `pydantic` models from existing `ormar.Models` without you needing to retype all the fields. 

That method is `get_pydantic()` method available on all models classes.

```python
# generate a tree of models without password on User and without priority on nested Category
ResponseUser = User.get_pydantic(exclude={"password": ..., "category": {"priority"}})
@app.post("/users3/", response_model=ResponseUser) # use the generated model here
async def create_user3(user: User):
    return await user.save()
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

### Separate `pydantic` model

The final solution is to just create separate pydantic model manually. 
That works exactly the same as with normal fastapi application so you can have different models for response and requests etc.

Sample:
```python
import pydantic

class UserBase(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(from_attributes=True)

    email: str
    first_name: str
    last_name: str


@app.post("/users3/", response_model=UserBase) # use pydantic model here
async def create_user3(user: User): #use ormar model here (but of course you CAN use pydantic also here)
    return await user.save()
```
