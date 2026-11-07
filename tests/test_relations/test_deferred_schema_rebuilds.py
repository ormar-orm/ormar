import collections
import importlib
import sys
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import ForwardRef, Optional

import pydantic
import pytest
import sqlalchemy
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import ConfigDict, PydanticUserError

import ormar
from ormar.models.helpers import (
    building_schemas,
    mark_for_rebuild,
    rebuild_pending_models,
    schema_snapshot,
)
from tests.lifespan import init_tests, lifespan
from tests.settings import create_config

base_ormar_config = create_config()
app = FastAPI(lifespan=lifespan(base_ormar_config))


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lazy_categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lazy_items")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    category: Optional[Category] = ormar.ForeignKey(Category, related_name="items")


class Note(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lazy_notes")

    id: int = ormar.Integer(primary_key=True)
    text: str = ormar.String(max_length=100)
    item: Optional[Item] = ormar.ForeignKey(Item, related_name="notes")


create_test_database = init_tests(base_ormar_config)


@app.get("/items/{item_id}", response_model=Item)
async def get_item(item_id: int) -> Item:
    return await Item.objects.select_related(["category", "notes"]).get(id=item_id)


@app.get("/categories/{category_id}", response_model=Category)
async def get_category(category_id: int) -> Category:
    return await Category.objects.select_related("items").get(id=category_id)


def schema_properties(model: type[ormar.Model]) -> set[str]:
    """
    Returns names of the top level properties of the model json schema.
    """
    schema = model.model_json_schema()
    if "$ref" in schema:
        schema = schema["$defs"][schema["$ref"].split("/")[-1]]
    return set(schema["properties"])


@pytest.fixture
def rebuild_counter(monkeypatch: pytest.MonkeyPatch) -> collections.Counter:
    """
    Counts pydantic model_rebuild calls per model name.
    """
    counter: collections.Counter = collections.Counter()
    original = pydantic.BaseModel.model_rebuild.__func__  # type: ignore[attr-defined]

    def counting_rebuild(cls, **kwargs):  # type: ignore[no-untyped-def]
        counter[cls.__name__] += 1
        return original(cls, **kwargs)

    monkeypatch.setattr(
        pydantic.BaseModel, "model_rebuild", classmethod(counting_rebuild)
    )
    return counter


def declare_hub(relations: int, defer_build: bool = False) -> type[ormar.Model]:
    """
    Declares a hub model with the given number of models pointing at it,
    each with a reverse relation, and returns the hub.
    """
    config = base_ormar_config.copy(metadata=sqlalchemy.MetaData())
    model_config = ConfigDict(defer_build=defer_build)

    class Hub(ormar.Model):
        ormar_config = config.copy(tablename="hub")
        model_config = ConfigDict(defer_build=defer_build)

        id: int = ormar.Integer(primary_key=True)

    for index in range(relations):
        type(
            f"Spoke{index}",
            (ormar.Model,),
            {
                "__annotations__": {"id": int, "hub": Optional[Hub]},
                "ormar_config": config.copy(tablename=f"spoke{index}"),
                "model_config": model_config,
                "id": ormar.Integer(primary_key=True),
                "hub": ormar.ForeignKey(Hub, related_name=f"spokes{index}"),
            },
        )
    return Hub


@pytest.mark.parametrize("defer_build", [False, True])
def test_reverse_relations_do_not_rebuild_the_target_on_declaration(
    rebuild_counter: collections.Counter, defer_build: bool
) -> None:
    hub = declare_hub(relations=10, defer_build=defer_build)
    assert rebuild_counter["Hub"] == 0
    assert hub.__pydantic_complete__ is False

    dumped = hub.__pydantic_serializer__.to_python(hub(id=1))
    assert {f"spokes{index}" for index in range(10)} <= set(dumped)
    assert rebuild_counter["Hub"] == 1

    hub(id=2)
    hub.model_json_schema()
    assert rebuild_counter["Hub"] == 1
    assert all(count <= 1 for count in rebuild_counter.values())


def test_many_to_many_through_model_is_rebuilt_once(
    rebuild_counter: collections.Counter,
) -> None:
    config = base_ormar_config.copy(metadata=sqlalchemy.MetaData())

    class Student(ormar.Model):
        ormar_config = config.copy(tablename="students")

        id: int = ormar.Integer(primary_key=True)

    class Course(ormar.Model):
        ormar_config = config.copy(tablename="courses")

        id: int = ormar.Integer(primary_key=True)
        students = ormar.ManyToMany(Student, related_name="courses")

    through = Course.ormar_config.model_fields["students"].through
    assert rebuild_counter[through.__name__] == 0
    assert {"student", "course"} <= schema_properties(model=through)
    assert rebuild_counter[through.__name__] == 1
    assert "courses" in schema_properties(model=Student)


def test_model_json_schema_and_openapi_include_reverse_relations() -> None:
    assert {"id", "name", "category", "notes"} <= schema_properties(model=Item)
    schemas = app.openapi()["components"]["schemas"]
    assert {"id", "name", "items"} <= set(schemas["Category"]["properties"])
    assert {"id", "name", "category", "notes"} <= set(schemas["Item"]["properties"])


@pytest.mark.asyncio
async def test_fastapi_response_with_reverse_relation_declared_after_route_model():
    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://testserver")
    async with client as client, LifespanManager(app):
        category = await Category.objects.create(name="cat")
        item = await Item.objects.create(name="item", category=category)
        note = await Note.objects.create(text="note", item=item)

        response = await client.get(f"/items/{item.id}")
        assert response.status_code == 200
        data = response.json()
        assert data["category"]["items"] == [
            {
                "id": item.id,
                "name": "item",
                "category": {"id": category.id, "name": "cat"},
            }
        ]
        assert [note_data["id"] for note_data in data["notes"]] == [note.id]

        response = await client.get(f"/categories/{category.id}")
        assert response.json() == {
            "id": category.id,
            "name": "cat",
            "items": [
                {
                    "id": item.id,
                    "name": "item",
                    "category": {"id": category.id, "name": "cat"},
                }
            ],
        }


def test_pydantic_model_embedding_pending_model_gets_complete_schema() -> None:
    config = base_ormar_config.copy(metadata=sqlalchemy.MetaData())

    class Shelf(ormar.Model):
        ormar_config = config.copy(tablename="shelves")

        id: int = ormar.Integer(primary_key=True)

    class Book(ormar.Model):
        ormar_config = config.copy(tablename="books")

        id: int = ormar.Integer(primary_key=True)
        shelf: Optional[Shelf] = ormar.ForeignKey(Shelf, related_name="books")

    assert schema_snapshot(model=Shelf) is None

    class Library(pydantic.BaseModel):
        shelves: list[Shelf]

    assert Shelf.__pydantic_complete__ is True
    library = Library.model_validate({"shelves": [{"id": 1, "books": [{"id": 2}]}]})
    assert library.shelves[0].books[0].id == 2


def test_pending_model_waiting_for_forward_refs_stays_pending() -> None:
    config = base_ormar_config.copy(metadata=sqlalchemy.MetaData())

    class Person(ormar.Model):
        ormar_config = config.copy(tablename="people")

        id: int = ormar.Integer(primary_key=True)
        friend = ormar.ForeignKey(ForwardRef("Friend"), related_name="people")

    class Pet(ormar.Model):
        ormar_config = config.copy(tablename="pets")

        id: int = ormar.Integer(primary_key=True)
        owner: Optional[Person] = ormar.ForeignKey(Person, related_name="pets")

    with pytest.raises(PydanticUserError, match="update_forward_refs"):
        Person.model_json_schema()

    class Friend(ormar.Model):
        ormar_config = config.copy(tablename="friends")

        id: int = ormar.Integer(primary_key=True)

    Person.update_forward_refs(Friend=Friend)
    assert {"pets", "friend"} <= schema_properties(model=Person)
    friend = Friend(id=1, people=[{"id": 2}])
    assert friend.people[0].id == 2
    pet = Pet(id=1, owner={"id": 2})
    assert pet.owner is not None
    assert pet.owner.id == 2


def test_marking_twice_keeps_first_snapshot_and_rebuilds_inside_build() -> None:
    config = base_ormar_config.copy(metadata=sqlalchemy.MetaData())

    class Box(ormar.Model):
        ormar_config = config.copy(tablename="boxes")

        id: int = ormar.Integer(primary_key=True)

    Box.model_rebuild()
    mark_for_rebuild(model=Box)
    with building_schemas():
        snapshot = schema_snapshot(model=Box)
        assert snapshot is not None
        mark_for_rebuild(model=Box)
        assert schema_snapshot(model=Box) is snapshot
        rebuild_pending_models(model=Box)
        assert schema_snapshot(model=Box) is None
    assert Box.__pydantic_complete__ is True


@pytest.fixture
def module_dir(tmp_path: Path) -> Iterator[Path]:
    """
    Puts a temporary directory on sys.path and cleans the imported modules.
    """
    sys.path.insert(0, str(tmp_path))
    yield tmp_path
    sys.path.remove(str(tmp_path))
    for name in list(sys.modules):
        if name.startswith("lazy_pkg"):
            del sys.modules[name]


@pytest.mark.parametrize(
    "import_order",
    [
        ["owners", "pets", "api"],
        ["api", "pets", "owners"],
        ["pets", "api", "owners"],
    ],
)
def test_models_declared_across_modules_in_different_orders(
    module_dir: Path, import_order: list[str]
) -> None:
    package = module_dir / "lazy_pkg"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "base.py").write_text(
        textwrap.dedent(
            """
            import ormar
            import sqlalchemy

            config = ormar.OrmarConfig(
                metadata=sqlalchemy.MetaData(),
                database=ormar.DatabaseConnection("sqlite+aiosqlite:///lazy.db"),
            )
            """
        )
    )
    (package / "owners.py").write_text(
        textwrap.dedent(
            """
            import ormar
            from lazy_pkg.base import config

            class Owner(ormar.Model):
                ormar_config = config.copy(tablename="owners")

                id: int = ormar.Integer(primary_key=True)
                name: str = ormar.String(max_length=10)
            """
        )
    )
    (package / "pets.py").write_text(
        textwrap.dedent(
            """
            from typing import Optional

            import ormar
            from lazy_pkg.base import config
            from lazy_pkg.owners import Owner

            class Pet(ormar.Model):
                ormar_config = config.copy(tablename="pets")

                id: int = ormar.Integer(primary_key=True)
                owner: Optional[Owner] = ormar.ForeignKey(Owner, related_name="pets")

            class Toy(ormar.Model):
                ormar_config = config.copy(tablename="toys")

                id: int = ormar.Integer(primary_key=True)
                pet: Optional[Pet] = ormar.ForeignKey(Pet, related_name="toys")
            """
        )
    )
    (package / "api.py").write_text(
        textwrap.dedent(
            """
            import pydantic
            from lazy_pkg.owners import Owner
            from lazy_pkg.pets import Pet

            class OwnerList(pydantic.BaseModel):
                owners: list[Owner]
                pets: list[Pet]
            """
        )
    )
    for name in import_order:
        importlib.import_module(f"lazy_pkg.{name}")
    owners = sys.modules["lazy_pkg.owners"]
    api = sys.modules["lazy_pkg.api"]

    payload = {
        "owners": [{"id": 1, "name": "o", "pets": [{"id": 2, "toys": [{"id": 3}]}]}],
        "pets": [{"id": 2, "toys": [{"id": 3}]}],
    }
    result = api.OwnerList(**payload)
    assert result.owners[0].pets[0].id == 2
    assert result.pets[0].toys[0].id == 3
    assert owners.Owner(id=5, name="x", pets=[{"id": 6}]).pets[0].id == 6
    dumped = api.OwnerList.__pydantic_serializer__.to_python(result)
    assert dumped["owners"][0]["pets"][0]["id"] == 2
    assert dumped["pets"][0]["toys"][0]["id"] == 3
