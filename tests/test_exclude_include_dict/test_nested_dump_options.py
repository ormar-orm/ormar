import datetime
import json
from typing import Optional

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Cat(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="cats")

    id: int = ormar.Integer(primary_key=True)
    created: Optional[datetime.datetime] = ormar.DateTime(nullable=True)
    note: Optional[str] = ormar.String(max_length=5, nullable=True)
    label: str = ormar.String(max_length=5, default="d")


class Pet(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="pets")

    id: int = ormar.Integer(primary_key=True)
    name: Optional[str] = ormar.String(max_length=5, nullable=True)
    cat: Optional[Cat] = ormar.ForeignKey(Cat, nullable=True)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tags")

    id: int = ormar.Integer(primary_key=True)


class Link(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="links")

    id: int = ormar.Integer(primary_key=True)
    created: Optional[datetime.datetime] = ormar.DateTime(nullable=True)
    note: Optional[str] = ormar.String(max_length=5, nullable=True)
    label: str = ormar.String(max_length=5, default="d")


class Owner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="owners")

    id: int = ormar.Integer(primary_key=True)
    data: Optional[dict] = ormar.JSON(nullable=True)
    tags: list[Tag] = ormar.ManyToMany(Tag, through=Link)


class Shop(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shops")

    id: int = ormar.Integer(primary_key=True)
    owner: Optional[Owner] = ormar.ForeignKey(Owner, nullable=True)


create_test_database = init_tests(base_ormar_config)


def test_nested_model_respects_json_mode():
    pet = Pet(id=1, cat=Cat(id=2, created=datetime.datetime(2020, 1, 1)))
    dumped = pet.model_dump(mode="json")
    assert dumped["cat"]["created"] == "2020-01-01T00:00:00"
    json.dumps(dumped)


def test_nested_model_respects_exclude_none():
    pet = Pet(id=1, cat=Cat(id=2))
    assert pet.model_dump(exclude_none=True) == {
        "id": 1,
        "cat": {"id": 2, "label": "d"},
    }
    assert Pet(id=1).model_dump(exclude_none=True) == {"id": 1}
    assert Pet(id=1).model_dump() == {"id": 1, "name": None, "cat": None}


def test_nested_list_respects_exclude_none():
    cat = Cat(id=2, pets=[Pet(id=1)])
    assert cat.model_dump(exclude_none=True) == {
        "id": 2,
        "label": "d",
        "pets": [{"id": 1}],
    }


def test_nested_model_respects_exclude_unset():
    pet = Pet(id=1, cat=Cat(id=2, note="x"))
    assert pet.model_dump(exclude_unset=True)["cat"] == {
        "id": 2,
        "note": "x",
    }
    pet = Pet(id=1, cat=Cat(id=2))
    assert pet.model_dump(exclude_unset=True)["cat"] == {"id": 2}


def test_flattened_relation_respects_json_mode_and_exclude_none():
    pet = Pet(id=1, cat=Cat(id=2))
    assert pet.model_dump(mode="json", flatten_all=True) == {
        "id": 1,
        "name": None,
        "cat": 2,
    }
    assert Pet(id=1).model_dump(exclude_none=True, flatten_all=True) == {"id": 1}


def test_nested_model_respects_exclude_defaults():
    pet = Pet(id=1, cat=Cat(id=2, note="x", label="d"))
    assert pet.model_dump(exclude_defaults=True)["cat"] == {"id": 2, "note": "x"}


def test_flattened_list_respects_json_mode():
    cat = Cat(id=2, pets=[Pet(id=1, cat=None)])
    assert cat.model_dump(mode="json", flatten_all=True)["pets"] == [1]


def test_through_model_respects_dump_options():
    tag = Tag(id=1)
    owner = Owner(id=3, tags=[tag])
    tag.link = Link(id=5, created=datetime.datetime(2020, 1, 1))
    dumped = owner.model_dump(mode="json", exclude_none=True)
    json.dumps(dumped)
    assert dumped["tags"][0]["link"] == {
        "id": 5,
        "created": "2020-01-01T00:00:00",
        "label": "d",
    }


def test_null_relation_respects_exclude_unset():
    assert Pet(id=1).model_dump(exclude_unset=True) == {"id": 1}
    assert Pet(id=1, cat=None).model_dump(exclude_unset=True) == {
        "id": 1,
        "cat": None,
    }
    assert Pet(id=1).model_dump(exclude_unset=True, flatten_all=True) == {"id": 1}


def test_null_relation_respects_exclude_defaults():
    assert Pet(id=1, cat=None).model_dump(exclude_defaults=True) == {"id": 1}
    assert Pet(id=1, cat=Cat(id=2)).model_dump(exclude_defaults=True) == {
        "id": 1,
        "cat": {"id": 2},
    }


def test_nested_model_respects_exclude_list():
    tag = Tag(id=1)
    shop = Shop(id=1, owner=Owner(id=3, tags=[tag]))
    assert shop.model_dump()["owner"]["tags"] == [{"id": 1}]
    assert "tags" not in shop.model_dump(exclude_list=True)["owner"]


def test_nested_model_respects_round_trip():
    owner = Owner(id=3, data={"a": 1})
    shop = Shop(id=1, owner=owner)
    round_trip_data = shop.model_dump(round_trip=True)["owner"]["data"]
    assert round_trip_data == owner.model_dump(round_trip=True)["data"]
    assert round_trip_data != shop.model_dump()["owner"]["data"]
