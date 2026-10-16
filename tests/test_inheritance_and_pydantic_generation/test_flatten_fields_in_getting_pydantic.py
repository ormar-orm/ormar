import uuid
from typing import Optional

import pydantic
import pytest

import ormar
from ormar.exceptions import QueryDefinitionError
from tests.settings import create_config

base_ormar_config = create_config()


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_pyd_categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_pyd_tags")

    id: uuid.UUID = ormar.UUID(primary_key=True, default=uuid.uuid4)
    name: str = ormar.String(max_length=100)


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_pyd_items")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    category: Optional[Category] = ormar.ForeignKey(Category, nullable=True)
    tags = ormar.ManyToMany(Tag)


class Position(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_pyd_positions")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    item: Item = ormar.ForeignKey(Item, nullable=False, skip_reverse=True)


class Order(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="flatten_pyd_orders")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    position: Optional[Position] = ormar.ForeignKey(Position, skip_reverse=True)


def build_item() -> Item:
    return Item(
        id=1,
        name="a",
        category=Category(id=3, name="cat"),
        tags=[Tag(name="first"), Tag(name="second")],
    )


def test_foreign_key_flattened_to_pk():
    PydanticItem = Item.get_pydantic(flatten_fields="category", exclude={"tags"})
    assert PydanticItem(id=1, name="a", category=3).category == 3
    assert PydanticItem(id=1, name="a", category={"id": 3}).category == 3
    assert PydanticItem(id=1, name="a", category=None).category is None
    assert PydanticItem(id=1, name="a").category is None
    category = Category(id=3, name="cat")
    assert PydanticItem(id=1, name="a", category=category).category == 3
    with pytest.raises(pydantic.ValidationError):
        PydanticItem(id=1, name="a", category={"name": "no pk"})


def test_schema_matches_flattened_model_dump():
    item = build_item()
    flatten_fields = {"category", "tags"}
    PydanticItem = Item.get_pydantic(flatten_fields=flatten_fields)
    expected = item.model_dump(flatten_fields=flatten_fields)
    assert PydanticItem.model_validate(expected).model_dump() == expected
    assert PydanticItem.model_validate(item.model_dump()).model_dump() == expected


def test_flatten_all_matches_model_dump():
    item = build_item()
    PydanticItem = Item.get_pydantic(flatten_all=True)
    expected = item.model_dump(flatten_all=True)
    assert PydanticItem.model_validate(item.model_dump()).model_dump() == expected


def test_many_to_many_flattened_to_list_of_pks():
    PydanticItem = Item.get_pydantic(flatten_fields=["tags"], exclude={"category"})
    tag_id = uuid.uuid4()
    validated = PydanticItem(id=1, name="a", tags=[str(tag_id), {"id": tag_id}])
    assert validated.tags == [tag_id, tag_id]


def test_reverse_relation_flattened_to_list_of_pks():
    PydanticCategory = Category.get_pydantic(flatten_fields="items")
    validated = PydanticCategory(id=1, name="cat", items=[1, {"id": 2}])
    assert validated.items == [1, 2]


def test_nested_flatten_fields():
    PydanticOrder = Order.get_pydantic(flatten_fields="position__item")
    PydanticPosition = PydanticOrder.model_fields["position"].annotation.__args__[0]
    assert set(PydanticPosition.model_fields) == {"id", "name", "item"}
    validated = PydanticOrder(
        id=1, name="order", position={"id": 2, "name": "pos", "item": {"id": 5}}
    )
    assert validated.position.item == 5


def test_flatten_fields_dict_and_accessor_forms():
    flatten_fields = {"position": {"item": ...}}
    from_dict = Order.get_pydantic(flatten_fields=flatten_fields)
    from_accessor = Order.get_pydantic(flatten_fields=Order.position.item)
    assert from_dict is from_accessor
    assert flatten_fields == {"position": {"item": ...}}


def test_whole_relation_exclude_is_allowed():
    PydanticItem = Item.get_pydantic(flatten_fields="category", exclude={"category"})
    assert "category" not in PydanticItem.model_fields


@pytest.mark.parametrize(
    "kwargs",
    [
        {"flatten_fields": "name"},
        {"flatten_fields": "missing"},
        {"flatten_fields": ["category", "category__items"]},
        {"flatten_fields": "category", "include": {"category__name"}},
        {"flatten_fields": "category", "exclude": {"category": {"name"}}},
    ],
)
def test_invalid_flatten_fields_raise(kwargs):
    with pytest.raises(QueryDefinitionError):
        Item.get_pydantic(**kwargs)


def test_flatten_fields_are_part_of_cache_key():
    Full = Item.get_pydantic(exclude={"tags"})
    Flattened = Item.get_pydantic(exclude={"tags"}, flatten_fields="category")
    FlattenedAll = Item.get_pydantic(exclude={"tags"}, flatten_all=True)
    assert len({Full, Flattened, FlattenedAll}) == 3
    assert Flattened is Item.get_pydantic(exclude={"tags"}, flatten_fields="category")
