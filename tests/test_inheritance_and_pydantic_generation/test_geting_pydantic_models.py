from typing import ForwardRef, Optional

import pydantic
from pydantic import computed_field
from pydantic_core import PydanticUndefined

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class SelfRef(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="self_refs")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, default="selfref")
    parent = ormar.ForeignKey(ForwardRef("SelfRef"), related_name="children")


SelfRef.update_forward_refs()


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Item(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100, default="test")
    category: Optional[Category] = ormar.ForeignKey(Category, nullable=True)


class CommentTopic(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="comment_topics")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)

    @computed_field
    def upper_name(self) -> str:
        return self.name.upper()


class Comment(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="comments")

    id: int = ormar.Integer(primary_key=True)
    body: str = ormar.String(max_length=200, default="")
    topic: Optional[CommentTopic] = ormar.ForeignKey(CommentTopic, nullable=True)

    @computed_field
    def shouted_body(self) -> str:
        return self.body.upper()

    @computed_field
    def trimmed_body(self) -> str:
        return self.body.strip()


class MutualA(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="mutual_a")

    id: int = ormar.Integer(primary_key=True)
    mutual_b = ormar.ForeignKey(ForwardRef("MutualB"), related_name="mutuals_a")


class MutualB(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="mutual_b")

    id: int = ormar.Integer(primary_key=True)
    name = ormar.String(max_length=100, default="test")
    mutual_a = ormar.ForeignKey(MutualA, related_name="mutuals_b")


MutualA.update_forward_refs()


create_test_database = init_tests(base_ormar_config)


def test_getting_pydantic_model():
    PydanticCategory = Category.get_pydantic()
    assert issubclass(PydanticCategory, pydantic.BaseModel)
    assert {*PydanticCategory.model_fields.keys()} == {"items", "id", "name"}

    assert not PydanticCategory.model_fields["id"].is_required()
    assert (
        PydanticCategory.__pydantic_core_schema__["schema"]["fields"]["id"]["schema"][
            "schema"
        ]["schema"]["type"]
        == "int"
    )
    assert PydanticCategory.model_fields["id"].default is None

    assert PydanticCategory.model_fields["name"].is_required()
    assert (
        PydanticCategory.__pydantic_core_schema__["schema"]["fields"]["name"]["schema"][
            "type"
        ]
        == "str"
    )
    assert PydanticCategory.model_fields["name"].default == PydanticUndefined

    PydanticItem = PydanticCategory.__pydantic_core_schema__["schema"]["fields"][
        "items"
    ]["schema"]["schema"]["schema"]["items_schema"]["cls"]
    assert (
        PydanticCategory.__pydantic_core_schema__["schema"]["fields"]["items"][
            "schema"
        ]["schema"]["schema"]["type"]
        == "list"
    )
    assert (
        PydanticCategory.model_fields["items"].annotation
        == Optional[list[PydanticItem]]
    )
    assert issubclass(PydanticItem, pydantic.BaseModel)
    assert not PydanticItem.model_fields["name"].is_required()
    assert PydanticItem.model_fields["name"].default == "test"
    assert PydanticItem.model_fields["name"].annotation == Optional[str]
    assert "category" not in PydanticItem.model_fields


def test_initializing_pydantic_model():
    data = {
        "id": 1,
        "name": "test",
        "items": [{"id": 1, "name": "test_i1"}, {"id": 2, "name": "test_i2"}],
    }
    PydanticCategory = Category.get_pydantic()
    ormar_cat = Category(**data)
    assert ormar_cat.model_dump() == data
    cat = PydanticCategory(**data)
    assert cat.model_dump() == data

    data = {"id": 1, "name": "test"}
    cat = PydanticCategory(**data)
    assert cat.model_dump() == {**data, "items": None}


def test_getting_pydantic_model_include():
    PydanticCategory = Category.get_pydantic(include={"id", "name"})
    assert len(PydanticCategory.model_fields) == 2
    assert "items" not in PydanticCategory.model_fields


def test_getting_pydantic_model_nested_include_set():
    PydanticCategory = Category.get_pydantic(include={"id", "items__id"})
    assert len(PydanticCategory.model_fields) == 2
    assert "name" not in PydanticCategory.model_fields
    PydanticItem = PydanticCategory.__pydantic_core_schema__["schema"]["fields"][
        "items"
    ]["schema"]["schema"]["schema"]["items_schema"]["cls"]
    assert len(PydanticItem.model_fields) == 1
    assert "id" in PydanticItem.model_fields


def test_getting_pydantic_model_nested_include_dict():
    PydanticCategory = Category.get_pydantic(include={"id": ..., "items": {"id"}})
    assert len(PydanticCategory.model_fields) == 2
    assert "name" not in PydanticCategory.model_fields
    PydanticItem = PydanticCategory.__pydantic_core_schema__["schema"]["fields"][
        "items"
    ]["schema"]["schema"]["schema"]["items_schema"]["cls"]
    assert len(PydanticItem.model_fields) == 1
    assert "id" in PydanticItem.model_fields


def test_getting_pydantic_model_nested_include_nested_dict():
    PydanticCategory = Category.get_pydantic(include={"id": ..., "items": {"id": ...}})
    assert len(PydanticCategory.model_fields) == 2
    assert "name" not in PydanticCategory.model_fields
    PydanticItem = PydanticCategory.__pydantic_core_schema__["schema"]["fields"][
        "items"
    ]["schema"]["schema"]["schema"]["items_schema"]["cls"]
    assert len(PydanticItem.model_fields) == 1
    assert "id" in PydanticItem.model_fields


def test_getting_pydantic_model_include_exclude():
    PydanticCategory = Category.get_pydantic(
        include={"id": ..., "items": {"id", "name"}}, exclude={"items__name"}
    )
    assert len(PydanticCategory.model_fields) == 2
    assert "name" not in PydanticCategory.model_fields
    PydanticItem = PydanticCategory.__pydantic_core_schema__["schema"]["fields"][
        "items"
    ]["schema"]["schema"]["schema"]["items_schema"]["cls"]
    assert len(PydanticItem.model_fields) == 1
    assert "id" in PydanticItem.model_fields


def test_getting_pydantic_model_exclude():
    PydanticItem = Item.get_pydantic(exclude={"category__name"})
    assert len(PydanticItem.model_fields) == 3
    assert "category" in PydanticItem.model_fields
    PydanticCategory = PydanticItem.__pydantic_core_schema__["schema"]["fields"][
        "category"
    ]["schema"]["schema"]["schema"]["cls"]
    assert len(PydanticCategory.model_fields) == 1
    assert "name" not in PydanticCategory.model_fields


def test_getting_pydantic_model_exclude_dict():
    PydanticItem = Item.get_pydantic(exclude={"id": ..., "category": {"name"}})
    assert len(PydanticItem.model_fields) == 2
    assert "category" in PydanticItem.model_fields
    assert "id" not in PydanticItem.model_fields
    PydanticCategory = PydanticItem.__pydantic_core_schema__["schema"]["fields"][
        "category"
    ]["schema"]["schema"]["schema"]["cls"]
    assert len(PydanticCategory.model_fields) == 1
    assert "name" not in PydanticCategory.model_fields


def test_getting_pydantic_model_self_ref():
    PydanticSelfRef = SelfRef.get_pydantic()
    assert len(PydanticSelfRef.model_fields) == 4
    assert set(PydanticSelfRef.model_fields.keys()) == {
        "id",
        "name",
        "parent",
        "children",
    }
    inner_self_ref_id = PydanticSelfRef.__pydantic_core_schema__["schema"]["schema"][
        "fields"
    ]["parent"]["schema"]["schema"]["schema"]["schema_ref"]
    InnerSelf = next(
        (
            x
            for x in PydanticSelfRef.__pydantic_core_schema__["definitions"]
            if x["ref"] == inner_self_ref_id
        )
    )["cls"]
    assert len(InnerSelf.model_fields) == 2
    assert set(InnerSelf.model_fields.keys()) == {"id", "name"}

    inner_self_ref_id2 = PydanticSelfRef.__pydantic_core_schema__["schema"]["schema"][
        "fields"
    ]["children"]["schema"]["schema"]["schema"]["items_schema"]["schema_ref"]
    InnerSelf2 = next(
        (
            x
            for x in PydanticSelfRef.__pydantic_core_schema__["definitions"]
            if x["ref"] == inner_self_ref_id2
        )
    )["cls"]
    assert len(InnerSelf2.model_fields) == 2
    assert set(InnerSelf2.model_fields.keys()) == {"id", "name"}


def test_getting_pydantic_model_self_ref_exclude():
    PydanticSelfRef = SelfRef.get_pydantic(exclude={"children": {"name"}})
    assert len(PydanticSelfRef.model_fields) == 4
    assert set(PydanticSelfRef.model_fields.keys()) == {
        "id",
        "name",
        "parent",
        "children",
    }

    InnerSelf = PydanticSelfRef.__pydantic_core_schema__["schema"]["fields"]["parent"][
        "schema"
    ]["schema"]["schema"]["cls"]
    assert len(InnerSelf.model_fields) == 2
    assert set(InnerSelf.model_fields.keys()) == {"id", "name"}

    # PydanticSelfRefChildren = PydanticSelfRef.model_fields["children"].type_
    PydanticSelfRefChildren = PydanticSelfRef.__pydantic_core_schema__["schema"][
        "fields"
    ]["children"]["schema"]["schema"]["schema"]["items_schema"]["cls"]
    assert len(PydanticSelfRefChildren.model_fields) == 1
    assert set(PydanticSelfRefChildren.model_fields.keys()) == {"id"}
    assert PydanticSelfRef != PydanticSelfRefChildren
    assert InnerSelf != PydanticSelfRefChildren


def test_getting_pydantic_model_mutual_rels():
    MutualAPydantic = MutualA.get_pydantic()
    assert len(MutualAPydantic.model_fields) == 3
    assert set(MutualAPydantic.model_fields.keys()) == {"id", "mutual_b", "mutuals_b"}

    mutual_ref_1 = MutualAPydantic.__pydantic_core_schema__["schema"]["schema"][
        "fields"
    ]["mutual_b"]["schema"]["schema"]["schema"]["schema_ref"]
    MutualB1 = next(
        (
            x
            for x in MutualAPydantic.__pydantic_core_schema__["definitions"]
            if x["ref"] == mutual_ref_1
        )
    )["cls"]
    mutual_ref_2 = MutualAPydantic.__pydantic_core_schema__["schema"]["schema"][
        "fields"
    ]["mutuals_b"]["schema"]["schema"]["schema"]["items_schema"]["schema_ref"]
    MutualB2 = next(
        (
            x
            for x in MutualAPydantic.__pydantic_core_schema__["definitions"]
            if x["ref"] == mutual_ref_2
        )
    )["cls"]
    assert len(MutualB1.model_fields) == 2
    assert set(MutualB1.model_fields.keys()) == {"id", "name"}
    assert len(MutualB2.model_fields) == 2
    assert set(MutualB2.model_fields.keys()) == {"id", "name"}
    assert MutualB1 == MutualB2


def test_getting_pydantic_model_mutual_rels_exclude():
    MutualAPydantic = MutualA.get_pydantic(exclude={"mutual_b": {"name"}})
    assert len(MutualAPydantic.model_fields) == 3
    assert set(MutualAPydantic.model_fields.keys()) == {"id", "mutual_b", "mutuals_b"}

    MutualB1 = MutualAPydantic.__pydantic_core_schema__["schema"]["fields"]["mutual_b"][
        "schema"
    ]["schema"]["schema"]["cls"]
    MutualB2 = MutualAPydantic.__pydantic_core_schema__["schema"]["fields"][
        "mutuals_b"
    ]["schema"]["schema"]["schema"]["items_schema"]["cls"]

    assert len(MutualB1.model_fields) == 1
    assert set(MutualB1.model_fields.keys()) == {"id"}
    assert len(MutualB2.model_fields) == 2
    assert set(MutualB2.model_fields.keys()) == {"id", "name"}
    assert MutualB1 != MutualB2


def test_getting_pydantic_model_includes_computed_fields():
    PydanticComment = Comment.get_pydantic()
    assert set(PydanticComment.model_computed_fields.keys()) == {
        "shouted_body",
        "trimmed_body",
    }
    instance = PydanticComment(id=1, body=" hi ")
    dumped = instance.model_dump()
    assert dumped["shouted_body"] == " HI "
    assert dumped["trimmed_body"] == "hi"


def test_getting_pydantic_model_excludes_computed_field_by_name():
    PydanticComment = Comment.get_pydantic(exclude={"trimmed_body"})
    assert set(PydanticComment.model_computed_fields.keys()) == {"shouted_body"}
    instance = PydanticComment(id=1, body=" hi ")
    dumped = instance.model_dump()
    assert "trimmed_body" not in dumped
    assert dumped["shouted_body"] == " HI "


def test_getting_pydantic_model_include_filters_computed_fields():
    PydanticComment = Comment.get_pydantic(include={"id", "body", "shouted_body"})
    assert set(PydanticComment.model_fields.keys()) == {"id", "body"}
    assert set(PydanticComment.model_computed_fields.keys()) == {"shouted_body"}


def test_getting_pydantic_model_propagates_computed_fields_through_relations():
    PydanticComment = Comment.get_pydantic()
    InnerTopic = PydanticComment.__pydantic_core_schema__["schema"]["fields"]["topic"][
        "schema"
    ]["schema"]["schema"]["cls"]
    assert set(InnerTopic.model_computed_fields.keys()) == {"upper_name"}
    inner = InnerTopic(id=1, name="news")
    assert inner.model_dump()["upper_name"] == "NEWS"
