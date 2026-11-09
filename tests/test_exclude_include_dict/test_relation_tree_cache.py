import copy
from typing import ForwardRef, Optional

import pytest

import ormar
import ormar.models.mixins.relation_mixin as relation_mixin
from ormar.models.relation_graph import RelationGraph
from ormar.queryset.utils import translate_list_to_dict
from tests.settings import create_config

base_ormar_config = create_config()


class Country(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tree_cache_countries")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Author(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tree_cache_authors")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    country: Optional[Country] = ormar.ForeignKey(Country)


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tree_cache_categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tree_cache_posts")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    author: Optional[Author] = ormar.ForeignKey(Author)
    categories: Optional[list[Category]] = ormar.ManyToMany(Category)


class Comment(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tree_cache_comments")

    id: int = ormar.Integer(primary_key=True)
    text: str = ormar.String(max_length=100)
    post: Optional[Post] = ormar.ForeignKey(Post)
    author: Optional[Author] = ormar.ForeignKey(Author, related_name="own_comments")


MODELS = [Country, Author, Category, Post, Comment]

DUMP_OPTIONS = [
    {},
    {"exclude": {"author"}},
    {"exclude": {"post": {"categories"}}},
    {"exclude": {"post__author__country", "author__posts"}},
    {"include": {"id", "post"}},
    {"include": {"id": ..., "text": ..., "post": {"id": ..., "author": {"name"}}}},
    {"exclude_through_models": True},
    {"exclude_list": True},
    {"exclude_primary_keys": True, "exclude_none": True},
    {"mode": "json", "by_alias": True},
]


def fresh_tree(model: type[ormar.Model]) -> dict:
    model.__relation_map__ = None
    return translate_list_to_dict(list_to_trans=model._iterate_related_models())


def build_comment() -> Comment:
    country = Country(id=1, name="PL")
    author = Author(id=1, name="Ann", country=country)
    reader = Author(id=2, name="Bob", country=country)
    post = Post(
        id=1,
        title="Post",
        author=author,
        categories=[Category(id=1, name="news"), Category(id=2, name="tech")],
    )
    Post(id=2, title="Other", author=author)
    Comment(id=2, text="first", post=post, author=author)
    return Comment(id=1, text="second", post=post, author=reader)


@pytest.mark.parametrize("model", MODELS)
def test_cached_tree_matches_a_fresh_translation(model):
    assert model._relation_tree() == fresh_tree(model=model)
    assert model._relation_tree() is model._relation_tree()


@pytest.mark.parametrize("options", DUMP_OPTIONS)
def test_dump_with_cached_tree_is_identical_to_fresh_map(options):
    comment = build_comment()
    instances = [
        comment,
        comment.post,
        comment.post.author,
        comment.post.author.country,
        comment.post.categories[0],
    ]
    for instance in instances:
        model = type(instance)
        expected = instance.model_dump(relation_map=fresh_tree(model=model), **options)
        assert instance.model_dump(**options) == expected


def test_dumping_does_not_modify_the_cached_tree():
    comment = build_comment()
    trees = {model: copy.deepcopy(model._relation_tree()) for model in MODELS}
    for options in DUMP_OPTIONS:
        comment.model_dump(**options)
        comment.post.model_dump(**options)
        Comment.get_pydantic(exclude={"post__author"})
    for model, tree in trees.items():
        assert model._relation_tree() == tree


def test_tree_is_built_once_per_class_across_dumps(monkeypatch):
    calls = []
    original = relation_mixin.translate_list_to_dict

    def counting(*args, **kwargs):
        calls.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(relation_mixin, "translate_list_to_dict", counting)
    RelationGraph.changed()
    comment = build_comment()
    for _ in range(20):
        for options in DUMP_OPTIONS:
            comment.model_dump(**options)
            comment.post.model_dump(**options)
    assert len(calls) == 2


def test_tree_follows_a_reverse_relation_registered_later():
    author = Author(id=1, name="Ann")
    assert "reviews" not in Author._relation_tree()
    assert "reviews" not in Comment._relation_tree()["author"]
    assert "reviews" not in author.model_dump()

    class Review(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="tree_cache_reviews")

        id: int = ormar.Integer(primary_key=True)
        author: Optional[Author] = ormar.ForeignKey(Author)

    assert "reviews" in Author._relation_tree()
    assert "reviews" in Comment._relation_tree()["author"]
    assert "author__reviews" in Comment._iterate_related_models()
    later_author = Author(id=2, name="Bob")
    review = Review(id=1, author=later_author)
    assert later_author.model_dump()["reviews"] == [{"id": review.id}]
    for model in [*MODELS, Review]:
        assert model._relation_tree() == fresh_tree(model=model)


def test_tree_follows_a_resolved_forward_reference():
    LabelRef = ForwardRef("Label")

    class Tag(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="tree_cache_tags")

        id: int = ormar.Integer(primary_key=True)
        label: Optional[LabelRef] = ormar.ForeignKey(LabelRef)  # type: ignore

    class Label(ormar.Model):
        ormar_config = base_ormar_config.copy(tablename="tree_cache_labels")

        id: int = ormar.Integer(primary_key=True)

    assert Label._relation_tree() == {}
    Tag.update_forward_refs(Label=Label)
    assert Tag._relation_tree() == {"label": ...}
    assert Label._relation_tree() == {"tags": ...}
