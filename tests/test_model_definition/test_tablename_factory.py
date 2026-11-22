"""Tests for ``OrmarConfig.tablename_factory`` and ``ormar.snake_case_tablename``."""

from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config().copy(tablename_factory=ormar.snake_case_tablename)
default_ormar_config = create_config()


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class UserProfile(ormar.Model):
    ormar_config = base_ormar_config.copy(
        constraints=[ormar.UniqueColumns("nick")],
    )

    id: int = ormar.Integer(primary_key=True)
    nick: str = ormar.String(max_length=100)
    tags: Optional[list[Tag]] = ormar.ManyToMany(Tag)


class HTTPRequest(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="requests_log")

    id: int = ormar.Integer(primary_key=True)


class TaggedBase(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True)

    id: int = ormar.Integer(primary_key=True)
    tags: Optional[list[Tag]] = ormar.ManyToMany(Tag, related_name="tagged")


class BlogPost(TaggedBase):
    ormar_config = base_ormar_config.copy()

    title: str = ormar.String(max_length=100)


class ProxyProfile(UserProfile):
    ormar_config = base_ormar_config.copy(proxy=True)


class DefaultTag(ormar.Model):
    ormar_config = default_ormar_config.copy()

    id: int = ormar.Integer(primary_key=True)


class DefaultProfile(ormar.Model):
    ormar_config = default_ormar_config.copy(
        constraints=[ormar.UniqueColumns("nick")],
    )

    id: int = ormar.Integer(primary_key=True)
    nick: str = ormar.String(max_length=100)
    tags: Optional[list[DefaultTag]] = ormar.ManyToMany(DefaultTag)


class DefaultTaggedBase(ormar.Model):
    ormar_config = default_ormar_config.copy(abstract=True)

    id: int = ormar.Integer(primary_key=True)
    tags: Optional[list[DefaultTag]] = ormar.ManyToMany(
        DefaultTag, related_name="tagged"
    )


class DefaultPost(DefaultTaggedBase):
    ormar_config = default_ormar_config.copy()


create_test_database = init_tests(base_ormar_config)


@pytest.mark.parametrize(
    "class_name, expected",
    [
        ("Tag", "tags"),
        ("UserProfile", "user_profiles"),
        ("HTTPRequest", "http_requests"),
        ("UserHTTPRequest", "user_http_requests"),
        ("APIKey", "api_keys"),
        ("Model2Name", "model2_names"),
    ],
)
def test_snake_case_tablename(class_name: str, expected: str) -> None:
    assert ormar.snake_case_tablename(class_name) == expected


def test_factory_is_inherited_through_copy() -> None:
    assert base_ormar_config.tablename_factory is ormar.snake_case_tablename
    assert UserProfile.ormar_config.tablename_factory is ormar.snake_case_tablename


def test_plain_model_uses_factory() -> None:
    assert UserProfile.ormar_config.tablename == "user_profiles"
    assert UserProfile.ormar_config.table.name == "user_profiles"
    constraint_names = {
        constraint.name for constraint in UserProfile.ormar_config.table.constraints
    }
    assert "uc_user_profiles_nick" in constraint_names


def test_explicit_tablename_wins() -> None:
    assert HTTPRequest.ormar_config.tablename == "requests_log"


def test_default_through_table_uses_owner_factory() -> None:
    through = UserProfile.ormar_config.model_fields["tags"].through
    assert through.ormar_config.tablename == "user_profiles_tags"
    assert through.ormar_config.tablename_factory is ormar.snake_case_tablename


def test_through_copy_for_inherited_model_uses_factory() -> None:
    through = BlogPost.ormar_config.model_fields["tags"].through
    assert BlogPost.ormar_config.tablename == "blog_posts"
    assert through.ormar_config.tablename == "tagged_bases_tags_blog_posts"


def test_proxy_model_uses_parent_table() -> None:
    assert ProxyProfile.ormar_config.tablename == "user_profiles"


def test_default_naming_unchanged() -> None:
    assert DefaultProfile.ormar_config.tablename == "defaultprofiles"
    through = DefaultProfile.ormar_config.model_fields["tags"].through
    assert through.ormar_config.tablename == "defaultprofiles_defaulttags"
    constraint_names = {
        constraint.name for constraint in DefaultProfile.ormar_config.table.constraints
    }
    assert "uc_defaultprofiles_nick" in constraint_names
    inherited_through = DefaultPost.ormar_config.model_fields["tags"].through
    assert (
        inherited_through.ormar_config.tablename
        == "defaulttaggedbases_defaulttags_defaultposts"
    )


@pytest.mark.asyncio
async def test_models_with_factory_tables_round_trip() -> None:
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            tag = await Tag.objects.create(name="news")
            profile = await UserProfile.objects.create(nick="radek")
            await profile.tags.add(tag)
            post = await BlogPost.objects.create(title="hello")
            await post.tags.add(tag)

            loaded = await UserProfile.objects.select_related("tags").get(nick="radek")
            assert [loaded_tag.name for loaded_tag in loaded.tags] == ["news"]
            loaded_post = await BlogPost.objects.select_related("tags").get()
            assert [loaded_tag.name for loaded_tag in loaded_post.tags] == ["news"]
