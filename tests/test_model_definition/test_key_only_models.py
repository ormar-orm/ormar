from typing import Optional

import pytest

import ormar
from ormar.exceptions import RelationshipInstanceError
from ormar.fields.key_only import key_only_instance, key_only_model_type
from tests.settings import create_config

base_ormar_config = create_config()


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ko_albums")

    id: int = ormar.Integer(primary_key=True, name="album_pk")
    name: str = ormar.String(max_length=50)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ko_tracks")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=50)
    album: Optional[Album] = ormar.ForeignKey(Album, name="album_ref")
    named_album: Optional[Album] = ormar.ForeignKey(
        Album,
        name="named_ref",
        related_name="named_tracks",
        foreign_key_name="fk_custom_named",
        ondelete="CASCADE",
    )


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ko_categories")

    id: int = ormar.Integer(primary_key=True, name="category_pk")


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ko_posts")

    id: int = ormar.Integer(primary_key=True)
    categories = ormar.ManyToMany(Category)


def foreign_keys(table) -> set:
    return {
        (column.name, fk.target_fullname, fk.name, fk.ondelete, fk.onupdate)
        for column in table.c
        for fk in column.foreign_keys
    }


def test_key_only_type_and_instance():
    model_type = key_only_model_type(target=Album, fields=("id",))
    assert set(model_type.model_fields) == {"id"}
    album = key_only_instance(target=Album, values={"id": 3})
    assert album.pk == 3
    assert album.__pk_only__ is True


def test_fk_ddl_unchanged():
    assert foreign_keys(Track.ormar_config.table) == {
        (
            "album_ref",
            "ko_albums.album_pk",
            "fk_ko_tracks_ko_albums_album_pk_album",
            None,
            None,
        ),
        ("named_ref", "ko_albums.album_pk", "fk_custom_named", "CASCADE", None),
    }


def test_m2m_through_fk_ddl_unchanged():
    through = Post.ormar_config.model_fields["categories"].through
    assert foreign_keys(through.ormar_config.table) == {
        (
            "post",
            "ko_posts.id",
            "fk_posts_categorys_ko_posts_post_id",
            "CASCADE",
            "CASCADE",
        ),
        (
            "category",
            "ko_categories.category_pk",
            "fk_posts_categorys_ko_categories_category_category_pk",
            "CASCADE",
            "CASCADE",
        ),
    }


def test_values_for_accepts_model_dict_and_raw():
    link = Track.ormar_config.model_fields["album"].link
    assert link.values_for(related=Album(id=2, name="a")) == {"album": 2}
    assert link.values_for(related={"id": 2}) == {"album": 2}
    assert link.values_for(related=2) == {"album": 2}
    assert link.values_for(related=None) == {"album": None}
    assert link.values_for(related=Album(name="no pk")) == {"album": None}


def test_construct_key_only_model_keeps_type_checks():
    field = Track.ormar_config.model_fields["album"]
    assert field.construct_key_only_model(value=5).pk == 5
    with pytest.raises(RelationshipInstanceError):
        field.construct_key_only_model(value="not an int")


def test_values_for_accepts_key_only_pydantic_model():
    link = Track.ormar_config.model_fields["album"].link
    key_only = Track.ormar_config.model_fields["album"].to_key_only(id=8)
    assert link.values_for(related=key_only) == {"album": 8}


def test_construct_key_only_model_keeps_zero_key():
    field = Track.ormar_config.model_fields["album"]
    assert field.construct_key_only_model(value=0).pk == 0
