from typing import ForwardRef, Optional

import ormar
from ormar.fields.link import ColumnPair, key_parts, key_value
from tests.settings import create_config

base_ormar_config = create_config()


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rl_albums")

    id: int = ormar.Integer(primary_key=True, name="album_pk")
    name: str = ormar.String(max_length=50)


class TrackBase(ormar.Model):
    ormar_config = base_ormar_config.copy(abstract=True)

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=50)
    album: Optional[Album] = ormar.ForeignKey(Album, name="album_ref")


class Track(TrackBase):
    ormar_config = base_ormar_config.copy(tablename="rl_tracks")


class SubTrack(TrackBase):
    ormar_config = base_ormar_config.copy(tablename="rl_subtracks")


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rl_categories")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=50)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rl_posts")

    id: int = ormar.Integer(primary_key=True)
    categories = ormar.ManyToMany(Category)


PersonRef = ForwardRef("Person")


class Person(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rl_people")

    id: int = ormar.Integer(primary_key=True)
    friends = ormar.ManyToMany(PersonRef, related_name="friend_of")


Person.update_forward_refs()

LateParentRef = ForwardRef("LateParent")


class LateChild(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rl_late_children")

    id: int = ormar.Integer(primary_key=True)
    parent: Optional[LateParentRef] = ormar.ForeignKey(LateParentRef)  # type: ignore


class LateParent(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="rl_late_parents")

    id: int = ormar.Integer(primary_key=True, name="late_pk")


LateChild.update_forward_refs()


def test_forward_fk_link():
    link = Track.ormar_config.model_fields["album"].link
    assert link.source is Track
    assert link.target is Album
    assert link.pairs == (
        ColumnPair(
            source_field="album",
            source_column="album_ref",
            target_field="id",
            target_column="album_pk",
        ),
    )


def test_reverse_fk_link_is_mirror():
    link = Album.ormar_config.model_fields["tracks"].link
    assert link == Track.ormar_config.model_fields["album"].link.mirrored()
    assert link.source is Album
    assert link.source_fields() == ("id",)
    assert link.source_columns() == ("album_pk",)
    assert link.target_fields() == ("album",)
    assert link.target_columns() == ("album_ref",)


def test_m2m_links_to_and_from_through():
    field = Post.ormar_config.model_fields["categories"]
    assert field.link.source is Post
    assert field.link.target is field.through
    assert field.link.target_fields() == (field.through_owner_field_name(),)
    second = field.through.ormar_config.model_fields[
        field.through_target_field_name()
    ].link
    assert second.source is field.through
    assert second.target is Category


def test_self_reference_m2m_links():
    primary = Person.ormar_config.model_fields["friends"]
    reverse = Person.ormar_config.model_fields["friend_of"]
    assert primary.through_owner_field_name() != reverse.through_owner_field_name()
    assert primary.through_owner_field_name() == reverse.through_target_field_name()
    assert primary.link.target_fields() == (primary.through_owner_field_name(),)
    assert reverse.link.target_fields() == (reverse.through_owner_field_name(),)


def test_link_after_forward_ref_resolution():
    link = LateChild.ormar_config.model_fields["parent"].link
    assert link.target is LateParent
    assert link.target_columns() == ("late_pk",)


def test_link_of_inherited_field_points_at_child_owner():
    assert SubTrack.ormar_config.model_fields["album"].link.source is SubTrack
    assert Track.ormar_config.model_fields["album"].link.source is Track


def test_key_of_and_key_getter():
    link = Track.ormar_config.model_fields["album"].link
    track = Track(album=Album(id=4, name="x"), title="t")
    assert link.key_of(track) == 4
    assert link.key_getter(side="source", prefix="ab")({"ab_album_ref": 4}) == 4
    assert link.key_getter(side="target")({"album_pk": 5}) == 5
    reverse = Album.ormar_config.model_fields["tracks"].link
    assert reverse.key_of(Album(id=6, name="y")) == 6


def test_key_value_and_key_parts_round_trip():
    assert key_value(parts=(3,)) == 3
    assert key_value(parts=(1, 7)) == (1, 7)
    assert key_parts(value=3, size=1) == (3,)
    assert key_parts(value=(1, 7), size=2) == (1, 7)
    assert key_parts(value=[1, 7], size=2) == (1, 7)
