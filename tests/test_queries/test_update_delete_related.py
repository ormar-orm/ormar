from typing import ForwardRef, Optional

import pytest
import pytest_asyncio

import ormar
from ormar import Coalesce, F, Greatest, Least
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Label(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="udr_labels")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    bonus: int = ormar.Integer(default=0)


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="udr_albums")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    rank: int = ormar.Integer(default=0, name="rank_col")
    label: Optional[Label] = ormar.ForeignKey(Label, nullable=True)
    parent = ormar.ForeignKey(
        ForwardRef("Album"), nullable=True, related_name="children"
    )


Album.update_forward_refs()


class Genre(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="udr_genres")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="udr_tracks")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=20)
    position: Optional[int] = ormar.Integer(nullable=True)
    album: Optional[Album] = ormar.ForeignKey(Album, nullable=True)
    genres: list[Genre] = ormar.ManyToMany(Genre)


class Release(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="udr_releases")

    region: str = ormar.String(max_length=5, primary_key=True)
    number: int = ormar.Integer(primary_key=True)
    copies: int = ormar.Integer(default=0)
    album: Optional[Album] = ormar.ForeignKey(Album, nullable=True)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def catalog():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            indie = await Label.objects.create(name="indie", bonus=100)
            first = await Album.objects.create(name="first", rank=5, label=indie)
            second = await Album.objects.create(name="second", rank=7, parent=first)
            rock = await Genre.objects.create(name="rock")
            jazz = await Genre.objects.create(name="jazz")
            for title, album, genres in [
                ("a", first, [rock]),
                ("b", first, [rock, jazz]),
                ("c", second, [jazz]),
                ("d", None, []),
            ]:
                track = await Track.objects.create(title=title, album=album)
                for genre in genres:
                    await track.genres.add(genre)
            for region, number, album in [("eu", 1, first), ("us", 1, second)]:
                await Release.objects.create(region=region, number=number, album=album)
            yield


async def positions() -> dict:
    tracks = await Track.objects.order_by("title").all()
    return {track.title: track.position for track in tracks}


@pytest.mark.asyncio
async def test_update_and_delete_touch_the_rows_all_returns(catalog):
    querysets = [
        Track.objects.filter(album__name="first"),
        Track.objects.filter(album__label__name="indie"),
        Track.objects.filter(genres__name="jazz"),
        Track.objects.filter(Track.album.rank > 5),
        Track.objects.filter(position__isnull=True).exclude(album__name="first"),
        Track.objects.filter(ormar.or_(album__name="second", genres__name="rock")),
        Track.objects.filter(
            title__in=["a", "c"], album__rank__lt=F("album__rank") + 1
        ),
    ]
    for index, queryset in enumerate(querysets):
        expected = sorted(track.title for track in await queryset.all())
        assert await queryset.update(position=index) == len(expected)
        assert (
            sorted(
                title
                for title, position in (await positions()).items()
                if position == index
            )
            == expected
        )


@pytest.mark.asyncio
async def test_delete_with_related_filters(catalog):
    assert await Track.objects.filter(genres__name="rock").delete() == 2
    assert await Track.objects.delete(album__parent__name="first") == 1
    assert await Track.objects.values_list("title", flatten=True) == ["d"]
    assert await Release.objects.delete(each=True) == 2
    assert (
        await Album.objects.filter(tracks__id__isnull=True).delete(name="second") == 1
    )
    assert await Album.objects.count() == 1


@pytest.mark.asyncio
async def test_update_through_reverse_relation(catalog):
    assert await Album.objects.filter(tracks__title="c").update(rank=0) == 1
    assert await Album.objects.filter(children__name="second").update(rank=1) == 1
    assert {album.name: album.rank for album in await Album.objects.all()} == {
        "first": 1,
        "second": 0,
    }


@pytest.mark.asyncio
async def test_update_reads_related_fields(catalog):
    assert await Track.objects.update(each=True, position=F("album__rank") * 10) == 3
    assert await positions() == {"a": 50, "b": 50, "c": 70, "d": None}

    updated = await Track.objects.filter(title="a").update(
        position=F("album__label__bonus") + Track.album.rank
    )
    assert updated == 1
    assert (await positions())["a"] == 105

    assert await Album.objects.update(each=True, rank=F("parent__rank") + 1) == 1
    assert {album.name: album.rank for album in await Album.objects.all()} == {
        "first": 5,
        "second": 6,
    }


@pytest.mark.asyncio
async def test_update_functions_read_related_fields(catalog):
    updated = await Track.objects.update(
        each=True, position=Greatest(F("album__rank"), 6) + Coalesce(F("position"), 0)
    )
    assert updated == 3
    assert await positions() == {"a": 6, "b": 6, "c": 7, "d": None}

    assert (
        await Track.objects.filter(title="c").update(
            position=Least(F("album__parent__rank"), F("album__rank"))
        )
        == 1
    )
    assert (await positions())["c"] == 5

    with pytest.raises(QueryDefinitionError):
        await Album.objects.update(each=True, rank=Greatest(F("tracks__position"), 0))


@pytest.mark.asyncio
async def test_update_reads_related_fields_with_related_filters(catalog):
    queryset = Track.objects.filter(genres__name="jazz").exclude(album__name="first")
    assert await queryset.update(position=F("album__parent__rank")) == 1
    assert await positions() == {"a": None, "b": None, "c": 5, "d": None}
    assert await queryset.update(position=F("album__label__bonus")) == 0


@pytest.mark.asyncio
async def test_update_with_composite_primary_key(catalog):
    queryset = Release.objects.filter(album__name="second")
    assert await queryset.update(copies=F("album__rank") + F("copies")) == 1
    assert await Release.objects.filter(album__rank__gt=0).delete(region="eu") == 1
    releases = await Release.objects.all()
    assert [(release.region, release.copies) for release in releases] == [("us", 7)]


@pytest.mark.asyncio
async def test_update_expressions_read_only_forward_relations(catalog):
    for path, relation in [
        ("tracks__position", "tracks"),
        ("children__rank", "children"),
    ]:
        with pytest.raises(QueryDefinitionError, match=f"relation '{relation}'"):
            await Album.objects.update(each=True, rank=F(path))
    with pytest.raises(QueryDefinitionError, match="relation 'genres'"):
        await Track.objects.update(each=True, position=F("genres__id"))
    with pytest.raises(QueryDefinitionError, match="many rows"):
        await Track.objects.update(each=True, position=F("trackgenre__id"))


@pytest.mark.asyncio
async def test_update_reads_the_old_values_of_the_updated_table(catalog):
    second = await Album.objects.get(name="second")
    await Album.objects.create(name="third", rank=9, parent=second)
    updated = await Album.objects.filter(parent__isnull=False).update(
        rank=F("parent__rank") + 1, name=F("name")
    )
    assert updated == 2
    ranks = {album.name: album.rank for album in await Album.objects.all()}
    assert ranks == {"first": 5, "second": 6, "third": 8}


@pytest.mark.asyncio
async def test_related_writes_are_cached_per_joined_relations(catalog):
    cache = template_cache_for(model_cls=Track)
    cache.clear()
    for name in ("first", "second"):
        await Track.objects.filter(album__name=name).update(position=1)
    assert len(cache.entries) == 1
    await (
        Track.objects.select_related("genres")
        .filter(album__name="x")
        .update(position=1)
    )
    assert len(cache.entries) == 2
    assert await Track.objects.filter(position=1).count() == 3
