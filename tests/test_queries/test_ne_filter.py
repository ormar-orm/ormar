from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar import F
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nef_albums")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    is_best_seller: bool = ormar.Boolean(default=False)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nef_tracks")

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    name: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
    play_count: Optional[int] = ormar.Integer(nullable=True)


class Location(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nef_locations")

    id: int = ormar.Integer(primary_key=True)
    ne: int = ormar.Integer()


class Corner(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nef_corners")

    id: int = ormar.Integer(primary_key=True)
    ne: int = ormar.Integer()
    location: Optional[Location] = ormar.ForeignKey(Location)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def tracks():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            malibu = await Album.objects.create(name="Malibu", is_best_seller=True)
            hits = await Album.objects.create(name="Hits")
            for album, name, position, play_count in [
                (malibu, "Malibu", 1, None),
                (malibu, "Surf", 2, 5),
                (hits, "Hits", 3, 3),
            ]:
                await Track.objects.create(
                    album=album, name=name, position=position, play_count=play_count
                )
            yield


async def names(queryset):
    return sorted(track.name for track in await queryset.all())


@pytest.mark.asyncio
async def test_ne_filters_values_none_and_booleans(tracks):
    expected = [
        (Track.objects.filter(name__ne="Surf"), ["Hits", "Malibu"]),
        (Track.objects.filter(Track.name != "Surf"), ["Hits", "Malibu"]),
        (Track.objects.filter(play_count__ne=5), ["Hits"]),
        (Track.objects.filter(play_count__ne=None), ["Hits", "Surf"]),
        (Track.objects.filter(Track.play_count != None), ["Hits", "Surf"]),  # noqa
        (Track.objects.filter(album__is_best_seller__ne=True), ["Hits"]),
        (Track.objects.filter(album__name__ne="Malibu"), ["Hits"]),
        (Track.objects.filter(Track.album.name != "Malibu"), ["Hits"]),
        (Track.objects.exclude(name__ne="Surf"), ["Surf"]),
        (
            Track.objects.filter(ormar.or_(play_count__ne=5, position=1)),
            ["Hits", "Malibu"],
        ),
    ]
    for queryset, result in expected:
        assert await names(queryset=queryset) == result
    assert await Album.objects.filter(is_best_seller__ne=True).values_list(
        "name", flatten=True
    ) == ["Hits"]


@pytest.mark.asyncio
async def test_ne_filters_with_expressions(tracks):
    for queryset in [
        Track.objects.filter(play_count__ne=F("position")),
        Track.objects.filter(Track.play_count != Track.position),
        Track.objects.filter(name__ne=F("album__name")),
        Track.objects.filter(Track.name != Track.album.name),
    ]:
        assert await names(queryset=queryset) == ["Surf"]


@pytest.mark.asyncio
async def test_ne_gets_its_own_statement(tracks):
    template_cache_for(model_cls=Track).clear()

    def template(**kwargs):
        return Track.objects.filter(**kwargs).build_select_template()

    not_equal = template(name__ne="a")
    assert not_equal.statement is not template(name="a").statement
    assert template(name__ne="b").statement is not_equal.statement
    assert template(name__ne=None).statement is not template(name=None).statement
    by_expression = template(position__ne=F("play_count"))
    assert by_expression.statement is not template(position=F("play_count")).statement


@pytest.mark.asyncio
async def test_ne_filters_updates_and_deletes(tracks):
    assert await Track.objects.filter(name__ne="Surf").update(position=0) == 2
    assert await Track.objects.filter(position__ne=0).delete() == 1
    assert await names(queryset=Track.objects) == ["Hits", "Malibu"]


@pytest.mark.asyncio
async def test_operator_takes_precedence_over_a_field_named_like_it(tracks):
    first = await Location.objects.create(ne=5)
    second = await Location.objects.create(ne=7)
    await Corner.objects.create(ne=1, location=first)
    await Corner.objects.create(ne=2, location=second)

    async def corners(queryset):
        return sorted(corner.ne for corner in await queryset.all())

    assert await corners(queryset=Corner.objects.filter(ne__exact=1)) == [1]
    assert await corners(queryset=Corner.objects.filter(ne__ne=1)) == [2]
    assert await corners(queryset=Corner.objects.filter(Corner.ne != 1)) == [2]
    assert await corners(queryset=Corner.objects.filter(location__ne=first.id)) == [2]
    assert await corners(queryset=Corner.objects.filter(location__ne__exact=5)) == [1]
    assert await corners(queryset=Corner.objects.filter(location__ne__ne=5)) == [2]
    with pytest.raises(QueryDefinitionError, match="ne__exact"):
        await Corner.objects.filter(ne=1).all()
    malibu = await Album.objects.get(name="Malibu")
    assert await names(queryset=Track.objects.filter(album__ne=malibu)) == ["Hits"]
