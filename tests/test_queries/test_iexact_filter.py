from typing import Optional

import pytest
import pytest_asyncio

import ormar
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="iex_albums")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="iex_tracks")

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    name: str = ormar.String(max_length=100)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def tracks():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            first = await Album.objects.create(name="A_C")
            second = await Album.objects.create(name="abc")
            for album, name in [
                (first, "a_c"),
                (first, "A_C"),
                (second, "abc"),
                (second, "100%"),
                (second, "1000"),
            ]:
                await Track.objects.create(album=album, name=name)
            yield


async def names(queryset):
    return sorted(track.name for track in await queryset.all())


@pytest.mark.asyncio
async def test_iexact_does_not_treat_like_wildcards_as_wildcards(tracks):
    expected = [
        (Track.objects.filter(name__iexact="a_c"), ["A_C", "a_c"]),
        (Track.objects.filter(Track.name.iexact("A_C")), ["A_C", "a_c"]),
        (Track.objects.filter(name__iexact="100%"), ["100%"]),
        (Track.objects.filter(name__iexact="%"), []),
        (Track.objects.filter(name__iexact="ABC"), ["abc"]),
        (Track.objects.filter(album__name__iexact="a_c"), ["A_C", "a_c"]),
        (Track.objects.exclude(name__iexact="a_c"), ["100%", "1000", "abc"]),
        (
            Track.objects.filter(ormar.or_(name__iexact="a_c", name="1000")),
            ["1000", "A_C", "a_c"],
        ),
    ]
    for queryset, result in expected:
        assert await names(queryset=queryset) == result
    assert await Track.objects.get(name__iexact="100%") is not None
    assert await Track.objects.filter(name__iexact="_bc").count() == 0


@pytest.mark.asyncio
async def test_iexact_with_and_without_wildcards_get_their_own_statements(tracks):
    template_cache_for(model_cls=Track).clear()

    def template(**kwargs):
        return Track.objects.filter(**kwargs).build_select_template()

    plain = template(name__iexact="abc")
    escaped = template(name__iexact="a_c")
    assert escaped.statement is not plain.statement
    assert template(name__iexact="b%").statement is escaped.statement
    assert template(name__iexact="xyz").statement is plain.statement
    assert await names(queryset=Track.objects.filter(name__iexact="abc")) == ["abc"]
    assert await names(queryset=Track.objects.filter(name__iexact="a_c")) == [
        "A_C",
        "a_c",
    ]
