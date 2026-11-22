from typing import ForwardRef, Optional

import pytest
import pytest_asyncio

import ormar
from ormar import Case, F, When
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.templates import template_cache_for
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Label(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ccr_labels")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ccr_albums")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    rank: int = ormar.Integer(default=0)
    label: Optional[Label] = ormar.ForeignKey(Label, nullable=True)
    parent = ormar.ForeignKey(
        ForwardRef("Album"), nullable=True, related_name="children"
    )


Album.update_forward_refs()


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ccr_tracks")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=20)
    position: Optional[int] = ormar.Integer(nullable=True)
    album: Optional[Album] = ormar.ForeignKey(Album, nullable=True)


class Release(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="ccr_releases")

    region: str = ormar.String(max_length=5, primary_key=True)
    number: int = ormar.Integer(primary_key=True)
    copies: int = ormar.Integer(default=0)
    album: Optional[Album] = ormar.ForeignKey(Album, nullable=True)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def catalog():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            indie = await Label.objects.create(name="indie")
            first = await Album.objects.create(name="first", rank=5, label=indie)
            second = await Album.objects.create(name="second", rank=7, parent=first)
            for title, album in [("a", first), ("b", first), ("c", second)]:
                await Track.objects.create(title=title, album=album)
            await Track.objects.create(title="d")
            await Release.objects.create(region="eu", number=1, album=first)
            await Release.objects.create(region="eu", number=2)
            await Release.objects.create(region="us", number=1, album=second)
            yield


async def positions() -> dict:
    return dict(
        await Track.objects.order_by("title").values_list(["title", "position"])
    )


async def matching_titles(case: Case) -> list:
    queryset = Track.objects.filter(position=case).order_by("title")
    return await queryset.values_list("title", flatten=True)


@pytest.mark.asyncio
async def test_rows_without_related_row_take_the_default(catalog):
    in_first = Case(When(album__name="first", then=1), default=0)
    assert await Track.objects.update(each=True, position=in_first) == 4
    assert await positions() == {"a": 1, "b": 1, "c": 0, "d": 0}
    assert await matching_titles(case=in_first) == ["a", "b", "c", "d"]


@pytest.mark.asyncio
async def test_branches_fall_through_to_the_next_one(catalog):
    by_album = Case(
        When(album__label__name="indie", then=1),
        When(album__name="second", then=2),
        default=0,
    )
    updated = await Track.objects.filter(title__in=["a", "c", "d"]).update(
        position=by_album
    )
    assert updated == 3
    assert await positions() == {"a": 1, "b": None, "c": 2, "d": 0}
    assert await matching_titles(case=by_album) == ["a", "c", "d"]


@pytest.mark.asyncio
async def test_or_of_own_and_related_conditions(catalog):
    either = Case(When(ormar.or_(title="d", album__name="second"), then=2), default=5)
    assert await Track.objects.update(each=True, position=either) == 4
    assert await positions() == {"a": 5, "b": 5, "c": 2, "d": 2}
    assert await matching_titles(case=either) == ["a", "b", "c", "d"]


@pytest.mark.asyncio
async def test_composite_primary_key(catalog):
    await Release.objects.update(
        each=True, copies=Case(When(album__name="first", then=10), default=1)
    )
    rows = await Release.objects.order_by(["region", "number"]).values_list(
        ["region", "number", "copies"]
    )
    assert rows == [("eu", 1, 10), ("eu", 2, 1), ("us", 1, 1)]


@pytest.mark.asyncio
async def test_related_value_joins_only_its_path(catalog):
    ranked = Case(When(album__label__name="indie", then=F("album__rank")), default=-1)
    # the value reads the album, so tracks without one are not updated
    assert await Track.objects.update(each=True, position=ranked) == 3
    assert await positions() == {"a": 5, "b": 5, "c": -1, "d": None}


@pytest.mark.asyncio
async def test_self_referencing_condition_and_filter(catalog):
    with_parent = Case(When(parent__name="first", then=1), default=0)
    assert await Album.objects.update(each=True, rank=with_parent) == 2
    rows = await Album.objects.order_by("name").values_list(["name", "rank"])
    assert rows == [("first", 0), ("second", 1)]
    updated = await Album.objects.filter(children__name="second").update(
        rank=Case(When(parent__name="first", then=3), default=4)
    )
    assert updated == 1
    rows = await Album.objects.order_by("name").values_list(["name", "rank"])
    assert rows == [("first", 4), ("second", 1)]


@pytest.mark.asyncio
async def test_update_returning(catalog):
    updated = await Track.objects.filter(title__in=["c", "d"]).update_returning(
        position=Case(When(album__parent__name="first", then=7), default=8)
    )
    assert sorted((track.title, track.position) for track in updated) == [
        ("c", 7),
        ("d", 8),
    ]


@pytest.mark.asyncio
async def test_statements_are_cached_with_the_condition_values(catalog):
    cache = template_cache_for(model_cls=Track)
    cache.clear()
    for name, value in [("first", 1), ("second", 2)]:
        await Track.objects.update(
            each=True, position=Case(When(album__name=name, then=value), default=0)
        )
    assert len(cache.entries) == 1
    await Track.objects.update(
        each=True, position=Case(When(title="a", then=3), default=0)
    )
    assert len(cache.entries) == 2
    assert await positions() == {"a": 3, "b": 0, "c": 0, "d": 0}


@pytest.mark.asyncio
async def test_upsert_still_rejects_related_conditions(catalog):
    with pytest.raises(QueryDefinitionError, match="only fields of the inserted"):
        await Track.objects.create(
            id=1,
            title="a",
            on_conflict="update",
            conflict_target=["id"],
            update_values={"position": Case(When(album__name="first", then=1))},
        )
