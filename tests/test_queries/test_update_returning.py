import asyncio
import contextvars
from typing import Any, Optional

import pytest
import pytest_asyncio
import sqlalchemy
from sqlalchemy.dialects import mysql, postgresql, sqlite

import ormar
from ormar import F, Greatest
from ormar.exceptions import QueryDefinitionError
from tests.lifespan import init_tests
from tests.settings import DATABASE_URL, create_config

base_ormar_config = create_config()
database = base_ormar_config.database


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="urt_albums")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)
    rank: int = ormar.Integer(default=0, name="rank_col")


class Genre(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="urt_genres")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=20)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="urt_tracks")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=20)
    position: Optional[int] = ormar.Integer(nullable=True)
    album: Optional[Album] = ormar.ForeignKey(Album, nullable=True)
    genres: list[Genre] = ormar.ManyToMany(Genre)


class Release(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="urt_releases")

    region: str = ormar.String(max_length=5, primary_key=True)
    number: int = ormar.Integer(primary_key=True)
    copies: int = ormar.Integer(default=0)
    album: Optional[Album] = ormar.ForeignKey(Album, nullable=True)


create_test_database = init_tests(base_ormar_config)


async def create_catalog() -> None:
    first = await Album.objects.create(name="first", rank=5)
    second = await Album.objects.create(name="second", rank=7)
    jazz = await Genre.objects.create(name="jazz")
    for title, album in [("a", first), ("b", first), ("c", second), ("d", None)]:
        track = await Track.objects.create(title=title, album=album)
        if title in ("b", "c"):
            await jazz.tracks.add(track)
    for region, number, album in [("eu", 1, first), ("us", 1, second)]:
        await Release.objects.create(region=region, number=number, album=album)


async def delete_catalog() -> None:
    await Release.objects.delete(each=True)
    await Track.objects.delete(each=True)
    await Genre.objects.delete(each=True)
    await Album.objects.delete(each=True)


@pytest.fixture(params=["returning", "locked"])
def statements(request, monkeypatch):
    """
    Runs a test with ``UPDATE ... RETURNING`` where the database has it and with
    the locked select, update and read back MySQL uses, on every database.
    """
    if request.param == "locked":
        monkeypatch.setattr(type(database.rules), "update_returning", False)
    return request.param


@pytest_asyncio.fixture()
async def catalog(statements):
    async with database:
        async with database.transaction(force_rollback=True):
            await create_catalog()
            yield


def by_title(tracks: list[Track]) -> dict:
    return {track.title: track.position for track in tracks}


async def assert_stored(returned: list) -> None:
    model = type(returned[0])
    stored = await model.objects.all()
    stored_by_pk = {item.pk: item for item in stored}
    for item in returned:
        assert item.model_dump() == stored_by_pk[item.pk].model_dump()
        assert item.saved


@pytest.mark.asyncio
async def test_update_returning_plain(catalog):
    returned = await Track.objects.filter(title__in=["a", "c"]).update_returning(
        position=3
    )
    assert by_title(returned) == {"a": 3, "c": 3}
    await assert_stored(returned)
    first = next(track for track in returned if track.title == "a")
    assert first.album.pk == (await Album.objects.get(name="first")).pk
    assert first.album.name is None
    assert by_title(await Track.objects.all()) == {
        "a": 3,
        "b": None,
        "c": 3,
        "d": None,
    }


@pytest.mark.asyncio
async def test_update_returning_related_filter(catalog):
    returned = await Track.objects.filter(album__name="first").update_returning(
        position=1
    )
    assert by_title(returned) == {"a": 1, "b": 1}
    await assert_stored(returned)
    queryset = Track.objects.filter(ormar.or_(album__name="second", title="d")).exclude(
        album__rank__lt=6
    )
    expected = sorted(track.title for track in await queryset.all())
    returned = await queryset.update_returning(position=2)
    assert sorted(track.title for track in returned) == expected == ["c"]


@pytest.mark.asyncio
async def test_update_returning_expressions(catalog):
    returned = await Track.objects.filter(title__in=["a", "c", "d"]).update_returning(
        position=F("album__rank") + 1
    )
    # a track without an album has no row to read the rank from
    assert by_title(returned) == {"a": 6, "c": 8}
    await assert_stored(returned)
    returned = await Track.objects.filter(album__name="second").update_returning(
        position=Greatest(F("album__rank"), 10)
    )
    assert by_title(returned) == {"c": 10}
    returned = await Album.objects.filter(name="first").update_returning(
        rank=Album.rank * 2
    )
    assert [(album.name, album.rank) for album in returned] == [("first", 10)]
    await assert_stored(returned)


@pytest.mark.asyncio
async def test_update_returning_each_and_no_match(catalog):
    with pytest.raises(QueryDefinitionError):
        await Track.objects.update_returning(position=1)
    assert await Track.objects.filter(title="x").update_returning(position=1) == []
    returned = await Track.objects.update_returning(each=True, position=4)
    assert by_title(returned) == {"a": 4, "b": 4, "c": 4, "d": 4}
    await assert_stored(returned)


@pytest.mark.asyncio
async def test_update_returning_composite_pk(catalog):
    returned = await Release.objects.filter(album__name="first").update_returning(
        copies=5
    )
    assert [(release.region, release.number) for release in returned] == [("eu", 1)]
    returned = await Release.objects.update_returning(
        each=True, copies=F("album__rank") + F("copies")
    )
    assert {release.region: release.copies for release in returned} == {
        "eu": 10,
        "us": 7,
    }
    await assert_stored(returned)


@pytest.mark.asyncio
async def test_update_returning_on_queryset_proxy(catalog):
    album = await Album.objects.select_related("tracks").get(name="first")
    with pytest.raises(QueryDefinitionError):
        await album.tracks.update_returning(position=1)
    returned = await album.tracks.update_returning(each=True, position=7)
    assert by_title(returned) == {"a": 7, "b": 7}
    returned = await album.tracks.filter(title="b").update_returning(position=8)
    assert by_title(returned) == {"b": 8}
    await assert_stored(returned)
    jazz = await Genre.objects.get(name="jazz")
    returned = await jazz.tracks.update_returning(each=True, position=F("album__rank"))
    assert by_title(returned) == {"b": 5, "c": 7}
    await assert_stored(returned)


@pytest.mark.asyncio
async def test_update_returning_inside_transaction_is_rolled_back(catalog):
    try:
        async with database.transaction():
            returned = await Track.objects.filter(title="a").update_returning(
                position=9
            )
            assert by_title(returned) == {"a": 9}
            raise RuntimeError("roll back")
    except RuntimeError:
        pass
    assert (await Track.objects.get(title="a")).position is None


@pytest.mark.asyncio
async def test_update_returning_outside_transaction(statements):
    async with database:
        try:
            await create_catalog()
            returned = await Track.objects.filter(album__name="first").update_returning(
                position=F("album__rank")
            )
            assert by_title(returned) == {"a": 5, "b": 5}
            await assert_stored(returned)
        finally:
            await delete_catalog()


@pytest.mark.asyncio
async def test_update_returning_statements(catalog):
    queryset = Track.objects.filter(album__name="first")
    values = {"position": F("album__rank")._node()}
    returning = queryset.build_write_template(kind="update_returning", values=values)
    assert str(returning.statement.compile(dialect=postgresql.dialect())).endswith(
        "RETURNING urt_tracks.id, urt_tracks.title, urt_tracks.position, "
        "urt_tracks.album"
    )
    lock = queryset.build_write_template(kind="lock", values=values)
    lock_sql = str(lock.statement.compile(dialect=postgresql.dialect()))
    assert lock_sql.startswith("SELECT urt_tracks.id \nFROM urt_tracks, urt_albums")
    assert lock_sql.endswith("FOR UPDATE OF urt_tracks")
    locked = Release.objects.build_write_template(
        kind="update_locked", values={"copies": 1}
    )
    assert str(locked.statement.compile(dialect=sqlite.dialect())) == (
        "UPDATE urt_releases SET copies=? "
        "WHERE (urt_releases.region, urt_releases.number) IN "
        "(__[POSTCOMPILE_ormar_pks])"
    )


def single_spaced(compiled: Any) -> str:
    return " ".join(str(compiled).split())


@pytest.mark.asyncio
async def test_update_sql_is_unchanged(catalog):
    """
    ``update()`` statements as built before ``update_returning()``.
    """
    plain = Track.objects.filter(title="a").build_write_template(
        kind="update", values={"position": 3}
    )
    expression = Track.objects.filter(title="a").build_write_template(
        kind="update", values={"position": (F("album__rank") + 1)._node()}
    )
    related = Release.objects.filter(album__name="x").build_write_template(
        kind="update", values={"copies": 2}
    )
    expected = {
        "sqlite": [
            "UPDATE urt_tracks SET position=? WHERE urt_tracks.title = ?",
            "UPDATE urt_tracks SET position=(t1b26c7_urt_albums.rank_col + ?) "
            "FROM urt_albums AS t1b26c7_urt_albums "
            "WHERE t1b26c7_urt_albums.id = urt_tracks.album "
            "AND urt_tracks.title = ?",
        ],
        "mysql": [
            "UPDATE urt_tracks SET position=%s WHERE urt_tracks.title = %s",
            "UPDATE urt_tracks, urt_albums AS t1b26c7_urt_albums "
            "SET urt_tracks.position=(t1b26c7_urt_albums.rank_col + %s) "
            "WHERE t1b26c7_urt_albums.id = urt_tracks.album "
            "AND urt_tracks.title = %s",
        ],
    }
    for dialect in (sqlite.dialect(), mysql.dialect()):
        rendered = [
            single_spaced(template.statement.compile(dialect=dialect))
            for template in (plain, expression)
        ]
        assert rendered == expected[dialect.name]
    assert single_spaced(related.statement.compile(dialect=postgresql.dialect())) == (
        "UPDATE urt_releases SET copies=%(ormar_v0)s::INTEGER "
        "WHERE (urt_releases.region, urt_releases.number) IN "
        "(SELECT ormar_matched.region, ormar_matched.number FROM "
        "(SELECT urt_releases.region AS region, urt_releases.number AS number "
        "FROM urt_releases LEFT OUTER JOIN urt_albums AS t1b26c7_urt_albums "
        "ON t1b26c7_urt_albums.id=urt_releases.album "
        "WHERE t1b26c7_urt_albums.name = %(ormar_p0)s::VARCHAR) AS ormar_matched)"
    )


async def hold_updated_rows(
    locked: asyncio.Event, release: asyncio.Event
) -> None:  # pragma: no cover
    async with database.transaction():
        returned = await Track.objects.filter(title="a").update_returning(position=1)
        assert by_title(returned) == {"a": 1}
        locked.set()
        await release.wait()


@pytest.mark.skipif(DATABASE_URL.startswith("sqlite"), reason="SQLite has no row locks")
@pytest.mark.asyncio
async def test_updated_rows_stay_locked(statements):  # pragma: no cover
    async with database:
        try:
            await create_catalog()
            locked, release = asyncio.Event(), asyncio.Event()

            async def probe() -> None:
                await locked.wait()
                try:
                    with pytest.raises(sqlalchemy.exc.DBAPIError):
                        async with database.transaction():
                            await Track.objects.select_for_update(nowait=True).get(
                                title="a"
                            )
                finally:
                    release.set()

            await asyncio.gather(hold_updated_rows(locked, release), probe())
            assert (await Track.objects.get(title="a")).position == 1
        finally:
            await delete_catalog()


async def rename_track(title: str, new_title: str) -> None:  # pragma: no cover
    await Track.objects.filter(title=title).update(title=new_title)


@pytest.mark.skipif(
    DATABASE_URL.startswith("sqlite"), reason="SQLite locks the whole database"
)
@pytest.mark.asyncio
async def test_rows_changed_by_others_come_back_as_stored(
    statements,
):  # pragma: no cover
    """
    A row changed and committed by another connection after the transaction
    read it comes back as stored, also when this update leaves it unchanged
    (a plain read of the MySQL path would return the transaction's snapshot).
    """
    async with database:
        try:
            await create_catalog()
            track_id = (await Track.objects.get(title="a")).id
            async with database.transaction():
                await Track.objects.all()
                # another connection: the task gets a context without the
                # transaction of this one
                await contextvars.Context().run(
                    asyncio.ensure_future, rename_track(title="a", new_title="z")
                )
                returned = await Track.objects.filter(id=track_id).update_returning(
                    position=None
                )
                assert [(track.title, track.position) for track in returned] == [
                    ("z", None)
                ]
        finally:
            await delete_catalog()
