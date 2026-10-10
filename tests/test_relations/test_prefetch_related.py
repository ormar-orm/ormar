from typing import Optional

import pytest

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config(force_rollback=True)


class RandomSet(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="randoms")

    id: int = ormar.Integer(name="random_id", primary_key=True)
    name: str = ormar.String(max_length=100)


class Tonation(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tonations")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(name="tonation_name", max_length=100)
    rand_set: Optional[RandomSet] = ormar.ForeignKey(RandomSet)


class Division(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="divisions")

    id: int = ormar.Integer(name="division_id", primary_key=True)
    name: Optional[str] = ormar.String(max_length=100, nullable=True)


class Shop(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="shops")

    id: int = ormar.Integer(primary_key=True)
    name: Optional[str] = ormar.String(max_length=100, nullable=True)
    division: Optional[Division] = ormar.ForeignKey(Division)


class AlbumShops(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="albums_x_shops")


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="albums")

    id: int = ormar.Integer(primary_key=True)
    name: Optional[str] = ormar.String(max_length=100, nullable=True)
    shops: list[Shop] = ormar.ManyToMany(to=Shop, through=AlbumShops)
    sides: list = ormar.JSON(default=list)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tracks")

    id: int = ormar.Integer(name="track_id", primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    title: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
    tonation: Optional[Tonation] = ormar.ForeignKey(Tonation, name="tonation_id")


class Cover(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="covers")

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(
        Album, related_name="cover_pictures", name="album_id"
    )
    title: str = ormar.String(max_length=100)
    artist: Optional[str] = ormar.String(max_length=200, nullable=True)


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_prefetch_related():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            album = Album(name="Malibu")
            await album.save()
            ton1 = await Tonation.objects.create(name="B-mol")
            await Track.objects.create(
                album=album, title="The Bird", position=1, tonation=ton1
            )
            await Track.objects.create(
                album=album,
                title="Heart don't stand a chance",
                position=2,
                tonation=ton1,
            )
            await Track.objects.create(
                album=album, title="The Waters", position=3, tonation=ton1
            )
            await Cover.objects.create(title="Cover1", album=album, artist="Artist 1")
            await Cover.objects.create(title="Cover2", album=album, artist="Artist 2")

            fantasies = Album(name="Fantasies")
            await fantasies.save()
            await Track.objects.create(
                album=fantasies, title="Help I'm Alive", position=1
            )
            await Track.objects.create(album=fantasies, title="Sick Muse", position=2)
            await Track.objects.create(
                album=fantasies, title="Satellite Mind", position=3
            )
            await Cover.objects.create(
                title="Cover3", album=fantasies, artist="Artist 3"
            )
            await Cover.objects.create(
                title="Cover4", album=fantasies, artist="Artist 4"
            )

            album = (
                await Album.objects.filter(name="Malibu")
                .prefetch_related(["tracks__tonation", "cover_pictures"])
                .get()
            )
            assert len(album.tracks) == 3
            assert album.tracks[0].title == "The Bird"
            assert len(album.cover_pictures) == 2
            assert album.cover_pictures[0].title == "Cover1"
            assert (
                album.tracks[0].tonation.name
                == album.tracks[2].tonation.name
                == "B-mol"
            )

            albums = await Album.objects.prefetch_related("tracks").all()
            assert len(albums[0].tracks) == 3
            assert len(albums[1].tracks) == 3
            assert albums[0].tracks[0].title == "The Bird"
            assert albums[1].tracks[0].title == "Help I'm Alive"

            track = await Track.objects.prefetch_related(["album__cover_pictures"]).get(
                title="The Bird"
            )
            assert track.album.name == "Malibu"
            assert len(track.album.cover_pictures) == 2
            assert track.album.cover_pictures[0].artist == "Artist 1"

            track = (
                await Track.objects.prefetch_related(["album__cover_pictures"])
                .exclude_fields("album__cover_pictures__artist")
                .get(title="The Bird")
            )
            assert track.album.name == "Malibu"
            assert len(track.album.cover_pictures) == 2
            assert track.album.cover_pictures[0].artist is None

            tracks = await Track.objects.prefetch_related("album").all()
            assert len(tracks) == 6


@pytest.mark.asyncio
async def test_prefetch_related_with_many_to_many():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            div = await Division.objects.create(name="Div 1")
            shop1 = await Shop.objects.create(name="Shop 1", division=div)
            shop2 = await Shop.objects.create(name="Shop 2", division=div)
            album = Album(name="Malibu")
            await album.save()
            await album.shops.add(shop1)
            await album.shops.add(shop2)

            await Track.objects.create(album=album, title="The Bird", position=1)
            await Track.objects.create(
                album=album, title="Heart don't stand a chance", position=2
            )
            await Track.objects.create(album=album, title="The Waters", position=3)
            await Cover.objects.create(title="Cover1", album=album, artist="Artist 1")
            await Cover.objects.create(title="Cover2", album=album, artist="Artist 2")

            track = await Track.objects.prefetch_related(
                ["album__cover_pictures", "album__shops__division"]
            ).get(title="The Bird")
            assert track.album.name == "Malibu"
            assert len(track.album.cover_pictures) == 2
            assert track.album.cover_pictures[0].artist == "Artist 1"

            assert len(track.album.shops) == 2
            assert track.album.shops[0].name == "Shop 1"
            assert track.album.shops[0].division.name == "Div 1"

            album2 = Album(name="Malibu 2")
            await album2.save()
            await album2.shops.add(shop1)
            await album2.shops.add(shop2)
            await Track.objects.create(album=album2, title="The Bird 2", position=1)

            tracks = await Track.objects.prefetch_related(["album__shops"]).all()
            assert tracks[0].album.name == "Malibu"
            assert tracks[0].album.shops[0].name == "Shop 1"
            assert tracks[3].album.name == "Malibu 2"
            assert tracks[3].album.shops[0].name == "Shop 1"

            assert tracks[0].album.shops[0] == tracks[3].album.shops[0]
            assert id(tracks[0].album.shops[0]) == id(tracks[3].album.shops[0])
            tracks[0].album.shops[0].name = "Dummy"
            assert tracks[0].album.shops[0].name == tracks[3].album.shops[0].name


@pytest.mark.asyncio
async def test_prefetch_related_empty():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            await Track.objects.create(title="The Bird", position=1)
            track = await Track.objects.prefetch_related(["album__cover_pictures"]).get(
                title="The Bird"
            )
            assert track.title == "The Bird"
            assert track.album is None


@pytest.mark.asyncio
async def test_prefetch_related_with_select_related():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            div = await Division.objects.create(name="Div 1")
            shop1 = await Shop.objects.create(name="Shop 1", division=div)
            shop2 = await Shop.objects.create(name="Shop 2", division=div)
            album = Album(name="Malibu")
            await album.save()
            await album.shops.add(shop1)
            await album.shops.add(shop2)

            await Cover.objects.create(title="Cover1", album=album, artist="Artist 1")
            await Cover.objects.create(title="Cover2", album=album, artist="Artist 2")

            album = (
                await Album.objects.select_related(["tracks", "shops"])
                .filter(name="Malibu")
                .prefetch_related(["cover_pictures", "shops__division"])
                .first()
            )

            assert len(album.tracks) == 0
            assert len(album.cover_pictures) == 2
            assert album.shops[0].division.name == "Div 1"

            rand_set = await RandomSet.objects.create(name="Rand 1")
            ton1 = await Tonation.objects.create(name="B-mol", rand_set=rand_set)
            await Track.objects.create(
                album=album, title="The Bird", position=1, tonation=ton1
            )
            await Track.objects.create(
                album=album,
                title="Heart don't stand a chance",
                position=2,
                tonation=ton1,
            )
            await Track.objects.create(
                album=album, title="The Waters", position=3, tonation=ton1
            )

            album = (
                await Album.objects.select_related("tracks__tonation__rand_set")
                .filter(name="Malibu")
                .prefetch_related(["cover_pictures", "shops__division"])
                .order_by(
                    ["-shops__name", "-cover_pictures__artist", "shops__division__name"]
                )
                .get()
            )
            assert len(album.tracks) == 3
            assert album.tracks[0].tonation == album.tracks[2].tonation == ton1
            assert len(album.cover_pictures) == 2
            assert album.cover_pictures[0].artist == "Artist 2"

            assert len(album.shops) == 2
            assert album.shops[0].name == "Shop 2"
            assert album.shops[0].division.name == "Div 1"

            track = (
                await Track.objects.select_related("album")
                .prefetch_related(["album__cover_pictures", "album__shops__division"])
                .get(title="The Bird")
            )
            assert track.album.name == "Malibu"
            assert len(track.album.cover_pictures) == 2
            assert track.album.cover_pictures[0].artist == "Artist 1"

            assert len(track.album.shops) == 2
            assert track.album.shops[0].name == "Shop 1"
            assert track.album.shops[0].division.name == "Div 1"


@pytest.mark.asyncio
async def test_prefetch_related_with_order_by_reusable_queryset():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            album = await Album.objects.create(name="Malibu")
            await Cover.objects.create(title="Cover1", album=album, artist="Artist 1")
            await Cover.objects.create(title="Cover2", album=album, artist="Artist 2")

            queryset = Album.objects.prefetch_related("cover_pictures").order_by(
                "-cover_pictures__artist"
            )
            for _ in range(2):
                albums = await queryset.all()
                assert len(albums) == 1
                assert albums[0].cover_pictures[0].artist == "Artist 2"


@pytest.mark.asyncio
async def test_prefetch_related_with_select_related_and_fields():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            div = await Division.objects.create(name="Div 1")
            shop1 = await Shop.objects.create(name="Shop 1", division=div)
            shop2 = await Shop.objects.create(name="Shop 2", division=div)
            album = Album(name="Malibu")
            await album.save()
            await album.shops.add(shop1)
            await album.shops.add(shop2)
            await Cover.objects.create(title="Cover1", album=album, artist="Artist 1")
            await Cover.objects.create(title="Cover2", album=album, artist="Artist 2")
            rand_set = await RandomSet.objects.create(name="Rand 1")
            ton1 = await Tonation.objects.create(name="B-mol", rand_set=rand_set)
            await Track.objects.create(
                album=album, title="The Bird", position=1, tonation=ton1
            )
            await Track.objects.create(
                album=album,
                title="Heart don't stand a chance",
                position=2,
                tonation=ton1,
            )
            await Track.objects.create(
                album=album, title="The Waters", position=3, tonation=ton1
            )

            album = (
                await Album.objects.select_related("tracks__tonation__rand_set")
                .filter(name="Malibu")
                .prefetch_related(["cover_pictures", "shops__division"])
                .exclude_fields({"shops": {"division": {"name"}}})
                .get()
            )
            assert len(album.tracks) == 3
            assert album.tracks[0].tonation == album.tracks[2].tonation == ton1
            assert len(album.cover_pictures) == 2
            assert album.cover_pictures[0].artist == "Artist 1"

            assert len(album.shops) == 2
            assert album.shops[0].name == "Shop 1"
            assert album.shops[0].division.name is None

            album = (
                await Album.objects.select_related("tracks")
                .filter(name="Malibu")
                .prefetch_related(["cover_pictures", "shops__division"])
                .fields(
                    {
                        "name": ...,
                        "shops": {"division"},
                        "cover_pictures": {"id": ..., "title": ...},
                    }
                )
                .exclude_fields({"shops": {"division": {"name"}}})
                .get()
            )
            assert len(album.tracks) == 3
            assert len(album.cover_pictures) == 2
            assert album.cover_pictures[0].artist is None
            assert album.cover_pictures[0].title is not None

            assert len(album.shops) == 2
            assert album.shops[0].name is None
            assert album.shops[0].division is not None
            assert album.shops[0].division.name is None
