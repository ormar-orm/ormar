from typing import Optional

import pytest

import ormar
from ormar.exceptions import MultipleMatches, NoMatch, RelationshipInstanceError
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="albums")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    is_best_seller: bool = ormar.Boolean(default=False)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="tracks")

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album)
    title: str = ormar.String(max_length=100)
    position: int = ormar.Integer()
    play_count: Optional[int] = ormar.Integer(nullable=True, default=0)
    is_disabled: bool = ormar.Boolean(default=False)


class Cover(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="covers")

    id: int = ormar.Integer(primary_key=True)
    album: Optional[Album] = ormar.ForeignKey(Album, related_name="cover_pictures")
    title: str = ormar.String(max_length=100)


class Organisation(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="org")

    id: int = ormar.Integer(primary_key=True)
    ident: str = ormar.String(max_length=100)


class Team(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="teams")

    id: int = ormar.Integer(primary_key=True)
    org: Optional[Organisation] = ormar.ForeignKey(Organisation)
    name: str = ormar.String(max_length=100)


class Member(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="members")

    id: int = ormar.Integer(primary_key=True)
    team: Optional[Team] = ormar.ForeignKey(Team)
    email: str = ormar.String(max_length=100)


create_test_database = init_tests(base_ormar_config)


@pytest.mark.asyncio
async def test_wrong_query_foreign_key_type():
    async with base_ormar_config.database:
        with pytest.raises(RelationshipInstanceError):
            Track(title="The Error", album="wrong_pk_type")


@pytest.mark.asyncio
async def test_setting_explicitly_empty_relation():
    async with base_ormar_config.database:
        track = Track(album=None, title="The Bird", position=1)
        assert track.album is None


@pytest.mark.asyncio
async def test_related_name():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            album = await Album.objects.create(name="Vanilla")
            await Cover.objects.create(album=album, title="The cover file")
            assert len(album.cover_pictures) == 1


@pytest.mark.asyncio
async def test_model_crud():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            album = Album(name="Jamaica")
            await album.save()
            track1 = Track(album=album, title="The Bird", position=1)
            track2 = Track(album=album, title="Heart don't stand a chance", position=2)
            track3 = Track(album=album, title="The Waters", position=3)
            await track1.save()
            await track2.save()
            await track3.save()

            track = await Track.objects.get(title="The Bird")
            assert track.album.pk == album.pk
            assert isinstance(track.album, ormar.Model)
            assert track.album.name is None
            await track.album.load()
            assert track.album.name == "Jamaica"

            assert len(album.tracks) == 3
            assert album.tracks[1].title == "Heart don't stand a chance"

            album1 = await Album.objects.get(name="Jamaica")
            assert album1.pk == album.pk
            assert album1.tracks == []

            await Track.objects.create(
                album={"id": track.album.pk}, title="The Bird2", position=4
            )


@pytest.mark.asyncio
async def test_select_related():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            album = Album(name="Malibu")
            await album.save()
            track1 = Track(album=album, title="The Bird", position=1)
            track2 = Track(album=album, title="Heart don't stand a chance", position=2)
            track3 = Track(album=album, title="The Waters", position=3)
            await track1.save()
            await track2.save()
            await track3.save()

            fantasies = Album(name="Fantasies")
            await fantasies.save()
            track4 = Track(album=fantasies, title="Help I'm Alive", position=1)
            track5 = Track(album=fantasies, title="Sick Muse", position=2)
            track6 = Track(album=fantasies, title="Satellite Mind", position=3)
            await track4.save()
            await track5.save()
            await track6.save()

            track = await Track.objects.select_related("album").get(title="The Bird")
            assert track.album.name == "Malibu"

            tracks = await Track.objects.select_related("album").all()
            assert len(tracks) == 6


@pytest.mark.asyncio
async def test_model_removal_from_relations():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            album = Album(name="Chichi")
            await album.save()
            track1 = Track(album=album, title="The Birdman", position=1)
            track2 = Track(album=album, title="Superman", position=2)
            track3 = Track(album=album, title="Wonder Woman", position=3)
            await track1.save()
            await track2.save()
            await track3.save()

            assert len(album.tracks) == 3
            await album.tracks.remove(track1)
            assert len(album.tracks) == 2
            assert track1.album is None

            await track1.update()
            track1 = await Track.objects.get(title="The Birdman")
            assert track1.album is None

            await album.tracks.add(track1)
            assert len(album.tracks) == 3
            assert track1.album == album

            await track1.update()
            track1 = await Track.objects.select_related("album__tracks").get(
                title="The Birdman"
            )
            album = await Album.objects.select_related("tracks").get(name="Chichi")
            assert track1.album == album

            track1.remove(album, name="album")
            assert track1.album is None
            assert len(album.tracks) == 2

            track2.remove(album, name="album")
            assert track2.album is None
            assert len(album.tracks) == 1


@pytest.mark.asyncio
async def test_fk_filter():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            malibu = Album(name="Malibu%")
            await malibu.save()
            await Track.objects.create(album=malibu, title="The Bird", position=1)
            await Track.objects.create(
                album=malibu, title="Heart don't stand a chance", position=2
            )
            await Track.objects.create(album=malibu, title="The Waters", position=3)

            fantasies = await Album.objects.create(name="Fantasies")
            await Track.objects.create(
                album=fantasies, title="Help I'm Alive", position=1
            )
            await Track.objects.create(album=fantasies, title="Sick Muse", position=2)
            await Track.objects.create(
                album=fantasies, title="Satellite Mind", position=3
            )

            tracks = (
                await Track.objects.select_related("album")
                .filter(album__name="Fantasies")
                .all()
            )
            assert len(tracks) == 3
            for track in tracks:
                assert track.album.name == "Fantasies"

            tracks = (
                await Track.objects.select_related("album")
                .filter(album__name__icontains="fan")
                .all()
            )
            assert len(tracks) == 3
            for track in tracks:
                assert track.album.name == "Fantasies"

            tracks = await Track.objects.filter(album__name__contains="Fan").all()
            assert len(tracks) == 3
            for track in tracks:
                assert track.album.name == "Fantasies"

            tracks = await Track.objects.filter(album__name__contains="Malibu%").all()
            assert len(tracks) == 3

            tracks = (
                await Track.objects.filter(album=malibu).select_related("album").all()
            )
            assert len(tracks) == 3
            for track in tracks:
                assert track.album.name == "Malibu%"

            tracks = await Track.objects.select_related("album").all(album=malibu)
            assert len(tracks) == 3
            for track in tracks:
                assert track.album.name == "Malibu%"


@pytest.mark.asyncio
async def test_multiple_fk():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            acme = await Organisation.objects.create(ident="ACME Ltd")
            red_team = await Team.objects.create(org=acme, name="Red Team")
            blue_team = await Team.objects.create(org=acme, name="Blue Team")
            await Member.objects.create(team=red_team, email="a@example.org")
            await Member.objects.create(team=red_team, email="b@example.org")
            await Member.objects.create(team=blue_team, email="c@example.org")
            await Member.objects.create(team=blue_team, email="d@example.org")

            other = await Organisation.objects.create(ident="Other ltd")
            team = await Team.objects.create(org=other, name="Green Team")
            await Member.objects.create(team=team, email="e@example.org")

            members = (
                await Member.objects.select_related("team__org")
                .filter(team__org__ident="ACME Ltd")
                .all()
            )
            assert len(members) == 4
            for member in members:
                assert member.team.org.ident == "ACME Ltd"


@pytest.mark.asyncio
async def test_pk_filter():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            fantasies = await Album.objects.create(name="Test")
            track = await Track.objects.create(
                album=fantasies, title="Test1", position=1
            )
            await Track.objects.create(album=fantasies, title="Test2", position=2)
            await Track.objects.create(album=fantasies, title="Test3", position=3)
            tracks = (
                await Track.objects.select_related("album").filter(pk=track.pk).all()
            )
            assert len(tracks) == 1

            tracks = (
                await Track.objects.select_related("album")
                .filter(position=2, album__name="Test")
                .all()
            )
            assert len(tracks) == 1


@pytest.mark.asyncio
async def test_limit_and_offset():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            fantasies = await Album.objects.create(name="Limitless")
            await Track.objects.create(
                id=None, album=fantasies, title="Sample", position=1
            )
            await Track.objects.create(album=fantasies, title="Sample2", position=2)
            await Track.objects.create(album=fantasies, title="Sample3", position=3)

            tracks = await Track.objects.limit(1).all()
            assert len(tracks) == 1
            assert tracks[0].title == "Sample"

            tracks = await Track.objects.limit(1).offset(1).all()
            assert len(tracks) == 1
            assert tracks[0].title == "Sample2"

            album = await Album.objects.select_related("tracks").limit(1).get()
            assert len(album.tracks) == 3
            assert album.tracks[0].title == "Sample"

            album = (
                await Album.objects.select_related("tracks")
                .limit(1, limit_raw_sql=True)
                .get()
            )
            assert len(album.tracks) == 1
            assert album.tracks[0].title == "Sample"


@pytest.mark.asyncio
async def test_get_exceptions():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            fantasies = await Album.objects.create(name="Test")

            with pytest.raises(NoMatch):
                await Album.objects.get(name="Test2")

            await Track.objects.create(album=fantasies, title="Test1", position=1)
            await Track.objects.create(album=fantasies, title="Test2", position=2)
            await Track.objects.create(album=fantasies, title="Test3", position=3)
            with pytest.raises(MultipleMatches):
                await Track.objects.select_related("album").get(album=fantasies)


@pytest.mark.asyncio
async def test_wrong_model_passed_as_fk():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            with pytest.raises(RelationshipInstanceError):
                org = await Organisation.objects.create(ident="ACME Ltd")
                await Track.objects.create(album=org, title="Test1", position=1)


@pytest.mark.asyncio
async def test_bulk_update_model_with_no_children():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            album = await Album.objects.create(name="Test")
            album.name = "Test2"
            await Album.objects.bulk_update([album], columns=["name"])

            updated_album = await Album.objects.get(id=album.id)
            assert updated_album.name == "Test2"


@pytest.mark.asyncio
async def test_bulk_update_model_with_children():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            best_seller = await Album.objects.create(name="to_be_best_seller")
            best_seller2 = await Album.objects.create(name="to_be_best_seller2")
            not_best_seller = await Album.objects.create(name="unpopular")
            await Track.objects.create(
                album=best_seller, title="t1", position=1, play_count=100
            )
            await Track.objects.create(
                album=best_seller2, title="t2", position=1, play_count=100
            )
            await Track.objects.create(
                album=not_best_seller, title="t3", position=1, play_count=3
            )
            await Track.objects.create(
                album=best_seller, title="t4", position=1, play_count=500
            )

            tracks = (
                await Track.objects.select_related("album")
                .filter(play_count__gt=10)
                .all()
            )
            best_seller_albums = {}
            for track in tracks:
                album = track.album
                if album.id in best_seller_albums:
                    continue
                album.is_best_seller = True
                best_seller_albums[album.id] = album
            await Album.objects.bulk_update(
                best_seller_albums.values(), columns=["is_best_seller"]
            )
            best_seller_albums_db = await Album.objects.filter(
                is_best_seller=True
            ).all()
            assert len(best_seller_albums_db) == 2


@pytest.mark.asyncio
async def test_fk_update_to_none():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            album = await Album.objects.create(name="Limitless")
            track = await Track.objects.create(album=album, title="Test1", position=1)
            assert track.album.name == "Limitless"

            track.album = None
            assert track.album is None

            updated_track = await track.update(_columns=["album"])
            assert updated_track.album is None

            reloaded = await Track.objects.get(id=track.id)
            assert reloaded.album is None


@pytest.mark.asyncio
async def test_setting_fk_to_none_removes_child_from_parent_relation():
    async with base_ormar_config.database:
        async with base_ormar_config.database.transaction(force_rollback=True):
            album = await Album(name="Chichi").save()
            track = await Track(
                album=album, title="The Birdman", position=1
            ).save()

            assert len(album.tracks) == 1
            assert track in album.tracks

            track.album = None
            await track.update()

            assert track.album is None
            assert track not in album.tracks
            assert len(album.tracks) == 0
