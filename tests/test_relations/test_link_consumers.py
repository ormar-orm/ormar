from typing import Optional

import pytest
import pytest_asyncio

import ormar
from tests.lifespan import init_tests
from tests.settings import create_config

base_ormar_config = create_config()


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lc_albums")

    id: int = ormar.Integer(primary_key=True, name="album_pk")
    name: str = ormar.String(max_length=50)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lc_tracks")

    id: int = ormar.Integer(primary_key=True, name="track_pk")
    title: str = ormar.String(max_length=50)
    album: Optional[Album] = ormar.ForeignKey(
        Album, name="album_ref", related_name="tracks"
    )


class Category(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lc_categories")

    id: int = ormar.Integer(primary_key=True, name="category_pk")
    name: str = ormar.String(max_length=50)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="lc_posts")

    id: int = ormar.Integer(primary_key=True, name="post_pk")
    title: str = ormar.String(max_length=50)
    categories = ormar.ManyToMany(Category)


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture()
async def connected():
    async with base_ormar_config.database:
        yield


def compiled(queryset) -> str:
    return str(queryset.build_select_expression())


EXPECTED_JOINS = {
    "track_album": (
        "FROM lc_tracks LEFT OUTER JOIN lc_albums AS t1b26c7_lc_albums "
        "ON t1b26c7_lc_albums.album_pk=lc_tracks.album_ref"
    ),
    "album_tracks": (
        "FROM lc_albums LEFT OUTER JOIN lc_tracks AS tf5ec81_lc_tracks "
        "ON tf5ec81_lc_tracks.album_ref=lc_albums.album_pk"
    ),
    "post_categories": (
        "FROM lc_posts LEFT OUTER JOIN posts_categorys AS tb6d45f_posts_categorys "
        "ON tb6d45f_posts_categorys.post=lc_posts.post_pk "
        "LEFT OUTER JOIN lc_categories AS ta01dee_lc_categories "
        "ON ta01dee_lc_categories.category_pk=tb6d45f_posts_categorys.category"
    ),
    "category_posts_categories": (
        "FROM lc_categories LEFT OUTER JOIN posts_categorys AS t947735_posts_categorys "
        "ON t947735_posts_categorys.category=lc_categories.category_pk "
        "LEFT OUTER JOIN lc_posts AS t5d13af_lc_posts "
        "ON t5d13af_lc_posts.post_pk=t947735_posts_categorys.post "
        "LEFT OUTER JOIN posts_categorys AS t07e726_posts_categorys "
        "ON t07e726_posts_categorys.post=t5d13af_lc_posts.post_pk "
        "LEFT OUTER JOIN lc_categories AS tf4ceba_lc_categories "
        "ON tf4ceba_lc_categories.category_pk=t07e726_posts_categorys.category"
    ),
}


@pytest.mark.asyncio
async def test_join_sql_unchanged(connected):
    queries = {
        "track_album": Track.objects.select_related("album"),
        "album_tracks": Album.objects.select_related("tracks"),
        "post_categories": Post.objects.select_related("categories"),
        "category_posts_categories": Category.objects.select_related(
            "posts__categories"
        ),
    }
    for name, queryset in queries.items():
        assert EXPECTED_JOINS[name] in compiled(queryset).replace("\n", " ")


@pytest.mark.asyncio
async def test_aliased_fk_join_and_prefetch(connected):
    album = await Album.objects.create(name="a")
    await Track.objects.create(album=album, title="t1")
    await Track.objects.create(album=album, title="t2")
    joined = await Album.objects.select_related("tracks").get()
    fetched = await Album.objects.prefetch_related("tracks").get()
    assert [t.title for t in joined.tracks] == ["t1", "t2"]
    assert [t.title for t in fetched.tracks] == ["t1", "t2"]
    assert len(await album.tracks.all()) == 2
    tracks = await Track.objects.prefetch_related("album").all()
    assert {t.album.name for t in tracks} == {"a"}


@pytest.mark.asyncio
async def test_m2m_proxy_add_and_prefetch_through_link(connected):
    post = await Post.objects.create(title="p")
    category = await Category.objects.create(name="c")
    await post.categories.add(category)
    assert [c.name for c in await post.categories.all()] == ["c"]
    loaded = await Post.objects.prefetch_related("categories").get(pk=post.pk)
    assert [c.name for c in loaded.categories] == ["c"]
    reverse = await Category.objects.prefetch_related("posts").get(pk=category.pk)
    assert [p.title for p in reverse.posts] == ["p"]
    await post.categories.remove(category)
    assert await post.categories.count() == 0
