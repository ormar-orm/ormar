import json
from typing import ForwardRef, Optional, get_args, get_origin

import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

import ormar
from tests.lifespan import init_tests, lifespan
from tests.settings import create_config

base_ormar_config = create_config()
app = FastAPI(lifespan=lifespan(base_ormar_config))


class Album(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nocopy_albums")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Track(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nocopy_tracks")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    album: Optional[Album] = ormar.ForeignKey(Album, related_name="tracks")


class Node(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nocopy_nodes")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)
    parent = ormar.ForeignKey(ForwardRef("Node"), related_name="children")


Node.update_forward_refs()


class Tag(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nocopy_tags")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Post(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="nocopy_posts")

    id: int = ormar.Integer(primary_key=True)
    title: str = ormar.String(max_length=100)
    tags: Optional[list[Tag]] = ormar.ManyToMany(Tag, related_name="posts")


create_test_database = init_tests(base_ormar_config)


@pytest_asyncio.fixture(scope="function", autouse=True)
async def cleanup():
    yield
    async with base_ormar_config.database:
        await Track.objects.delete(each=True)
        await Album.objects.delete(each=True)
        await Node.objects.filter(parent__isnull=False).delete()
        await Node.objects.delete(each=True)
        await Post.ormar_config.model_fields["tags"].through.objects.delete(each=True)
        await Post.objects.delete(each=True)
        await Tag.objects.delete(each=True)


@app.get("/albums/{album_id}", response_model=Album)
async def get_album(album_id: int) -> Album:
    return await Album.objects.select_related("tracks").get(id=album_id)


@app.get("/nodes/{node_id}", response_model=Node)
async def get_node(node_id: int) -> Node:
    return await Node.objects.select_related(["children", "parent"]).get(id=node_id)


def all_ormar_models() -> set[type]:
    """
    Collect every ``ormar.Model`` subclass declared so far.

    :return: set of all ormar model classes
    :rtype: set[type]
    """
    found: set[type] = set()
    pending = list(ormar.Model.__subclasses__())
    while pending:
        model = pending.pop()
        if model not in found:
            found.add(model)
            pending.extend(model.__subclasses__())
    return found


def test_reverse_relations_do_not_create_model_copies():
    config = create_config()
    before = all_ormar_models()

    class Owner(ormar.Model):
        ormar_config = config.copy(tablename="nocopy_owners")

        id: int = ormar.Integer(primary_key=True)

    class Pet(ormar.Model):
        ormar_config = config.copy(tablename="nocopy_pets")

        id: int = ormar.Integer(primary_key=True)
        owner: Optional[Owner] = ormar.ForeignKey(Owner, related_name="pets")

    class Toy(ormar.Model):
        ormar_config = config.copy(tablename="nocopy_toys")

        id: int = ormar.Integer(primary_key=True)
        owner: Optional[Owner] = ormar.ForeignKey(Owner, related_name="toys")
        pet: Optional[Pet] = ormar.ForeignKey(Pet, related_name="toys")

    assert all_ormar_models() - before == {Owner, Pet, Toy}
    assert Pet in Owner.model_fields["pets"].annotation.__args__
    assert Toy in Owner.model_fields["toys"].annotation.__args__
    assert Toy in Pet.model_fields["toys"].annotation.__args__


def test_reverse_field_validates_and_serializes():
    album = Album(
        id=1, name="Album", tracks=[{"id": 1, "title": "One"}, Track(id=2, title="Two")]
    )
    assert all(isinstance(track, Track) for track in album.tracks)
    assert all(track.album == album for track in album.tracks)
    tracks = [{"id": 1, "title": "One"}, {"id": 2, "title": "Two"}]
    assert album.model_dump() == {"id": 1, "name": "Album", "tracks": tracks}
    assert json.loads(album.model_dump_json()) == album.model_dump()
    assert album.tracks[0].model_dump() == {
        "id": 1,
        "title": "One",
        "album": {"id": 1, "name": "Album"},
    }
    assert album.model_dump(exclude={"tracks"}) == {"id": 1, "name": "Album"}
    assert album.model_dump(exclude={"tracks": {"__all__": {"title"}}})["tracks"] == [
        {"id": 1},
        {"id": 2},
    ]
    with pytest.raises(ValidationError):
        Album(name="Album", tracks=[{"id": "not-an-int", "title": "One"}])


def test_self_reference_serializes_without_recursion():
    root = Node(id=1, name="root")
    child = Node(id=2, name="child", parent=root)
    assert root.model_dump() == {
        "id": 1,
        "name": "root",
        "parent": None,
        "children": [{"id": 2, "name": "child"}],
    }
    assert child.model_dump() == {
        "id": 2,
        "name": "child",
        "parent": {"id": 1, "name": "root"},
        "children": [],
    }
    assert json.loads(root.model_dump_json()) == root.model_dump()


def test_get_pydantic_keeps_reverse_relations():
    album_pydantic = Album.get_pydantic()
    tracks_annotation = get_args(album_pydantic.model_fields["tracks"].annotation)[0]
    assert get_origin(tracks_annotation) is list
    assert set(get_args(tracks_annotation)[0].model_fields) == {"id", "title"}
    assert "tracks" not in Album.get_pydantic(exclude={"tracks"}).model_fields
    node_pydantic = Node.get_pydantic()
    assert set(node_pydantic.model_fields) == {"id", "name", "parent", "children"}


def test_openapi_schema_with_reverse_relations():
    schemas = app.openapi()["components"]["schemas"]
    assert {"Album", "Track", "Node"} <= set(schemas)
    assert "tracks" in schemas["Album"]["properties"]
    assert "children" in schemas["Node"]["properties"]


@pytest.mark.asyncio
async def test_fastapi_responses_with_reverse_relations():
    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://testserver")
    async with client as client, LifespanManager(app):
        album = await Album.objects.create(name="Album")
        track = await Track.objects.create(title="One", album=album)
        root = await Node.objects.create(name="root")
        await Node.objects.create(name="child", parent=root)

        response = await client.get(f"/albums/{album.id}")
        assert response.json() == {
            "id": album.id,
            "name": "Album",
            "tracks": [
                {
                    "id": track.id,
                    "title": "One",
                    "album": {"id": album.id, "name": "Album"},
                }
            ],
        }
        response = await client.get(f"/nodes/{root.id}")
        assert response.json()["children"][0]["name"] == "child"
        assert response.json()["children"][0]["parent"]["id"] == root.id


@pytest.mark.asyncio
async def test_many_to_many_reverse_relation_serializes():
    async with base_ormar_config.database:
        post = await Post.objects.create(title="Post")
        tag = await Tag.objects.create(name="Tag")
        await post.tags.add(tag)
        loaded = await Tag.objects.select_related("posts").get()
        dumped = loaded.model_dump(exclude={"posts": {"posttag"}})
        assert dumped == {
            "id": tag.id,
            "name": "Tag",
            "posts": [{"id": post.id, "title": "Post"}],
        }
        assert json.loads(loaded.model_dump_json()) == loaded.model_dump()
