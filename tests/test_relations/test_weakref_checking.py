import gc

import ormar
from tests.settings import create_config

base_ormar_config = create_config()
from tests.lifespan import init_tests


class Band(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="bands")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)


class Artist(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="artists")

    id: int = ormar.Integer(primary_key=True)
    name: str = ormar.String(max_length=100)

    band: Band = ormar.ForeignKey(Band)


create_test_database = init_tests(base_ormar_config)


def test_weakref_init():
    band = Band(name="Band")
    artist1 = Artist(name="Artist 1", band=band)
    artist2 = Artist(name="Artist 2", band=band)
    artist3 = Artist(name="Artist 3", band=band)

    del artist1
    Artist(
        name="Artist 2", band=band
    )  # Force it to check for weakly-referenced objects
    del artist3

    band.artists  # Force it to clean

    assert len(band.artists) == 1
    assert band.artists[0].name == artist2.name


def test_json_serialization_skips_collected_reverse_children():
    band = Band(id=1, name="Band")
    Artist(id=1, name="Artist 1", band=band)
    Artist(id=2, name="Artist 2", band=band)
    gc.collect()

    dumped = Band.__pydantic_serializer__.to_python(band, mode="json")

    assert dumped == {"id": 1, "name": "Band", "artists": None}
