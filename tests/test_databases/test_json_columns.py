import json
from typing import Optional

import pytest
from sqlalchemy import text

import ormar
from ormar.databases.connection import DatabaseConnection
from ormar.fields.parsers import deserialize_json_column, serialize_json_column
from tests.lifespan import init_tests
from tests.settings import ASYNC_DATABASE_URL, create_config

base_ormar_config = create_config()


class Document(ormar.Model):
    ormar_config = base_ormar_config.copy(tablename="json_documents")

    id: int = ormar.Integer(primary_key=True)
    data: Optional[dict] = ormar.JSON(nullable=True)


create_test_database = init_tests(base_ormar_config)


def test_serialize_json_column_is_compact_and_keeps_non_ascii():
    assert serialize_json_column(value={"a": [1, 2], "b": "zażółć"}) == (
        '{"a":[1,2],"b":"zażółć"}'
    )


def test_serialize_json_column_converts_non_string_keys():
    assert serialize_json_column(value={1: "a"}) == '{"1":"a"}'


def test_serialize_json_column_falls_back_for_big_integers():
    assert serialize_json_column(value={"a": 2**70}) == json.dumps({"a": 2**70})


def test_deserialize_json_column():
    assert deserialize_json_column(value='{"a": [1, 2]}') == {"a": [1, 2]}


@pytest.mark.parametrize("value", ['{"a": NaN}', '{"a": "\\ud800"}'])
def test_deserialize_json_column_falls_back_for_rejected_text(value):
    assert repr(deserialize_json_column(value=value)) == repr(json.loads(value))


@pytest.mark.asyncio
async def test_json_codecs_default_to_orjson():
    database = DatabaseConnection(url=ASYNC_DATABASE_URL, min_connections=0)
    await database.connect()
    try:
        assert database.engine.dialect._json_serializer is serialize_json_column
        assert database.engine.dialect._json_deserializer is deserialize_json_column
    finally:
        await database.disconnect()


@pytest.mark.asyncio
async def test_json_codecs_can_be_overridden():
    database = DatabaseConnection(
        url=ASYNC_DATABASE_URL,
        min_connections=0,
        json_serializer=json.dumps,
        json_deserializer=json.loads,
    )
    await database.connect()
    try:
        assert database.engine.dialect._json_serializer is json.dumps
        assert database.engine.dialect._json_deserializer is json.loads
    finally:
        await database.disconnect()


@pytest.mark.asyncio
async def test_json_column_round_trip():
    async with base_ormar_config.database:
        data = {"a": [1, {"b": None}], "c": "zażółć", "d": 1.5, "e": True}
        document = await Document.objects.create(data=data)
        loaded = await Document.objects.get(id=document.id)
        assert loaded.data == data
        assert await Document.objects.values_list("data", flatten=True) == [data]
        async with base_ormar_config.database.connection() as connection:
            stored = await connection.execute(text("SELECT data FROM json_documents"))
        assert "zażółć" in str(stored.scalar())
