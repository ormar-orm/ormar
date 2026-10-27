import datetime
import decimal
import json as std_json
import uuid
from typing import Any, Callable, Optional

import orjson as json
import ormar_rust_utils
import pydantic
from pydantic_core import SchemaValidator, core_schema


def parse_bool(value: str) -> bool:
    return value == "true"


def encode_bool(value: bool) -> str:
    return "true" if value else "false"


def encode_decimal(value: decimal.Decimal, precision: Optional[int] = None) -> float:
    return (
        round(float(value), precision) if isinstance(value, decimal.Decimal) else value
    )


encode_bytes = ormar_rust_utils.encode_bytes
decode_bytes = ormar_rust_utils.decode_bytes
encode_json = ormar_rust_utils.encode_json


def serialize_json_column(value: Any) -> str:
    """
    Serializes a value of a JSON column for the database with orjson.

    Values orjson cannot serialize (integers over 64 bits) fall back to the
    standard library json module.

    :param value: value of the JSON column
    :type value: Any
    :return: JSON text of the value
    :rtype: str
    """
    try:
        return json.dumps(value, option=json.OPT_NON_STR_KEYS).decode()
    except TypeError:
        return std_json.dumps(value)


def deserialize_json_column(value: str) -> Any:
    """
    Deserializes JSON text of a database column with orjson.

    Text orjson rejects (``NaN``, lone surrogates) falls back to the standard
    library json module.

    :param value: JSON text read from the database
    :type value: str
    :return: deserialized value
    :rtype: Any
    """
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return std_json.loads(value)


ENCODERS_MAP: dict[type, Callable] = {
    datetime.datetime: lambda x: x.isoformat(),
    datetime.date: lambda x: x.isoformat(),
    datetime.time: lambda x: x.isoformat(),
    pydantic.Json: encode_json,
    decimal.Decimal: encode_decimal,
    uuid.UUID: str,
    bytes: encode_bytes,
}

SQL_ENCODERS_MAP: dict[type, Callable] = {bool: encode_bool, **ENCODERS_MAP}

ADDITIONAL_PARAMETERS_MAP: dict[type, str] = {
    bytes: "represent_as_base64_str",
    decimal.Decimal: "decimal_places",
}


DECODERS_MAP: dict[type, Callable] = {
    bool: parse_bool,
    datetime.datetime: SchemaValidator(core_schema.datetime_schema()).validate_python,
    datetime.date: SchemaValidator(core_schema.date_schema()).validate_python,
    datetime.time: SchemaValidator(core_schema.time_schema()).validate_python,
    pydantic.Json: json.loads,
    decimal.Decimal: lambda x, precision: decimal.Decimal(
        x, context=decimal.Context(prec=precision)
    ),
    bytes: decode_bytes,
}
