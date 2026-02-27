import base64
import datetime
import decimal
import uuid
from typing import Any, Callable, Optional, Union

import pydantic
from pydantic_core import SchemaValidator, core_schema

try:
    import orjson as json
except ImportError:  # pragma: no cover
    import json  # type: ignore

from ormar.utils.rust_utils import HAS_RUST, ormar_rust_utils

if HAS_RUST:
    _rs_encode_bytes = ormar_rust_utils.encode_bytes
    _rs_decode_bytes = ormar_rust_utils.decode_bytes
    _rs_encode_json = ormar_rust_utils.encode_json


def parse_bool(value: str) -> bool:
    return value == "true"


def encode_bool(value: bool) -> str:
    return "true" if value else "false"


def encode_decimal(value: decimal.Decimal, precision: Optional[int] = None) -> float:
    return (
        round(float(value), precision) if isinstance(value, decimal.Decimal) else value
    )


def _py_encode_bytes(
    value: Union[str, bytes], represent_as_string: bool = False
) -> str:
    if represent_as_string:
        value = (
            value if isinstance(value, str) else base64.b64encode(value).decode("utf-8")
        )
    else:
        value = value if isinstance(value, str) else value.decode("utf-8")
    return value


def _py_decode_bytes(value: str, represent_as_string: bool = False) -> bytes:
    if represent_as_string:
        return value if isinstance(value, bytes) else base64.b64decode(value)
    return value if isinstance(value, bytes) else value.encode("utf-8")


def _py_encode_json(value: Any) -> Optional[str]:
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        value = value.isoformat()
    value = json.dumps(value) if not isinstance(value, str) else re_dump_value(value)
    value = value.decode("utf-8") if isinstance(value, bytes) else value
    return value


def re_dump_value(value: str) -> Union[str, bytes]:
    """
    Re-dumps value due to different string representation in orjson and json
    :param value: string to re-dump
    :type value: str
    :return: re-dumped value
    :rtype: list[str]
    """
    try:
        result: Union[str, bytes] = json.dumps(json.loads(value))
    except json.JSONDecodeError:
        result = value
    return result


encode_bytes = _rs_encode_bytes if HAS_RUST else _py_encode_bytes
decode_bytes = _rs_decode_bytes if HAS_RUST else _py_decode_bytes
encode_json = _rs_encode_json if HAS_RUST else _py_encode_json


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
