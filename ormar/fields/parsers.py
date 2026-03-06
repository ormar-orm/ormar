import datetime
import decimal
import uuid
from typing import Callable, Optional

import ormar_rust_utils
import pydantic
from pydantic_core import SchemaValidator, core_schema

try:
    import orjson as json
except ImportError:  # pragma: no cover
    import json  # type: ignore


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
