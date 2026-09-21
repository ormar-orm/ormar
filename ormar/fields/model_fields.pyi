from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal as DecimalType
from enum import Enum as EnumBase
from typing import Any, Literal, TypeVar, Union, overload
from uuid import UUID as UuidType

T = TypeVar("T", bound=EnumBase)
L = TypeVar("L", bound=str)

@overload
def Boolean(*, nullable: Literal[False] = False, **kwargs: Any) -> bool: ...
@overload
def Boolean(*, nullable: Literal[True], **kwargs: Any) -> bool | None: ...
@overload
def String(
    *,
    max_length: int,
    choices: list[L] | tuple[L, ...],
    nullable: Literal[False] = False,
    **kwargs: Any,
) -> L: ...
@overload
def String(
    *,
    max_length: int,
    choices: list[L] | tuple[L, ...],
    nullable: Literal[True],
    **kwargs: Any,
) -> L | None: ...
@overload
def String(
    *, max_length: int, nullable: Literal[False] = False, **kwargs: Any
) -> str: ...
@overload
def String(
    *, max_length: int, nullable: Literal[True], **kwargs: Any
) -> str | None: ...
@overload
def Integer(*, nullable: Literal[False] = False, **kwargs: Any) -> int: ...
@overload
def Integer(*, nullable: Literal[True], **kwargs: Any) -> int | None: ...
@overload
def Text(*, nullable: Literal[False] = False, **kwargs: Any) -> str: ...
@overload
def Text(*, nullable: Literal[True], **kwargs: Any) -> str | None: ...
@overload
def Float(*, nullable: Literal[False] = False, **kwargs: Any) -> float: ...
@overload
def Float(*, nullable: Literal[True], **kwargs: Any) -> float | None: ...
@overload
def DateTime(*, nullable: Literal[False] = False, **kwargs: Any) -> datetime: ...
@overload
def DateTime(*, nullable: Literal[True], **kwargs: Any) -> datetime | None: ...
@overload
def Date(*, nullable: Literal[False] = False, **kwargs: Any) -> date: ...
@overload
def Date(*, nullable: Literal[True], **kwargs: Any) -> date | None: ...
@overload
def Time(*, nullable: Literal[False] = False, **kwargs: Any) -> time: ...
@overload
def Time(*, nullable: Literal[True], **kwargs: Any) -> time | None: ...
@overload
def JSON(*, nullable: Literal[False] = False, **kwargs: Any) -> Any: ...
@overload
def JSON(*, nullable: Literal[True], **kwargs: Any) -> Any | None: ...
@overload
def BigInteger(*, nullable: Literal[False] = False, **kwargs: Any) -> int: ...
@overload
def BigInteger(*, nullable: Literal[True], **kwargs: Any) -> int | None: ...
@overload
def SmallInteger(*, nullable: Literal[False] = False, **kwargs: Any) -> int: ...
@overload
def SmallInteger(*, nullable: Literal[True], **kwargs: Any) -> int | None: ...
@overload
def Decimal(
    *,
    max_digits: int,
    decimal_places: int,
    nullable: Literal[False] = False,
    **kwargs: Any,
) -> DecimalType: ...
@overload
def Decimal(
    *, max_digits: int, decimal_places: int, nullable: Literal[True], **kwargs: Any
) -> DecimalType | None: ...
@overload
def UUID(*, nullable: Literal[False] = False, **kwargs: Any) -> UuidType: ...
@overload
def UUID(*, nullable: Literal[True], **kwargs: Any) -> UuidType | None: ...
@overload
def LargeBinary(
    max_length: int, *, represent_as_base64_str: Literal[True], **kwargs: Any
) -> str: ...
@overload
def LargeBinary(
    max_length: int, *, represent_as_base64_str: Literal[False], **kwargs: Any
) -> bytes: ...
@overload
def LargeBinary(
    max_length: int, represent_as_base64_str: Literal[False] = ..., **kwargs: Any
) -> bytes: ...
@overload
def LargeBinary(
    max_length: int, represent_as_base64_str: bool = False, **kwargs: Any
) -> Union[str, bytes]: ...
@overload
def Enum(
    enum_class: type[T],
    *,
    nullable: Literal[False] = False,
    **kwargs: Any,
) -> T: ...
@overload
def Enum(
    enum_class: type[T],
    *,
    nullable: Literal[True],
    **kwargs: Any,
) -> T | None: ...
