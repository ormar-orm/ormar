import typing
from typing import Any

import pydantic


def relation_schema(schema: type[pydantic.BaseModel], field: str) -> Any:
    """
    Returns the nested pydantic model of a relation field of a generated
    model, unwrapping ``Optional`` and ``list``.

    :param schema: generated pydantic model
    :type schema: type[pydantic.BaseModel]
    :param field: relation field name
    :type field: str
    :return: nested pydantic model
    :rtype: Any
    """
    pending = [schema.model_fields[field].annotation]
    while pending:
        current = pending.pop()
        if isinstance(current, type) and issubclass(current, pydantic.BaseModel):
            return current
        pending.extend(typing.get_args(current))
    raise AssertionError(f"{field} is not a nested model")  # pragma: no cover
