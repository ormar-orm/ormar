import typing
from typing import Any

import pydantic
import sqlalchemy
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import ExecutableDDLElement
from sqlalchemy.sql.compiler import DDLCompiler


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


class AddColumn(ExecutableDDLElement):
    """``ALTER TABLE ... ADD COLUMN`` of a column, as a migration tool emits it."""

    def __init__(self, table_name: str, column: sqlalchemy.Column) -> None:
        self.table_name = table_name
        self.column = column


@compiles(AddColumn)
def compile_add_column(
    element: AddColumn, compiler: DDLCompiler, **kwargs: object
) -> str:
    """Render the column specification with the dialect's DDL compiler."""
    column_spec = compiler.get_column_specification(element.column)
    return f"ALTER TABLE {element.table_name} ADD COLUMN {column_spec}"
