"""
The `ormar` package is an async mini ORM for Python, with support for **Postgres,
MySQL**, and **SQLite**.

The main benefit of using `ormar` are:

*  getting an **async ORM that can be used with async frameworks**
(fastapi, starlette etc.)
*  getting just **one model to maintain** - you don't have to maintain pydantic
and other orm model (sqlalchemy, peewee, gino etc.)

The goal was to create a simple ORM that can be **used directly
(as request and response models)
with `fastapi`** that bases it's data validation on pydantic.

Ormar - apart form obvious ORM in name - get it's name from ormar in swedish which means
snakes, and ormar(e) in italian which means cabinet.

And what's a better name for python ORM than snakes cabinet :)

"""

from ormar.protocols import QuerySetProtocol, RelationProtocol  # noqa: I001
from importlib.metadata import version

from ormar.decorators import (  # noqa: I100
    post_bulk_update,
    post_delete,
    post_relation_add,
    post_relation_remove,
    post_save,
    post_update,
    pre_delete,
    pre_relation_add,
    pre_relation_remove,
    pre_save,
    pre_update,
)
from ormar.exceptions import (  # noqa: I100
    ModelDefinitionError,
    MultipleMatches,
    NoMatch,
)
from ormar.fields import (
    DECODERS_MAP,
    ENCODERS_MAP,
    JSON,
    SQL_ENCODERS_MAP,
    UUID,
    BaseField,
    BigInteger,
    Boolean,
    CheckColumns,
    Date,
    DateTime,
    Decimal,
    EncryptBackends,
    Enum,
    Float,
    ForeignKey,
    ForeignKeyField,
    IndexColumns,
    Integer,
    LargeBinary,
    ManyToMany,
    ManyToManyField,
    ReferentialAction,
    SmallInteger,
    String,
    Text,
    Time,
    UniqueColumns,
)

# noqa: I100
from ormar.databases.connection import DatabaseConnection
from ormar.models import ExcludableItems, Extra, Model, OrmarConfig
from ormar.queryset import NullsOrdering, OrderAction, QuerySet, and_, or_
from ormar.queryset.aggregations import Avg, Count, Max, Min, Sum
from ormar.relations import RelationType
from ormar.signals import Signal


class UndefinedType:  # pragma no cover
    def __repr__(self) -> str:
        return "OrmarUndefined"


Undefined = UndefinedType()

__version__ = version("ormar")
__all__ = [
    "Integer",
    "BigInteger",
    "SmallInteger",
    "Boolean",
    "Time",
    "Text",
    "String",
    "JSON",
    "DateTime",
    "Date",
    "Decimal",
    "Enum",
    "Float",
    "ManyToMany",
    "Model",
    "ModelDefinitionError",
    "MultipleMatches",
    "NoMatch",
    "ForeignKey",
    "QuerySet",
    "Count",
    "Sum",
    "Avg",
    "Min",
    "Max",
    "RelationType",
    "Undefined",
    "UUID",
    "UniqueColumns",
    "IndexColumns",
    "CheckColumns",
    "ReferentialAction",
    "QuerySetProtocol",
    "RelationProtocol",
    "post_bulk_update",
    "post_delete",
    "post_save",
    "post_update",
    "post_relation_add",
    "post_relation_remove",
    "pre_delete",
    "pre_save",
    "pre_update",
    "pre_relation_remove",
    "pre_relation_add",
    "Signal",
    "BaseField",
    "ManyToManyField",
    "ForeignKeyField",
    "OrderAction",
    "NullsOrdering",
    "ExcludableItems",
    "and_",
    "or_",
    "EncryptBackends",
    "ENCODERS_MAP",
    "SQL_ENCODERS_MAP",
    "DECODERS_MAP",
    "LargeBinary",
    "Extra",
    "OrmarConfig",
    "DatabaseConnection",
]
