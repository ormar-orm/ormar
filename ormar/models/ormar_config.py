import re
from typing import TYPE_CHECKING, Callable, Optional, Union

import sqlalchemy
from pydantic_core import PydanticUndefined, PydanticUndefinedType
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql.schema import ColumnCollectionConstraint, Index

from ormar.databases.connection import DatabaseConnection
from ormar.fields import BaseField, ForeignKeyField, ManyToManyField
from ormar.models.primary_key import PrimaryKey
from ormar.models.utils import Extra
from ormar.queryset.queryset import QuerySet
from ormar.signals import SignalEmitter

# IndexColumns subclasses sqlalchemy Index, which is not a
# ColumnCollectionConstraint, but Table accepts both
TableConstraint = Union[ColumnCollectionConstraint, Index]

WORD_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def default_tablename(class_name: str) -> str:
    """
    Default table name factory: lowercase class name plus 's'.

    :param class_name: name of the model class
    :type class_name: str
    :return: table name, i.e. ``UserProfile`` -> ``userprofiles``
    :rtype: str
    """
    return class_name.lower() + "s"


def snake_case_tablename(class_name: str) -> str:
    """
    Table name factory: snake case class name plus 's'.

    Acronyms are kept together, so ``HTTPRequest`` becomes ``http_requests``.

    :param class_name: name of the model class
    :type class_name: str
    :return: table name, i.e. ``UserProfile`` -> ``user_profiles``
    :rtype: str
    """
    return default_tablename(class_name=WORD_BOUNDARY.sub("_", class_name))


class OrmarConfig:
    if TYPE_CHECKING:  # pragma: no cover
        pk: PrimaryKey
        metadata: sqlalchemy.MetaData
        database: DatabaseConnection
        engine: AsyncEngine
        tablename: str
        tablename_factory: Callable[[str], str]
        schema: Optional[str]
        comment: Optional[str]
        order_by: list[str]
        abstract: bool
        view: bool
        proxy: bool
        emit_parent_signals: bool
        exclude_parent_fields: list[str]
        constraints: list[TableConstraint]
        partial_updates: Optional[bool]
        not_null_defaults: Optional[bool]

    def __init__(
        self,
        metadata: Optional[sqlalchemy.MetaData] = None,
        database: Optional[DatabaseConnection] = None,
        engine: Optional[AsyncEngine] = None,
        tablename: Optional[str] = None,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        order_by: Optional[list[str]] = None,
        abstract: bool = False,
        proxy: bool = False,
        emit_parent_signals: bool = False,
        queryset_class: type[QuerySet] = QuerySet,
        extra: Extra = Extra.forbid,
        constraints: Optional[list[TableConstraint]] = None,
        view: bool = False,
        partial_updates: Optional[bool] = None,
        tablename_factory: Optional[Callable[[str], str]] = None,
        not_null_defaults: Optional[bool] = None,
    ) -> None:
        self.pk = None  # type: ignore
        self.metadata = metadata  # type: ignore
        self.database = database  # type: ignore
        self.engine = engine  # type: ignore
        self.tablename = tablename  # type: ignore
        self.tablename_factory = tablename_factory or default_tablename
        self.schema = schema
        self.comment = comment
        self.orders_by = order_by or []
        self.columns: list[sqlalchemy.Column] = []
        self.constraints = constraints or []
        self.model_fields: dict[
            str, Union[BaseField, ForeignKeyField, ManyToManyField]
        ] = {}
        self.property_fields: set = set()
        self.signals: SignalEmitter = SignalEmitter()
        self.abstract = abstract
        self.view = view
        self.proxy = proxy
        self.emit_parent_signals = emit_parent_signals
        self.requires_ref_update: bool = False
        self.extra = extra
        self.queryset_class = queryset_class
        self.table: sqlalchemy.Table = None  # type: ignore
        self.partial_updates = partial_updates
        self.not_null_defaults = not_null_defaults

    def copy(
        self,
        metadata: Optional[sqlalchemy.MetaData] = None,
        database: Optional[DatabaseConnection] = None,
        engine: Optional[AsyncEngine] = None,
        tablename: Optional[str] = None,
        schema: Union[str, None, PydanticUndefinedType] = PydanticUndefined,
        comment: Union[str, None, PydanticUndefinedType] = PydanticUndefined,
        order_by: Optional[list[str]] = None,
        abstract: Optional[bool] = None,
        proxy: Optional[bool] = None,
        emit_parent_signals: Optional[bool] = None,
        queryset_class: Optional[type[QuerySet]] = None,
        extra: Optional[Extra] = None,
        constraints: Optional[list[TableConstraint]] = None,
        view: Optional[bool] = None,
        partial_updates: Union[bool, None, PydanticUndefinedType] = PydanticUndefined,
        tablename_factory: Optional[Callable[[str], str]] = None,
        not_null_defaults: Optional[bool] = None,
    ) -> "OrmarConfig":
        # PydanticUndefined distinguishes "omitted" (inherit parent schema)
        # from an explicit ``schema=None`` (clear schema).
        resolved_schema = (
            self.schema if isinstance(schema, PydanticUndefinedType) else schema
        )
        resolved_comment = (
            self.comment if isinstance(comment, PydanticUndefinedType) else comment
        )
        return OrmarConfig(
            metadata=metadata or self.metadata,
            database=database or self.database,
            engine=engine or self.engine,
            tablename=tablename,
            schema=resolved_schema,
            comment=resolved_comment,
            order_by=order_by,
            abstract=abstract or self.abstract,
            proxy=proxy if proxy is not None else self.proxy,
            emit_parent_signals=(
                emit_parent_signals
                if emit_parent_signals is not None
                else self.emit_parent_signals
            ),
            queryset_class=queryset_class or self.queryset_class,
            extra=extra or self.extra,
            constraints=constraints,
            view=view if view is not None else self.view,
            partial_updates=(
                self.partial_updates
                if isinstance(partial_updates, PydanticUndefinedType)
                else partial_updates
            ),
            tablename_factory=tablename_factory or self.tablename_factory,
            not_null_defaults=(
                not_null_defaults
                if not_null_defaults is not None
                else self.not_null_defaults
            ),
        )

    def resolve_tablename(self, class_name: str) -> str:
        """
        Returns the explicit tablename or the one built by tablename_factory.

        :param class_name: name of the model class
        :type class_name: str
        :return: table name of the model
        :rtype: str
        """
        return self.tablename or self.tablename_factory(class_name)
