from typing import TYPE_CHECKING, Optional, Union

import sqlalchemy
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql.schema import ColumnCollectionConstraint

from ormar.databases.connection import DatabaseConnection
from ormar.fields import BaseField, ForeignKeyField, ManyToManyField
from ormar.models.helpers import alias_manager
from ormar.models.utils import Extra
from ormar.queryset.queryset import QuerySet
from ormar.relations import AliasManager
from ormar.signals import SignalEmitter


class OrmarConfig:
    if TYPE_CHECKING:  # pragma: no cover
        pkname: str
        metadata: sqlalchemy.MetaData
        database: DatabaseConnection
        engine: AsyncEngine
        tablename: str
        order_by: list[str]
        abstract: bool
        proxy: bool
        emit_parent_signals: bool
        exclude_parent_fields: list[str]
        constraints: list[ColumnCollectionConstraint]

    def __init__(
        self,
        metadata: Optional[sqlalchemy.MetaData] = None,
        database: Optional[DatabaseConnection] = None,
        engine: Optional[AsyncEngine] = None,
        tablename: Optional[str] = None,
        order_by: Optional[list[str]] = None,
        abstract: bool = False,
        proxy: bool = False,
        emit_parent_signals: bool = False,
        queryset_class: type[QuerySet] = QuerySet,
        extra: Extra = Extra.forbid,
        constraints: Optional[list[ColumnCollectionConstraint]] = None,
    ) -> None:
        self.pkname = None  # type: ignore
        self.metadata = metadata  # type: ignore
        self.database = database  # type: ignore
        self.engine = engine  # type: ignore
        self.tablename = tablename  # type: ignore
        self.orders_by = order_by or []
        self.columns: list[sqlalchemy.Column] = []
        self.constraints = constraints or []
        self.model_fields: dict[
            str, Union[BaseField, ForeignKeyField, ManyToManyField]
        ] = {}
        self.alias_manager: AliasManager = alias_manager
        self.property_fields: set = set()
        self.signals: SignalEmitter = SignalEmitter()
        self.abstract = abstract
        self.proxy = proxy
        self.emit_parent_signals = emit_parent_signals
        self.requires_ref_update: bool = False
        self.extra = extra
        self.queryset_class = queryset_class
        self.table: sqlalchemy.Table = None  # type: ignore

    def copy(
        self,
        metadata: Optional[sqlalchemy.MetaData] = None,
        database: Optional[DatabaseConnection] = None,
        engine: Optional[AsyncEngine] = None,
        tablename: Optional[str] = None,
        order_by: Optional[list[str]] = None,
        abstract: Optional[bool] = None,
        proxy: Optional[bool] = None,
        emit_parent_signals: Optional[bool] = None,
        queryset_class: Optional[type[QuerySet]] = None,
        extra: Optional[Extra] = None,
        constraints: Optional[list[ColumnCollectionConstraint]] = None,
    ) -> "OrmarConfig":
        return OrmarConfig(
            metadata=metadata or self.metadata,
            database=database or self.database,
            engine=engine or self.engine,
            tablename=tablename,
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
        )
