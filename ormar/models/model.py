import builtins
from typing import TYPE_CHECKING, Any, Optional, TypeVar, Union

from sqlalchemy import Executable

import ormar.queryset  # noqa I100
from ormar.exceptions import ModelPersistenceError, NoMatch
from ormar.models import NewBaseModel  # noqa I100
from ormar.models.model_row import ModelRow
from ormar.queryset.utils import subtract_dict, translate_list_to_dict

T = TypeVar("T", bound="Model")

if TYPE_CHECKING:  # pragma: no cover
    from ormar import ForeignKeyField
    from ormar.models.ormar_config import OrmarConfig


class Model(ModelRow):
    __abstract__ = False
    if TYPE_CHECKING:  # pragma nocover
        ormar_config: OrmarConfig

    def __repr__(self) -> str:  # pragma nocover
        _repr = {
            k: getattr(self, k)
            for k, v in self.ormar_config.model_fields.items()
            if not v.skip_field
        }
        return f"{self.__class__.__name__}({str(_repr)})"

    async def _execute_query(self, expr: Executable, is_select: bool = False) -> Any:
        async with self.ormar_config.database.get_query_executor() as executor:
            row = (
                await executor.fetch_one(expr)
                if is_select
                else await executor.execute(expr)
            )
        return row

    async def _emit_signal(self, name: str, **kwargs: Any) -> None:
        """
        Emit a lifecycle signal on this model's SignalEmitter.

        When ``ormar_config.emit_parent_signals`` is True, the same signal is
        also dispatched on every concrete ormar ancestor in the MRO. Each
        emit uses ``sender=ancestor_cls`` so handlers registered with
        ``@pre_save(Parent)`` see the parent class as the sender.

        :param name: signal name on the SignalEmitter (e.g. ``"pre_save"``).
        :type name: str
        :param kwargs: extra payload forwarded to receivers.
        :type kwargs: Any
        """
        cls = type(self)
        await getattr(self.ormar_config.signals, name).send(sender=cls, **kwargs)
        if not self.ormar_config.emit_parent_signals:
            return
        seen = {id(self.ormar_config.signals)}
        for ancestor in cls.__mro__[1:]:
            cfg = getattr(ancestor, "ormar_config", None)
            if cfg is None or cfg.abstract or id(cfg.signals) in seen:
                continue
            seen.add(id(cfg.signals))
            await getattr(cfg.signals, name).send(sender=ancestor, **kwargs)

    async def upsert(self: T, **kwargs: Any) -> T:
        """
        Performs either a save or an update depending on the presence of the pk.
        If the pk field is filled it's an update, otherwise the save is performed.
        For save kwargs are ignored, used only in update if provided.

        :param kwargs: list of fields to update
        :type kwargs: Any
        :return: saved Model
        :rtype: Model
        """

        force_save = kwargs.pop("__force_save__", False)
        if force_save:
            expr = self.ormar_config.table.select().where(self.pk_column == self.pk)
            row = await self._execute_query(expr, is_select=True)
            if not row:
                return await self.save()
            return await self.update(**kwargs)

        if not self.pk:
            return await self.save()
        return await self.update(**kwargs)

    async def save(self: T) -> T:
        """
        Performs a save of given Model instance.
        If primary key is already saved, db backend will throw integrity error.

        Related models are saved by pk number, reverse relation and many to many fields
        are not saved - use corresponding relations methods.

        If there are fields with server_default set and those fields
        are not already filled save will trigger also a second query
        to refreshed the fields populated server side.

        Does not recognize if model was previously saved.
        If you want to perform update or insert depending on the pk
        fields presence use upsert.

        Sends pre_save and post_save signals.

        Sets model save status to True.

        :return: saved Model
        :rtype: Model
        """
        await self._emit_signal("pre_save", instance=self)
        self_fields = self._extract_model_db_fields()

        if (
            not self.pk
            and self.ormar_config.model_fields[self.ormar_config.pkname].autoincrement
        ):
            self_fields.pop(self.ormar_config.pkname, None)
        self_fields = self.populate_default_values(self_fields)
        self.update_from_dict(
            {
                k: v
                for k, v in self_fields.items()
                if k not in self.extract_related_names()
            }
        )

        self_fields = self.translate_columns_to_aliases(self_fields)
        expr = self.ormar_config.table.insert()
        expr = expr.values(**self_fields)

        pkname = self.ormar_config.pkname
        pk_returned_from_insert = False
        pk = await self._execute_query(expr)
        if pk and isinstance(pk, self.pk_type()):
            setattr(self, pkname, pk)
            pk_returned_from_insert = True

        if self.pk is None:
            raise ModelPersistenceError(  # pragma: no cover
                f"Could not recover the generated primary key for "
                f"{self.__class__.__name__} after INSERT. This happens on "
                "backends that lack RETURNING support for server-side "
                "defaults on non-AUTO_INCREMENT primary keys — most notably "
                "Oracle MySQL, which does not implement RETURNING in any "
                "version. Use autoincrement=True, provide the primary key "
                "client-side, or switch to a RETURNING-capable backend "
                "(PostgreSQL, SQLite 3.35+, MariaDB 10.5+)."
            )

        self.set_save_status(True)
        # refresh server-side defaults — but skip the pk if the insert already
        # returned it, so save() stays a single round-trip when the pk is the
        # only server_default field.
        if any(
            field.server_default is not None
            for name, field in self.ormar_config.model_fields.items()
            if name not in self_fields
            and not (pk_returned_from_insert and name == pkname)
        ):
            await self.load()

        self.__setattr_fields__.clear()
        await self._emit_signal("post_save", instance=self)
        return self

    async def save_related(  # noqa: CCR001, CFQ002
        self,
        follow: bool = False,
        save_all: bool = False,
        relation_map: Optional[builtins.dict] = None,
        exclude: Union[set, builtins.dict, None] = None,
        update_count: int = 0,
        previous_model: Optional["Model"] = None,
        relation_field: Optional["ForeignKeyField"] = None,
    ) -> int:
        """
        Triggers a upsert method on all related models
        if the instances are not already saved.
        By default saves only the directly related ones.

        If follow=True is set it saves also related models of related models.

        To not get stuck in an infinite loop as related models also keep a relation
        to parent model visited models set is kept.

        That way already visited models that are nested are saved, but the save do not
        follow them inside. So Model A -> Model B -> Model A -> Model C will save second
        Model A but will never follow into Model C.
        Nested relations of those kind need to be persisted manually.

        :param relation_field: field with relation leading to this model
        :type relation_field: Optional[ForeignKeyField]
        :param previous_model: previous model from which method came
        :type previous_model: Model
        :param exclude: items to exclude during saving of relations
        :type exclude: Union[set, dict]
        :param relation_map: map of relations to follow
        :type relation_map: dict
        :param save_all: flag if all models should be saved or only not saved ones
        :type save_all: bool
        :param follow: flag to trigger deep save -
        by default only directly related models are saved
        with follow=True also related models of related models are saved
        :type follow: bool
        :param update_count: internal parameter for recursive calls -
        number of updated instances
        :type update_count: int
        :return: number of updated/saved models
        :rtype: int
        """
        relation_map = (
            relation_map
            if relation_map is not None
            else translate_list_to_dict(self._iterate_related_models())
        )
        if exclude and isinstance(exclude, set):
            exclude = translate_list_to_dict(exclude)
        relation_map = subtract_dict(relation_map, exclude or {})

        if relation_map:
            fields_to_visit = {
                field
                for field in self.extract_related_fields()
                if field.name in relation_map
            }
            pre_save = {
                field
                for field in fields_to_visit
                if not field.virtual and not field.is_multi
            }

            update_count = await self._update_relation_list(
                fields_list=pre_save,
                follow=follow,
                save_all=save_all,
                relation_map=relation_map,
                update_count=update_count,
            )

            update_count = await self._upsert_model(
                instance=self,
                save_all=save_all,
                previous_model=previous_model,
                relation_field=relation_field,
                update_count=update_count,
            )

            post_save = fields_to_visit - pre_save

            update_count = await self._update_relation_list(
                fields_list=post_save,
                follow=follow,
                save_all=save_all,
                relation_map=relation_map,
                update_count=update_count,
            )

        else:
            update_count = await self._upsert_model(
                instance=self,
                save_all=save_all,
                previous_model=previous_model,
                relation_field=relation_field,
                update_count=update_count,
            )

        return update_count

    async def update(self: T, _columns: Optional[list[str]] = None, **kwargs: Any) -> T:
        """
        Performs update of Model instance in the database.
        Fields can be updated before or you can pass them as kwargs.

        Sends pre_update and post_update signals.

        Sets model save status to True.

        :param _columns: list of columns to update, if None all are updated
        :type _columns: list
        :raises ModelPersistenceError: If the pk column is not set

        :param kwargs: list of fields to update as field=value pairs
        :type kwargs: Any
        :return: updated Model
        :rtype: Model
        """
        explicit_fields = self.__setattr_fields__ | kwargs.keys()
        values = self.populate_onupdate_value(
            dict(kwargs), explicit_fields=explicit_fields
        )
        if values:
            self.update_from_dict(values)

        if not self.pk:
            raise ModelPersistenceError(
                "You cannot update not saved model! Use save or upsert method."
            )

        await self._emit_signal("pre_update", instance=self, passed_args=kwargs)
        self_fields = self._extract_model_db_fields()
        self_fields.pop(self.get_column_name_from_alias(self.ormar_config.pkname))
        if _columns:
            self_fields = {
                k: v
                for k, v in self_fields.items()
                if k in _columns or k in self._onupdate_fields
            }
        if self_fields:
            self_fields = self.translate_columns_to_aliases(self_fields)
            expr = self.ormar_config.table.update().values(**self_fields)
            expr = expr.where(self.pk_column == getattr(self, self.ormar_config.pkname))
            await self._execute_query(expr)
        self.set_save_status(True)
        self.__setattr_fields__.clear()
        await self._emit_signal("post_update", instance=self)
        return self

    async def delete(self) -> int:
        """
        Removes the Model instance from the database.

        Sends pre_delete and post_delete signals.

        Sets model save status to False.

        Note it does not delete the Model itself (python object).
        So you can delete and later save (since pk is deleted no conflict will arise)
        or update and the Model will be saved in database again.

        :return: number of deleted rows (for some backends)
        :rtype: int
        """
        await self._emit_signal("pre_delete", instance=self)
        expr = self.ormar_config.table.delete()
        expr = expr.where(self.pk_column == (getattr(self, self.ormar_config.pkname)))
        result = await self._execute_query(expr)
        self.set_save_status(False)
        await self._emit_signal("post_delete", instance=self)
        return result

    async def load(self: T) -> T:
        """
        Allow to refresh existing Models fields from database.
        Be careful as the related models can be overwritten by pk_only models in load.
        Does NOT refresh the related models fields if they were loaded before.

        :raises NoMatch: If given pk is not found in database.

        :return: reloaded Model
        :rtype: Model
        """
        expr = self.ormar_config.table.select().where(self.pk_column == self.pk)
        row = await self._execute_query(expr, is_select=True)
        if not row:  # pragma nocover
            raise NoMatch("Instance was deleted from database and cannot be refreshed")
        kwargs = dict(row)
        kwargs = self.translate_aliases_to_columns(kwargs)
        self.update_from_dict(kwargs)
        self.set_save_status(True)
        self.__setattr_fields__.clear()
        return self

    async def load_all(
        self: T,
        follow: bool = False,
        exclude: Union[list, str, set, dict, None] = None,
        order_by: Union[list, str, None] = None,
    ) -> T:
        """
        Allow to refresh existing Models fields from database.
        Performs refresh of the related models fields.

        By default, loads only self and the directly related ones.

        If follow=True is set it loads also related models of related models.

        To not get stuck in an infinite loop as related models also keep a relation
        to parent model visited models set is kept.

        That way already visited models that are nested are loaded, but the load do not
        follow them inside. So Model A -> Model B -> Model C -> Model A -> Model X
        will load second Model A but will never follow into Model X.
        Nested relations of those kind need to be loaded manually.

        :param order_by: columns by which models should be sorted
        :type order_by: Union[list, str]
        :raises NoMatch: If given pk is not found in database.

        :param exclude: related models to exclude
        :type exclude: Union[list, str, set, dict]
        :param follow: flag to trigger deep save -
        by default only directly related models are saved
        with follow=True also related models of related models are saved
        :type follow: bool
        :return: reloaded Model
        :rtype: Model
        """
        relations = list(self.extract_related_names())
        if follow:
            relations = self._iterate_related_models()
        queryset = self.__class__.objects
        if exclude:
            queryset = queryset.exclude_fields(exclude)
        if order_by:
            queryset = queryset.order_by(order_by)
        instance = await queryset.select_related(relations).get(pk=self.pk)
        self._orm.clear()
        self.update_from_dict(instance.model_dump())
        self.__setattr_fields__.clear()
        return self
