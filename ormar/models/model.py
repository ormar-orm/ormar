import builtins
import warnings
from typing import TYPE_CHECKING, Any, Literal, Optional, TypeVar, Union, cast

from sqlalchemy import Executable
from sqlalchemy.exc import IntegrityError

import ormar.queryset  # noqa I100
from ormar.exceptions import ModelPersistenceError, NoMatch
from ormar.models import NewBaseModel  # noqa I100
from ormar.models.model_row import ModelRow
from ormar.queryset.templates import (
    delete_template,
    insert_template,
    pk_select_template,
    update_template,
)
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

    async def _execute_query(
        self,
        expr: Executable,
        is_select: bool = False,
        params: Optional[dict] = None,
    ) -> Any:
        """
        Run a statement of this model: fetch one row of a select, otherwise
        execute the write and return its result (inserted primary key or
        number of changed rows).

        Single-row writes and ``load()`` pass a cached statement template
        (see ``ormar.queryset.templates``) and its values as ``params``.

        :param expr: statement to run
        :type expr: Executable
        :param is_select: fetch one row instead of executing a write
        :type is_select: bool
        :param params: values for named bind parameters of the statement
        :type params: Optional[dict]
        :return: fetched row, inserted primary key or row count
        :rtype: Any
        """
        async with self.ormar_config.database.get_query_executor() as executor:
            row = (
                await executor.fetch_one(query=expr, params=params)
                if is_select
                else await executor.execute(query=expr, params=params)
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
        Performs either a save or an update:

        * a model loaded from or already saved to the database is updated,
        * a model without a primary key is inserted,
        * a model never saved but with its key set (uuid default, natural or
          composite key) is looked up first and updated if the row exists,
          inserted otherwise.

        For save kwargs are ignored, used only in update if provided.
        ``__force_save__`` is accepted for backwards compatibility and ignored.

        :param kwargs: list of fields to update
        :type kwargs: Any
        :return: saved Model
        :rtype: Model
        """
        self.verify_writable()
        kwargs.pop("__force_save__", None)
        if self.persisted:
            return await self.update(**kwargs)
        if self.pk is None:
            return await self.save()
        template = pk_select_template(model_cls=type(self), pk=self.pk)
        row = await self._execute_query(
            expr=template.statement, is_select=True, params=template.params
        )
        if not row:
            return await self.save()
        return await self.update(_partial=False, **kwargs)

    def _verify_key_parts_set(self, row: dict[str, Any]) -> None:
        """
        Checks that every primary key column the database does not generate
        is set before an insert.

        :raises ModelPersistenceError: if a key part is missing
        :param row: values about to be inserted, by column name
        :type row: dict[str, Any]
        """
        key = self.ormar_config.pk
        if key.autoincrement:
            return
        model_fields = self.ormar_config.model_fields
        missing: list[str] = []
        for column in key.column_aliases():
            owners = [
                name
                for name in self.get_field_names_of_column(column=column)
                if name in key.fields
            ]
            if row.get(column) is None and all(
                model_fields[name].server_default is None for name in owners
            ):
                missing.extend(name for name in owners if name not in missing)
        if missing:
            raise ModelPersistenceError(
                f"{self.__class__.__name__} primary key parts must be set before "
                f"saving: {missing}"
            )

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
        return cast(T, await self._save())

    async def _save(
        self: T, on_conflict: Optional[Literal["ignore"]] = None
    ) -> Optional[T]:
        """
        Body of ``save()``. With ``on_conflict="ignore"`` a row conflicting with
        an existing one is skipped: ``None`` is returned, ``post_save`` not sent.

        :param on_conflict: ``ignore`` to skip a conflicting row
        :type on_conflict: Optional[Literal["ignore"]]
        :return: saved Model, ``None`` when the row was skipped
        :rtype: Optional[Model]
        """
        self.verify_writable()
        await self._emit_signal("pre_save", instance=self)
        self_fields = self._extract_model_db_fields()

        key = self.ormar_config.pk
        if not self.pk and key.autoincrement:
            for field in key.fields:
                self_fields.pop(field, None)
        self_fields = self.populate_default_values(self_fields)
        # Values taken from ``__dict__`` are already set, re-assigning them only
        # costs a validation and two hash recomputations per field.
        current_values = self.__dict__
        related_names = self.extract_related_names()
        self.update_from_dict(
            {
                k: v
                for k, v in self_fields.items()
                if k not in related_names
                and k in self.ormar_config.model_fields
                and (k not in current_values or current_values[k] is not v)
            }
        )

        self_fields = self.translate_columns_to_aliases(self_fields)
        self._verify_key_parts_set(row=self_fields)
        # MySQL has no conflict clause telling an insert from a skipped row, so
        # it runs a plain insert and a duplicate key error means skipped
        returning = bool(on_conflict) and self.db_backend_name() != "mysql"
        template = insert_template(
            model_cls=type(self),
            values=self_fields,
            on_conflict=on_conflict if returning else None,
            returning=returning,
        )
        try:
            pk = await self._execute_query(
                expr=template.statement, is_select=returning, params=template.params
            )
        except IntegrityError as error:
            if not on_conflict or error.orig.args[0] != 1062:  # type: ignore
                raise
            return None  # pragma: no cover
        if returning:  # pragma: no cover
            if pk is None:
                return None
            pk = key.getter(keys=key.column_aliases())(pk)

        pk_returned_from_insert = False
        if pk and self.pk is None and isinstance(pk, key.python_type()):
            for field, part in key.to_dict(value=pk).items():
                setattr(self, field, part)
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
            and not (pk_returned_from_insert and name in key.fields)
        ):
            await self.load()

        self._reset_change_tracking()
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
        self.verify_writable()
        relation_map = (
            relation_map if relation_map is not None else self._relation_tree()
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

    async def update(
        self: T,
        _columns: Optional[list[str]] = None,
        _partial: Optional[bool] = None,
        **kwargs: Any,
    ) -> T:
        """
        Performs update of Model instance in the database.
        Fields can be updated before or you can pass them as kwargs.

        Which columns are written:

        * ``_columns`` given: only those (and ``onupdate`` fields),
        * nothing changed since the model was loaded or saved: all columns,
        * partial update (``_partial=True``, or ``partial_updates=True`` in
          ``OrmarConfig`` when ``_partial`` is not given): the kwargs, the
          fields assigned since the last load or save (also by ``pre_update``
          signal handlers), json fields mutated in place (with
          ``partial_updates=True``) and ``onupdate`` fields,
        * otherwise all columns. With the default ``partial_updates=None`` a
          ``DeprecationWarning`` is emitted when kwargs are passed and other
          columns are written too, as the default changes in a future
          release.

        Sends pre_update and post_update signals.

        Sets model save status to True.

        :param _columns: list of columns to update
        :type _columns: Optional[list[str]]
        :param _partial: write only changed fields (``True``) or all columns
            (``False``), ``None`` follows ``partial_updates`` of the config
        :type _partial: Optional[bool]
        :raises ModelPersistenceError: If the pk column is not set

        :param kwargs: list of fields to update as field=value pairs
        :type kwargs: Any
        :return: updated Model
        :rtype: Model
        """
        self.verify_writable()
        explicit_fields = self.__setattr_fields__ | kwargs.keys()
        auto_onupdate = self._onupdate_fields - explicit_fields
        values = self.populate_onupdate_value(
            dict(kwargs), explicit_fields=explicit_fields
        )
        if values:
            self.update_from_dict(values)

        if self.persisted_key is None:
            raise ModelPersistenceError(
                "You cannot update not saved model! Use save or upsert method."
            )

        await self._emit_signal("pre_update", instance=self, passed_args=kwargs)
        self_fields = self._extract_model_db_fields()
        key_fields = {
            value_key
            for field in self.ormar_config.pk.fields
            for value_key in self.value_keys(field_name=field)
        }
        key_moved = self.persisted_key != self.pk
        if not key_moved:
            for field in key_fields:
                self_fields.pop(field, None)
        self_fields = self._fields_to_update(
            self_fields=self_fields,
            columns=_columns,
            partial=_partial,
            changed=self._changed_fields() - auto_onupdate,
            passed_kwargs=bool(kwargs),
            key_fields=key_fields if key_moved else set(),
        )
        if self_fields:
            self_fields = self.translate_columns_to_aliases(self_fields)
            template = update_template(
                model_cls=type(self),
                values=self_fields,
                pk=self.persisted_key,
            )
            await self._execute_query(expr=template.statement, params=template.params)
        self.set_save_status(True)
        self._reset_change_tracking()
        await self._emit_signal("post_update", instance=self)
        return self

    def _fields_to_update(  # noqa: CFQ002
        self,
        self_fields: builtins.dict[str, Any],
        columns: Optional[list[str]],
        partial: Optional[bool],
        changed: set[str],
        passed_kwargs: bool,
        key_fields: set[str],
    ) -> builtins.dict[str, Any]:
        """
        Selects the column values ``update()`` writes, see its docstring.

        :param self_fields: values of all columns of the model by field name
        :type self_fields: dict[str, Any]
        :param columns: ``_columns`` passed to ``update()``
        :type columns: Optional[list[str]]
        :param partial: ``_partial`` passed to ``update()``
        :type partial: Optional[bool]
        :param changed: fields changed since the last load or save
        :type changed: set[str]
        :param passed_kwargs: if ``update()`` received field values
        :type passed_kwargs: bool
        :param key_fields: primary key fields to write as the key moved
        :type key_fields: set[str]
        :return: values of the columns to write
        :rtype: dict[str, Any]
        """
        if columns:
            return self._select_update_columns(
                self_fields=self_fields, field_names=set(columns) | key_fields
            )
        if partial is None:
            partial = self.ormar_config.partial_updates
        if not changed or partial is False:
            return self_fields
        selected = self._select_update_columns(
            self_fields=self_fields, field_names=changed | key_fields
        )
        if partial:
            return selected
        if passed_kwargs and selected.keys() != self_fields.keys():
            warnings.warn(
                f"{self.get_name(lower=False)}.update(**kwargs) writes all columns "
                "of the row, not only the passed and changed fields, which reverts "
                "changes made in the database since the model was loaded. Set "
                "`partial_updates=True` in `OrmarConfig` to write only the changed "
                "fields (the default in a future release), `partial_updates=False` "
                "to keep writing all columns, or pass `_columns`.",
                DeprecationWarning,
                stacklevel=3,
            )
        return self_fields

    def _select_update_columns(
        self, self_fields: builtins.dict[str, Any], field_names: set[str]
    ) -> builtins.dict[str, Any]:
        """
        Keeps the values of the given fields (by field or column name; a foreign
        key stored in several columns keeps all of them, a column it shares
        with other fields under the key that column is kept under, see
        ``value_keys``) and of ``onupdate`` fields.

        :param self_fields: values of the model columns by field name
        :type self_fields: dict[str, Any]
        :param field_names: names of fields to keep
        :type field_names: set[str]
        :return: values of the selected columns
        :rtype: dict[str, Any]
        """
        model_fields = self.ormar_config.model_fields
        selected = set(field_names) | self._onupdate_fields
        for name in field_names:
            selected.update(self.get_column_aliases(field_name=name))
            field = model_fields.get(name)
            if field is not None and field.kind.holds_columns:
                selected.update(self.value_keys(field_name=name))
        return {k: v for k, v in self_fields.items() if k in selected}

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
        self.verify_writable()
        await self._emit_signal("pre_delete", instance=self)
        template = delete_template(model_cls=type(self), pk=self.persisted_key)
        result = await self._execute_query(
            expr=template.statement, params=template.params
        )
        self.set_save_status(False)
        self.set_persisted(persisted=False)
        await self._emit_signal("post_delete", instance=self)
        return result

    async def _load_by_set_fields(self) -> dict[str, Any]:
        """
        Load the row of a key-only model referenced by a unique non-pk field
        (``ForeignKey(to_field=...)``): it has no primary key, so the row is
        looked up by its set own fields.

        :raises NoMatch: if no row matches
        :return: values of the row by field name
        :rtype: dict[str, Any]
        """
        own_fields = self.extract_db_own_fields()
        lookup = {
            name: value
            for name, value in self.__dict__.items()
            if name in own_fields and value is not None
        }
        found = await type(self).objects.get(**lookup)
        return {name: found.__dict__[name] for name in own_fields}

    async def load(self: T) -> T:
        """
        Allow to refresh existing Models fields from database.
        Be careful as the related models can be overwritten by pk_only models in load.
        Does NOT refresh the related models fields if they were loaded before.

        :raises NoMatch: If given pk is not found in database.

        :return: reloaded Model
        :rtype: Model
        """
        if self.persisted_key is None and self.__pk_only__:
            kwargs = await self._load_by_set_fields()
        elif self.persisted_key is None:
            raise NoMatch(
                f"{self.get_name(lower=False)} has no primary key, it cannot be "
                "loaded from the database"
            )
        else:
            template = pk_select_template(model_cls=type(self), pk=self.persisted_key)
            row = await self._execute_query(
                expr=template.statement, is_select=True, params=template.params
            )
            if not row:  # pragma nocover
                raise NoMatch(
                    "Instance was deleted from database and cannot be refreshed"
                )
            kwargs = self.translate_row_to_fields(row=dict(row))
        self.update_from_dict(kwargs)
        self.set_save_status(True)
        self._reset_change_tracking()
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
        instance = await queryset.select_related(relations).get(pk=self.persisted_key)
        self._orm.clear()
        self.update_from_dict(instance.model_dump())
        self._reset_change_tracking()
        return self
