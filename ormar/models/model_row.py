from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, AbstractSet, Any, Optional, Union, cast

try:
    from sqlalchemy.engine.result import ResultProxy  # type: ignore
except ImportError:  # pragma: no cover
    from sqlalchemy.engine.result import Row as ResultProxy  # type: ignore

from ormar.models import NewBaseModel  # noqa: I202
from ormar.models.excludable import ExcludableItems
from ormar.models.helpers.models import group_related_list
from ormar.models.row_registry import RegistryKey, RowRegistry, selects_multi_relation
from ormar.relations.aliases import alias_for

if TYPE_CHECKING:  # pragma: no cover
    from ormar.fields import ForeignKeyField
    from ormar.models import Model


@dataclass(frozen=True)
class RowExtractionPlan:
    """
    Precomputed per-``(model_cls, table_prefix, excludable)`` view of the work
    that ``from_row`` used to redo for every row: which columns to read from
    the SA row (already prefixed and filtered), which field names to nullify
    after construction, and the model's pk field name.

    :ivar column_mappings: ordered ``(prefixed_db_column, field_name)`` pairs
        the row reader iterates over to populate the model dict
    :ivar excluded_field_names: field names to nullify via
        ``_construct_with_excluded`` after pydantic validation
    :ivar pk_field_name: cached ``ormar_config.pkname`` so the row reader can
        test for a populated pk without a per-row attribute lookup
    :ivar pk_column: prefixed db column holding the pk, ``None`` when the pk is
        not among the selected columns
    """

    column_mappings: tuple[tuple[str, str], ...]
    excluded_field_names: frozenset[str]
    pk_field_name: str
    pk_column: Optional[str] = None


PlanCache = dict[tuple[type, str, int], RowExtractionPlan]


@lru_cache(maxsize=2048)
def build_shared_row_plan(
    model_cls: type["ModelRow"],
    table_prefix: str,
    include: frozenset,
    exclude: frozenset,
    pk_only: bool,
) -> RowExtractionPlan:
    """
    Builds a row extraction plan shared between querysets. A plan only depends
    on the model, the table prefix and the model's own include/exclude/pk_only
    settings, so those are the cache key.

    :param model_cls: model the plan is built for
    :type model_cls: type[ModelRow]
    :param table_prefix: alias of the table from alias_for
    :type table_prefix: str
    :param include: fields to include for this model
    :type include: frozenset
    :param exclude: fields to exclude for this model
    :type exclude: frozenset
    :param pk_only: flag if only the primary key is selected
    :type pk_only: bool
    :return: extraction plan for this row position
    :rtype: RowExtractionPlan
    """
    excludable = ExcludableItems()
    model_excludable = excludable.get(model_cls=model_cls, alias=table_prefix)  # type: ignore
    model_excludable.include = set(include)
    model_excludable.exclude = set(exclude)
    model_excludable.pk_only = pk_only
    return model_cls.build_row_extraction_plan(table_prefix, excludable)


class ModelRow(NewBaseModel):
    @classmethod
    def from_row(  # noqa: CFQ002
        cls,
        row: ResultProxy,
        source_model: type["Model"],
        select_related: Optional[list] = None,
        related_models: Any = None,
        related_field: Optional["ForeignKeyField"] = None,
        excludable: Optional[ExcludableItems] = None,
        current_relation_str: str = "",
        proxy_source_model: Optional[type["Model"]] = None,
        plan_cache: Optional[PlanCache] = None,
        registry: Optional[RowRegistry] = None,
        scope: Any = None,
    ) -> Optional["Model"]:
        """
        Model method to convert raw sql row from database into ormar.Model instance.
        Traverses nested models if they were specified in select_related for query.

        Called recurrently and returns model instance if it's present in the row.
        Note that it's processing one row at a time, so if there are duplicates of
        parent row that needs to be joined/combined
        (like parent row in sql join with 2+ child rows)
        instances populated in this method are later combined in the QuerySet.
        Other method working directly on raw database results is in prefetch_query,
        where rows are populated in a different way as they do not have
        nested models in result.

        :param proxy_source_model: source model from which querysetproxy is constructed
        :type proxy_source_model: Optional[type["ModelRow"]]
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :param current_relation_str: name of the relation field
        :type current_relation_str: str
        :param source_model: model on which relation was defined
        :type source_model: type[Model]
        :param row: raw result row from the database
        :type row: ResultProxy
        :param select_related: list of names of related models fetched from database
        :type select_related: list
        :param related_models: list or dict of related models
        :type related_models: Union[list, dict]
        :param related_field: field with relation declaration
        :type related_field: ForeignKeyField
        :param plan_cache: optional per-queryset plan cache
        :type plan_cache: Optional[PlanCache]
        :param registry: optional per-query identity map; when passed, a model
            already built for the same relation path and pk is reused and only
            new reverse/many-to-many children from this row are attached to it
        :type registry: Optional[RowRegistry]
        :param scope: registry key of the closest parent reached through a
            many-to-many relation, ``None`` outside of such relations
        :type scope: Any
        :return: returns model if model is populated from database
        :rtype: Optional[Model]
        """
        if select_related:
            related_models = group_related_list(select_related)
        instance, _ = cls._materialize_row(
            row=row,
            source_model=source_model,
            related_models=related_models or [],
            related_field=related_field,
            excludable=excludable or ExcludableItems(),
            current_relation_str=current_relation_str,
            proxy_source_model=proxy_source_model,
            plan_cache=plan_cache,
            registry=registry,
            scope=scope,
        )
        return instance

    @classmethod
    def _materialize_row(  # noqa: CFQ002
        cls,
        row: ResultProxy,
        source_model: type["Model"],
        related_models: Any,
        related_field: Optional["ForeignKeyField"],
        excludable: ExcludableItems,
        current_relation_str: str,
        proxy_source_model: Optional[type["Model"]],
        plan_cache: Optional[PlanCache],
        registry: Optional[RowRegistry],
        scope: Any,
    ) -> tuple[Optional["Model"], bool]:
        """
        Builds a model (and its selected relations) from a database row, or
        reuses the model already built for the same relation path and pk.

        :param row: raw result row from the database
        :type row: ResultProxy
        :param source_model: model on which relation was defined
        :type source_model: type[Model]
        :param related_models: list or dict of related models
        :type related_models: Union[list, dict]
        :param related_field: field with relation declaration
        :type related_field: Optional[ForeignKeyField]
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :param current_relation_str: name of the relation field
        :type current_relation_str: str
        :param proxy_source_model: source model from which querysetproxy is constructed
        :type proxy_source_model: Optional[type[Model]]
        :param plan_cache: optional per-queryset plan cache
        :type plan_cache: Optional[PlanCache]
        :param registry: optional per-query identity map
        :type registry: Optional[RowRegistry]
        :param scope: registry key of the closest parent reached through a
            many-to-many relation, ``None`` outside of such relations
        :type scope: Any
        :return: the model (``None`` if the row has no pk for it) and a flag
            whether it was reused from the registry, i.e. built from an
            earlier row and already registered on its parent
        :rtype: tuple[Optional[Model], bool]
        """
        table_prefix = alias_for(current_relation_str) if related_field else ""

        plan = cls.get_or_build_row_plan(table_prefix, excludable, plan_cache)
        key: Optional[RegistryKey] = None
        if registry is not None and plan.pk_column is not None:
            pk = row[plan.pk_column]
            if pk is None:
                return None, False
            key = (scope, table_prefix, pk)
            existing = registry.instances.get(key)
            if existing is not None:
                if cls._selects_multi_relation(registry, table_prefix, related_models):
                    children, reused = cls._populate_nested_models_from_row(
                        item={},
                        row=row,
                        related_models=related_models,
                        excludable=excludable,
                        current_relation_str=current_relation_str,
                        source_model=source_model,  # type: ignore
                        proxy_source_model=proxy_source_model,  # type: ignore
                        table_prefix=table_prefix,
                        plan_cache=plan_cache,
                        registry=registry,
                        parent_key=key,
                        scope=scope,
                    )
                    cls._attach_children(existing, children, reused)
                return existing, True

        item, _ = cls._populate_nested_models_from_row(
            item={},
            row=row,
            related_models=related_models,
            excludable=excludable,
            current_relation_str=current_relation_str,
            source_model=source_model,  # type: ignore
            proxy_source_model=proxy_source_model,  # type: ignore
            table_prefix=table_prefix,
            plan_cache=plan_cache,
            registry=registry,
            parent_key=key,
            scope=scope,
        )
        item = cls.apply_row_plan(plan, row, item)

        instance: Optional["Model"] = None
        if item.get(plan.pk_field_name, None) is not None:
            instance = cast(
                "Model",
                cls._construct_with_excluded(plan.excluded_field_names, **item),
            )
            instance.set_save_status(True)
            if key is not None:
                registry.instances[key] = instance  # type: ignore[union-attr]
        return instance, False

    @classmethod
    def _selects_multi_relation(
        cls, registry: RowRegistry, table_prefix: str, related_models: Any
    ) -> bool:
        """
        Memoized per query check if relations selected below this model contain
        a reverse or many-to-many relation that a repeated row can extend.

        :param registry: per-query identity map holding the memo
        :type registry: RowRegistry
        :param table_prefix: alias of the model table in the query
        :type table_prefix: str
        :param related_models: selected relations as list or nested dict
        :type related_models: Union[list, dict]
        :return: True if repeated rows may carry new children for this model
        :rtype: bool
        """
        memo_key = (cls, table_prefix)
        result = registry.subtree_has_multi.get(memo_key)
        if result is None:
            result = selects_multi_relation(cls, related_models)
            registry.subtree_has_multi[memo_key] = result
        return result

    @classmethod
    def _attach_children(
        cls, instance: "Model", children: dict, reused: AbstractSet[str]
    ) -> None:
        """
        Registers children read from a repeated row on an already built model.

        Only reverse, many-to-many and through relations can bring new children;
        a forward foreign key of the same model always resolves to the same,
        already registered instance. Children reused from the registry were
        registered on this model by the row that built them, so they are skipped.

        :param instance: model built from an earlier row
        :type instance: Model
        :param children: related models read from the current row by field name
        :type children: dict
        :param reused: names of relations whose child was reused from the registry
        :type reused: AbstractSet[str]
        """
        model_fields = cls.ormar_config.model_fields
        for name, child in children.items():
            if name in reused:
                continue
            field = model_fields[name]
            if child is not None and (
                field.is_multi or field.virtual or field.is_through
            ):
                field.expand_relationship(child, instance, to_register=True)

    @classmethod
    def _populate_nested_models_from_row(  # noqa: CFQ002
        cls,
        item: dict,
        row: ResultProxy,
        source_model: type["Model"],
        related_models: Any,
        excludable: ExcludableItems,
        table_prefix: str,
        current_relation_str: Optional[str] = None,
        proxy_source_model: Optional[type["Model"]] = None,
        plan_cache: Optional[PlanCache] = None,
        registry: Optional[RowRegistry] = None,
        parent_key: Optional[RegistryKey] = None,
        scope: Any = None,
    ) -> tuple[dict, set[str]]:
        """
        Traverses structure of related models and populates the nested models
        from the database row.
        Related models can be a list if only directly related models are to be
        populated, converted to dict if related models also have their own related
        models to be populated.

        Recurrently calls from_row method on nested instances and create nested
        instances. In the end those instances are added to the final model dictionary.

        :param proxy_source_model: source model from which querysetproxy is constructed
        :type proxy_source_model: Optional[type["ModelRow"]]
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :param source_model: source model from which relation started
        :type source_model: type[Model]
        :param current_relation_str: joined related parts into one string
        :type current_relation_str: str
        :param item: dictionary of already populated nested models, otherwise empty dict
        :type item: dict
        :param row: raw result row from the database
        :type row: ResultProxy
        :param related_models: list or dict of related models
        :type related_models: Union[dict, list]
        :param plan_cache: optional per-queryset plan cache
        :type plan_cache: Optional[PlanCache]
        :param registry: optional per-query identity map
        :type registry: Optional[RowRegistry]
        :param parent_key: registry key of the model the children belong to,
            used as the scope of children reached through many-to-many
        :type parent_key: Optional[RegistryKey]
        :param scope: registry scope of the model the children belong to
        :type scope: Any
        :return: dictionary with keys corresponding to model fields names
            and values are database values, and names of relations whose
            child was reused from the registry
        :rtype: tuple[dict, set[str]]
        """
        reused: set[str] = set()

        for related in related_models:
            field = cls.ormar_config.model_fields[related]
            field = cast("ForeignKeyField", field)
            model_cls = field.to
            model_excludable = excludable.get(
                model_cls=cast(type["Model"], cls), alias=table_prefix
            )
            if model_excludable.is_excluded(related):
                continue

            relation_str, remainder = cls._process_remainder_and_relation_string(
                related_models=related_models,
                current_relation_str=current_relation_str,
                related=related,
            )
            child, child_reused = model_cls._materialize_row(
                row=row,
                source_model=source_model,
                related_models=remainder or [],
                related_field=field,
                excludable=excludable,
                current_relation_str=relation_str,
                proxy_source_model=proxy_source_model,
                plan_cache=plan_cache,
                registry=registry,
                scope=parent_key if field.is_multi else scope,
            )
            item[related] = child
            if child_reused:
                reused.add(related)
            elif (
                field.is_multi
                and child
                and not model_excludable.is_excluded(field.through.get_name())
            ):
                cls._populate_through_instance(
                    row=row,
                    item=item,
                    related=related,
                    relation_str=relation_str,
                    excludable=excludable,
                    child=child,
                    proxy_source_model=proxy_source_model,
                    plan_cache=plan_cache,
                )

        return item, reused

    @staticmethod
    def _process_remainder_and_relation_string(
        related_models: Union[dict, list],
        current_relation_str: Optional[str],
        related: str,
    ) -> tuple[str, Optional[Union[dict, list]]]:
        """
        Process remainder models and relation string

        :param related_models: list or dict of related models
        :type related_models: Union[dict, list]
        :param current_relation_str: current relation string
        :type current_relation_str: Optional[str]
        :param related: name of the relation
        :type related: str
        """
        relation_str = (
            "__".join([current_relation_str, related])
            if current_relation_str
            else related
        )

        remainder = None
        if isinstance(related_models, dict) and related_models[related]:
            remainder = related_models[related]
        return relation_str, remainder

    @classmethod
    def _populate_through_instance(  # noqa: CFQ002
        cls,
        row: ResultProxy,
        item: dict,
        related: str,
        relation_str: str,
        excludable: ExcludableItems,
        child: "Model",
        proxy_source_model: Optional[type["Model"]],
        plan_cache: Optional[PlanCache] = None,
    ) -> None:
        """
        Populates the through model on reverse side of current query.
        Normally it's child class, unless the query is from queryset.

        :param row: row from db result
        :type row: ResultProxy
        :param item: parent item dict
        :type item: dict
        :param related: current relation name
        :type related: str
        :param relation_str: path of the many to many relation from the query root
        :type relation_str: str
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :param child: child item of parent
        :type child: "Model"
        :param proxy_source_model: source model from which querysetproxy is constructed
        :type proxy_source_model: type["Model"]
        :param plan_cache: optional per-queryset plan cache
        :type plan_cache: Optional[PlanCache]
        """
        through_name = cls.ormar_config.model_fields[related].through.get_name()
        through_child = cls._create_through_instance(
            row=row,
            through_name=through_name,
            relation_str=relation_str,
            excludable=excludable,
            plan_cache=plan_cache,
        )

        if child.__class__ != proxy_source_model:
            setattr(child, through_name, through_child)
        else:
            item[through_name] = through_child
        child.set_save_status(True)

    @classmethod
    def _create_through_instance(
        cls,
        row: ResultProxy,
        through_name: str,
        relation_str: str,
        excludable: ExcludableItems,
        plan_cache: Optional[PlanCache] = None,
    ) -> "ModelRow":
        """
        Initialize the through model from db row.
        Excluded all relation fields and other exclude/include set in excludable.

        :param row: loaded row from database
        :type row: sqlalchemy.engine.ResultProxy
        :param through_name: name of the through field
        :type through_name: str
        :param relation_str: path of the many to many relation from the query root
        :type relation_str: str
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :param plan_cache: optional per-queryset plan cache
        :type plan_cache: Optional[PlanCache]
        :return: initialized through model without relation
        :rtype: "ModelRow"
        """
        model_cls = cls.ormar_config.model_fields[through_name].to
        table_prefix = alias_for(f"{relation_str}__{through_name}")
        # remove relations on through field — must happen before the plan is
        # built so the plan reflects the through-model's full exclude set
        model_excludable = excludable.get(model_cls=model_cls, alias=table_prefix)
        model_excludable.set_values(
            value=model_cls.extract_related_names(), slot="exclude"
        )
        plan = model_cls.get_or_build_row_plan(table_prefix, excludable, plan_cache)
        child_dict = model_cls.apply_row_plan(plan, row, {})
        child = model_cls._construct_with_excluded(  # type: ignore
            plan.excluded_field_names, **child_dict
        )
        return child

    @classmethod
    def build_row_extraction_plan(
        cls,
        table_prefix: str,
        excludable: ExcludableItems,
    ) -> RowExtractionPlan:
        """
        Compute the per-row extraction plan for a ``(cls, table_prefix,
        excludable)`` triple — the work that previously ran inside
        ``extract_prefixed_table_columns`` for every row.

        :param table_prefix: alias of the table from alias_for
        :type table_prefix: str
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :return: cacheable plan for fast per-row extraction
        :rtype: RowExtractionPlan
        """
        selected_columns = set(
            cls.own_table_columns(
                model=cls, excludable=excludable, alias=table_prefix, use_alias=False
            )
        )
        column_prefix = table_prefix + "_" if table_prefix else ""
        column_pairs = cls._get_table_column_pairs(cls)
        mappings = tuple(
            (f"{column_prefix}{col_name}", field_name)
            for col_name, field_name in column_pairs
            if field_name in selected_columns
        )
        excluded = frozenset(
            cls.get_names_to_exclude(excludable=excludable, alias=table_prefix)
        )
        pk_name = cls.ormar_config.pkname
        pk_column = next(
            (column for column, field_name in mappings if field_name == pk_name),
            None,
        )
        return RowExtractionPlan(
            column_mappings=mappings,
            excluded_field_names=excluded,
            pk_field_name=pk_name,
            pk_column=pk_column,
        )

    @classmethod
    def get_or_build_row_plan(
        cls,
        table_prefix: str,
        excludable: ExcludableItems,
        plan_cache: Optional[PlanCache],
    ) -> RowExtractionPlan:
        """
        Return a plan from the per-queryset cache, falling back to the plans
        shared between querysets. When ``plan_cache`` is ``None`` (legacy /
        external caller) only the shared plans are used.

        :param table_prefix: alias of the table from alias_for
        :type table_prefix: str
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :param plan_cache: per-queryset cache keyed by
            ``(cls, table_prefix, id(excludable))``; ``None`` to bypass
        :type plan_cache: Optional[PlanCache]
        :return: extraction plan for this row position
        :rtype: RowExtractionPlan
        """
        if plan_cache is None:
            return cls.get_shared_row_plan(table_prefix, excludable)
        key = (cls, table_prefix, id(excludable))
        plan = plan_cache.get(key)
        if plan is None:
            plan = cls.get_shared_row_plan(table_prefix, excludable)
            plan_cache[key] = plan
        return plan

    @classmethod
    def get_shared_row_plan(
        cls, table_prefix: str, excludable: ExcludableItems
    ) -> RowExtractionPlan:
        """
        Return a plan shared between querysets for the model's own
        include/exclude/pk_only settings at the given table prefix.

        :param table_prefix: alias of the table from alias_for
        :type table_prefix: str
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :return: extraction plan for this row position
        :rtype: RowExtractionPlan
        """
        model_excludable = excludable.get(model_cls=cls, alias=table_prefix)  # type: ignore
        return build_shared_row_plan(
            cls,
            table_prefix,
            frozenset(model_excludable.include),
            frozenset(model_excludable.exclude),
            model_excludable.pk_only,
        )

    @staticmethod
    def apply_row_plan(
        plan: RowExtractionPlan,
        row: ResultProxy,
        item: dict,
    ) -> dict:
        """
        Populate ``item`` from ``row`` using ``plan.column_mappings``. Skips
        keys already present so a partially populated dict (e.g. from
        ``_populate_nested_models_from_row``) is not overwritten.

        :param plan: precomputed extraction plan
        :type plan: RowExtractionPlan
        :param row: raw result row from the database
        :type row: sqlalchemy.engine.result.ResultProxy
        :param item: dict to populate in place
        :type item: dict
        :return: ``item`` (returned for chaining symmetry with the legacy API)
        :rtype: dict
        """
        for prefixed_name, field_name in plan.column_mappings:
            if field_name not in item:
                item[field_name] = row[prefixed_name]
        return item
