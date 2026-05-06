from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Union, cast

try:
    from sqlalchemy.engine.result import ResultProxy  # type: ignore
except ImportError:  # pragma: no cover
    from sqlalchemy.engine.result import Row as ResultProxy  # type: ignore

from ormar.models import NewBaseModel  # noqa: I202
from ormar.models.excludable import ExcludableItems
from ormar.models.helpers.models import group_related_list

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
    """

    column_mappings: tuple[tuple[str, str], ...]
    excluded_field_names: frozenset[str]
    pk_field_name: str


PlanCache = dict[tuple[type, str, int], RowExtractionPlan]


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
        used_prefixes: Optional[list[str]] = None,
        plan_cache: Optional[PlanCache] = None,
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

        :param used_prefixes: list of already extracted prefixes
        :type used_prefixes: list[str]
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
        :return: returns model if model is populated from database
        :rtype: Optional[Model]
        """
        item: dict[str, Any] = {}
        select_related = select_related or []
        related_models = related_models or []
        table_prefix = ""
        used_prefixes = used_prefixes if used_prefixes is not None else []
        excludable = excludable or ExcludableItems()

        if select_related:
            related_models = group_related_list(select_related)

        if related_field:
            table_prefix = cls._process_table_prefix(
                source_model=source_model,
                current_relation_str=current_relation_str,
                related_field=related_field,
                used_prefixes=used_prefixes,
            )

        item = cls._populate_nested_models_from_row(
            item=item,
            row=row,
            related_models=related_models,
            excludable=excludable,
            current_relation_str=current_relation_str,
            source_model=source_model,  # type: ignore
            proxy_source_model=proxy_source_model,  # type: ignore
            table_prefix=table_prefix,
            used_prefixes=used_prefixes,
            plan_cache=plan_cache,
        )
        plan = cls.get_or_build_row_plan(table_prefix, excludable, plan_cache)
        item = cls.apply_row_plan(plan, row, item)

        instance: Optional["Model"] = None
        if item.get(plan.pk_field_name, None) is not None:
            instance = cast(
                "Model",
                cls._construct_with_excluded(plan.excluded_field_names, **item),
            )
            instance.set_save_status(True)
        return instance

    @classmethod
    def _process_table_prefix(
        cls,
        source_model: type["Model"],
        current_relation_str: str,
        related_field: "ForeignKeyField",
        used_prefixes: list[str],
    ) -> str:
        """

        :param source_model: model on which relation was defined
        :type source_model: type[Model]
        :param current_relation_str: current relation string
        :type current_relation_str: str
        :param related_field: field with relation declaration
        :type related_field: "ForeignKeyField"
        :param used_prefixes: list of already extracted prefixes
        :type used_prefixes: list[str]
        :return: table_prefix to use
        :rtype: str
        """
        if related_field.is_multi:
            previous_model = related_field.through
        else:
            previous_model = related_field.owner
        table_prefix = cls.ormar_config.alias_manager.resolve_relation_alias(
            from_model=previous_model, relation_name=related_field.name
        )
        if not table_prefix or table_prefix in used_prefixes:
            manager = cls.ormar_config.alias_manager
            table_prefix = manager.resolve_relation_alias_after_complex(
                source_model=source_model,
                relation_str=current_relation_str,
                relation_field=related_field,
            )
        used_prefixes.append(table_prefix)
        return table_prefix

    @classmethod
    def _populate_nested_models_from_row(  # noqa: CFQ002
        cls,
        item: dict,
        row: ResultProxy,
        source_model: type["Model"],
        related_models: Any,
        excludable: ExcludableItems,
        table_prefix: str,
        used_prefixes: list[str],
        current_relation_str: Optional[str] = None,
        proxy_source_model: Optional[type["Model"]] = None,
        plan_cache: Optional[PlanCache] = None,
    ) -> dict:
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
        :return: dictionary with keys corresponding to model fields names
        and values are database values
        :rtype: dict
        """

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
            child = model_cls.from_row(
                row,
                related_models=remainder,
                related_field=field,
                excludable=excludable,
                current_relation_str=relation_str,
                source_model=source_model,
                proxy_source_model=proxy_source_model,
                used_prefixes=used_prefixes,
                plan_cache=plan_cache,
            )
            item[model_cls.get_column_name_from_alias(related)] = child
            if (
                field.is_multi
                and child
                and not model_excludable.is_excluded(field.through.get_name())
            ):
                cls._populate_through_instance(
                    row=row,
                    item=item,
                    related=related,
                    excludable=excludable,
                    child=child,
                    proxy_source_model=proxy_source_model,
                    plan_cache=plan_cache,
                )

        return item

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
            related=related,
            through_name=through_name,
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
        related: str,
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
        :param related: name of the relation
        :type related: str
        :param excludable: structure of fields to include and exclude
        :type excludable: ExcludableItems
        :param plan_cache: optional per-queryset plan cache
        :type plan_cache: Optional[PlanCache]
        :return: initialized through model without relation
        :rtype: "ModelRow"
        """
        model_cls = cls.ormar_config.model_fields[through_name].to
        table_prefix = cls.ormar_config.alias_manager.resolve_relation_alias(
            from_model=cls, relation_name=related
        )
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

        :param table_prefix: prefix of the table from AliasManager
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
        return RowExtractionPlan(
            column_mappings=mappings,
            excluded_field_names=excluded,
            pk_field_name=cls.ormar_config.pkname,
        )

    @classmethod
    def get_or_build_row_plan(
        cls,
        table_prefix: str,
        excludable: ExcludableItems,
        plan_cache: Optional[PlanCache],
    ) -> RowExtractionPlan:
        """
        Return a cached plan for the given key, or build and cache one. When
        ``plan_cache`` is ``None`` (legacy / external caller) the plan is
        built fresh on every call so behavior matches the pre-cache shape.

        :param table_prefix: prefix of the table from AliasManager
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
            return cls.build_row_extraction_plan(table_prefix, excludable)
        key = (cls, table_prefix, id(excludable))
        plan = plan_cache.get(key)
        if plan is None:
            plan = cls.build_row_extraction_plan(table_prefix, excludable)
            plan_cache[key] = plan
        return plan

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
