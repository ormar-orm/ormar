import asyncio
import functools
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Generic,
    Iterable,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

import sqlalchemy

import ormar  # noqa I100
from ormar import MultipleMatches, NoMatch
from ormar.exceptions import (
    ModelListEmptyError,
    ModelPersistenceError,
    QueryDefinitionError,
)
from ormar.models.row_registry import RowRegistry
from ormar.queryset import FieldAccessor, FilterQuery, SelectAction, templates
from ormar.queryset.actions.order_action import OrderAction
from ormar.queryset.clause import FilterGroup, QueryClause
from ormar.queryset.queries.prefetch_query import PrefetchQuery
from ormar.queryset.queries.query import Query
from ormar.queryset.templates import (
    SlotBinder,
    StatementTemplate,
    aggregate_statement,
    build_bulk_update,
    cached_statement,
    canonical_actions,
    clauses_shape,
    count_statement,
    excludable_shape,
    exists_statement,
    insert_statement,
    insert_template,
    order_shape,
    release_values,
    template_cache_for,
    template_params,
    value_binds,
    value_params,
)
from ormar.queryset.utils import (
    extract_access_chains,
    get_relationship_alias_model_and_str,
    normalize_slice,
)
from ormar.queryset.values_columns import values_column_map

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model
    from ormar.models import T
    from ormar.models.excludable import ExcludableItems, Slot
    from ormar.models.ormar_config import OrmarConfig
else:
    T = TypeVar("T", bound="Model")


class QuerySet(Generic[T]):
    """
    Main class to perform database queries, exposed on each model as objects attribute.
    """

    def __init__(  # noqa CFQ002
        self,
        model_cls: Optional[type["T"]] = None,
        filter_clauses: Optional[list] = None,
        exclude_clauses: Optional[list] = None,
        select_related: Optional[list] = None,
        limit_count: Optional[int] = None,
        offset: Optional[int] = None,
        excludable: Optional["ExcludableItems"] = None,
        order_bys: Optional[list] = None,
        prefetch_related: Optional[list] = None,
        limit_raw_sql: bool = False,
        proxy_source_model: Optional[type["Model"]] = None,
        reverse_result: bool = False,
    ) -> None:
        self.proxy_source_model = proxy_source_model
        self.model_cls = model_cls
        self.filter_clauses = [] if filter_clauses is None else filter_clauses
        self.exclude_clauses = [] if exclude_clauses is None else exclude_clauses
        self._select_related = [] if select_related is None else select_related
        self._prefetch_related = [] if prefetch_related is None else prefetch_related
        self.limit_count = limit_count
        self.query_offset = offset
        self._excludable = excludable or ormar.ExcludableItems()
        self.order_bys = order_bys or []
        self.limit_sql_raw = limit_raw_sql
        self._reverse_result = reverse_result

    @property
    def model_config(self) -> "OrmarConfig":
        """
        Shortcut to model class OrmarConfig set on QuerySet model.

        :return: OrmarConfig of the model
        :rtype: model's OrmarConfig
        """
        if not self.model_cls:  # pragma nocover
            raise ValueError("Model class of QuerySet is not initialized")
        return self.model_cls.ormar_config

    @property
    def model(self) -> type["T"]:
        """
        Shortcut to model class set on QuerySet.

        :return: model class
        :rtype: type[Model]
        """
        if not self.model_cls:  # pragma nocover
            raise ValueError("Model class of QuerySet is not initialized")
        return self.model_cls

    def rebuild_self(  # noqa: CFQ002
        self,
        filter_clauses: Optional[list] = None,
        exclude_clauses: Optional[list] = None,
        select_related: Optional[list] = None,
        limit_count: Optional[int] = None,
        offset: Optional[int] = None,
        excludable: Optional["ExcludableItems"] = None,
        order_bys: Optional[list] = None,
        prefetch_related: Optional[list] = None,
        limit_raw_sql: Optional[bool] = None,
        proxy_source_model: Optional[type["Model"]] = None,
        reverse_result: Optional[bool] = None,
    ) -> "QuerySet":
        """
        Method that returns new instance of queryset based on passed params,
        all not passed params are taken from current values.
        """
        overwrites = {
            "select_related": "_select_related",
            "offset": "query_offset",
            "excludable": "_excludable",
            "prefetch_related": "_prefetch_related",
            "limit_raw_sql": "limit_sql_raw",
            "reverse_result": "_reverse_result",
        }
        passed_args = locals()

        def replace_if_none(arg_name: str) -> Any:
            if passed_args.get(arg_name) is None:
                return getattr(self, overwrites.get(arg_name, arg_name))
            return passed_args.get(arg_name)

        return self.__class__(
            model_cls=self.model_cls,
            filter_clauses=replace_if_none("filter_clauses"),
            exclude_clauses=replace_if_none("exclude_clauses"),
            select_related=replace_if_none("select_related"),
            limit_count=replace_if_none("limit_count"),
            offset=replace_if_none("offset"),
            excludable=replace_if_none("excludable"),
            order_bys=replace_if_none("order_bys"),
            prefetch_related=replace_if_none("prefetch_related"),
            limit_raw_sql=replace_if_none("limit_raw_sql"),
            proxy_source_model=replace_if_none("proxy_source_model"),
            reverse_result=replace_if_none("reverse_result"),
        )

    async def _prefetch_related_models(
        self, models: list["T"], rows: list
    ) -> list["T"]:
        """
        Performs prefetch query for selected models names.

        :param models: list of already parsed main Models from main query
        :type models: list[Model]
        :param rows: database rows from main query
        :type rows: list[sqlalchemy.engine.result.RowProxy]
        :return: list of models with prefetch models populated
        :rtype: list[Model]
        """
        query = PrefetchQuery(
            model_cls=self.model,
            excludable=self._excludable,
            prefetch_related=self._prefetch_related,
            select_related=self._select_related,
            orders_by=self.order_bys,
        )
        return await query.prefetch_related(models=models)  # type: ignore

    async def _process_query_result_rows(
        self, rows: list, plan_cache: Optional[dict] = None
    ) -> list["T"]:
        """
        Process database rows and initialize ormar Model from each of the rows.

        :param rows: list of database rows from query result
        :type rows: list[sqlalchemy.engine.result.RowProxy]
        :param plan_cache: optional row-extraction plan cache; ``iterate``
            passes a single dict shared across all chunks so amortization
            survives the per-chunk ``_process_query_result_rows`` boundary
        :type plan_cache: Optional[dict]
        :return: list of models
        :rtype: list[Model]
        """
        if plan_cache is None:
            plan_cache = {}
        registry = RowRegistry()
        result_rows = []
        for i, row in enumerate(rows):
            result_rows.append(
                self.model.from_row(
                    row=row,
                    select_related=self._select_related,
                    excludable=self._excludable,
                    source_model=self.model,
                    proxy_source_model=self.proxy_source_model,
                    plan_cache=plan_cache,
                    registry=registry,
                )
            )
            if i % 100 == 99:  # pragma: no cover
                await asyncio.sleep(0)

        if result_rows:
            return self.model.merge_instances_list(  # type: ignore[return-value]
                result_rows,  # type: ignore[arg-type]
                excludable=self._excludable,
            )
        return cast(list["T"], result_rows)

    def _resolve_filter_groups(
        self, groups: Any
    ) -> tuple[list[FilterGroup], list[str]]:
        """
        Resolves filter groups to populate FilterAction params in group tree.

        :param groups: tuple of FilterGroups
        :type groups: Any
        :return: list of resolver groups
        :rtype: tuple[list[FilterGroup], list[str]]
        """
        filter_groups = []
        select_related = self._select_related
        if groups:
            for group in groups:
                if not isinstance(group, FilterGroup):
                    raise QueryDefinitionError(
                        "Only ormar.and_ and ormar.or_ "
                        "can be passed as filter positional"
                        " arguments,"
                        "other values need to be passed by"
                        "keyword arguments"
                    )
                _, select_related = group.resolve(
                    model_cls=self.model,
                    select_related=self._select_related,
                    filter_clauses=self.filter_clauses,
                )
                filter_groups.append(group)
        return filter_groups, select_related

    @staticmethod
    def check_single_result_rows_count(rows: Sequence[Optional["T"]]) -> None:
        """
        Verifies if the result has one and only one row.

        :param rows: one element list of Models
        :type rows: list[Model]
        """
        if not rows or rows[0] is None:
            raise NoMatch()
        if len(rows) > 1:
            raise MultipleMatches()

    @property
    def table(self) -> sqlalchemy.Table:
        """
        Shortcut to models table from OrmarConfig.

        :return: database table
        :rtype: sqlalchemy.Table
        """
        return self.model_config.table

    def build_select_expression(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_bys: Optional[list] = None,
        binder: Optional[SlotBinder] = None,
    ) -> sqlalchemy.sql.Select:
        """
        Constructs the actual database query used in the QuerySet.
        If any of the params is not passed the QuerySet own value is used.

        :param limit: number to limit the query
        :type limit: int
        :param offset: number to offset by
        :type offset: int
        :param order_bys: list of order-by fields names
        :type order_bys: list
        :param binder: names per-call values for statement templates, if given
        :type binder: Optional[SlotBinder]
        :return: built sqlalchemy select expression
        :rtype: sqlalchemy.sql.selectable.Select
        """
        qry = Query(
            model_cls=self.model,
            select_related=self._select_related,
            filter_clauses=self.filter_clauses,
            exclude_clauses=self.exclude_clauses,
            offset=offset or self.query_offset,
            excludable=self._excludable,
            order_bys=order_bys or self.order_bys,
            limit_raw_sql=self.limit_sql_raw,
            limit_count=limit if limit is not None else self.limit_count,
            binder=binder,
        )
        exp = qry.build_select_expression()
        # print("\n", exp.compile(compile_kwargs={"literal_binds": True}))
        return exp

    def build_select_template(
        self,
        variant: tuple = (),
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_bys: Optional[list] = None,
        wrap: Optional[Callable[[sqlalchemy.sql.Select], Any]] = None,
    ) -> StatementTemplate:
        """
        Return the read statement for this queryset from the model's template
        cache (building and caching it on the first call of a shape) together
        with the values to bind for this call.

        :param variant: how the select is used (plain, count, aggregate, ...)
        :type variant: tuple
        :param limit: number to limit the query
        :type limit: Optional[int]
        :param offset: number to offset by
        :type offset: Optional[int]
        :param order_bys: list of order actions
        :type order_bys: Optional[list]
        :param wrap: turns the built select into the executed statement
        :type wrap: Optional[Callable[[sqlalchemy.sql.Select], Any]]
        :return: statement and values to bind
        :rtype: StatementTemplate
        """
        actions = canonical_actions(
            filter_clauses=self.filter_clauses, exclude_clauses=self.exclude_clauses
        )
        params = template_params(
            actions=actions,
            limit=limit if limit is not None else self.limit_count,
            offset=offset or self.query_offset,
        )
        key = self.template_key(
            variant=variant, limit=limit, offset=offset, order_bys=order_bys
        )
        cache = template_cache_for(model_cls=self.model)
        statement = cache.get(key)
        if statement is None:
            statement = self.build_template_statement(
                actions=actions,
                limit=limit,
                offset=offset,
                order_bys=order_bys,
                wrap=wrap,
            )
            cache.put(key=key, statement=statement)
        elif templates.CROSS_CHECK:
            fresh = self.build_select_expression(
                limit=limit, offset=offset, order_bys=order_bys
            )
            fresh = wrap(fresh) if wrap is not None else fresh
            templates.assert_same_statement(
                cached=statement,
                fresh=fresh,
                params=params,
                dialect=self.model_config.database.dialect,
            )
        return StatementTemplate(statement, params)

    def build_template_statement(
        self,
        actions: list,
        limit: Optional[int],
        offset: Optional[int],
        order_bys: Optional[list],
        wrap: Optional[Callable[[sqlalchemy.sql.Select], Any]],
    ) -> Any:
        """
        Build the read statement with named value slots.

        :param actions: filter actions in canonical order
        :type actions: list
        :param limit: number to limit the query
        :type limit: Optional[int]
        :param offset: number to offset by
        :type offset: Optional[int]
        :param order_bys: list of order actions
        :type order_bys: Optional[list]
        :param wrap: turns the built select into the executed statement
        :type wrap: Optional[Callable[[sqlalchemy.sql.Select], Any]]
        :return: statement to execute
        :rtype: Any
        """
        binder = SlotBinder(actions=actions)
        select = self.build_select_expression(
            limit=limit, offset=offset, order_bys=order_bys, binder=binder
        )
        statement = wrap(select) if wrap is not None else select
        release_values(statement=statement)
        return statement

    def build_write_template(self, kind: str, values: dict) -> StatementTemplate:
        """
        Return the filtered update (``kind="update"``) or delete of this
        queryset from the model's template cache, with the values to bind.

        :param kind: ``"update"`` or ``"delete"``
        :type kind: str
        :param values: updated values by column name (empty for delete)
        :type values: dict
        :return: statement and values to bind
        :rtype: StatementTemplate
        """
        actions = canonical_actions(self.filter_clauses, self.exclude_clauses)
        names = tuple(sorted(values))
        params = {
            **template_params(actions, None, None),
            **value_params(names, values),
        }
        key = (
            kind,
            clauses_shape(self.filter_clauses),
            clauses_shape(self.exclude_clauses),
            names,
        )
        cache = template_cache_for(self.model)
        statement = cache.get(key)
        if statement is None:
            statement = self.build_write_statement(kind, names, SlotBinder(actions))
            release_values(statement)
            cache.put(key, statement)
        elif templates.CROSS_CHECK:
            fresh = self.build_write_statement(kind, names, None, values)
            templates.assert_same_statement(
                statement, fresh, params, self.model_config.database.dialect
            )
        return StatementTemplate(statement, params)

    def build_write_statement(
        self,
        kind: str,
        names: tuple,
        binder: Optional[SlotBinder],
        values: Optional[dict] = None,
    ) -> Any:
        """
        Build the filtered update or delete, with named slots when a binder is
        given, otherwise with the literal values (as without templates).

        :param kind: ``"update"`` or ``"delete"``
        :type kind: str
        :param names: updated column names, sorted
        :type names: tuple
        :param binder: names filter values for templates, if given
        :type binder: Optional[SlotBinder]
        :param values: literal updated values, used without a binder
        :type values: Optional[dict]
        :return: statement to execute
        :rtype: Any
        """
        table = self.table
        expr: Any
        if kind == "update":
            assignments = (
                value_binds(table, names)
                if binder is not None
                else {table.c[name]: (values or {})[name] for name in names}
            )
            expr = table.update().values(assignments)
        else:
            expr = table.delete()
        expr = FilterQuery(filter_clauses=self.filter_clauses, binder=binder).apply(
            expr
        )
        return FilterQuery(
            filter_clauses=self.exclude_clauses, exclude=True, binder=binder
        ).apply(expr)

    def effective_limit_shape(self, limit: Optional[int]) -> tuple:
        """
        Return how the effective limit shapes the SQL: whether a LIMIT is
        applied at all, and whether it is non-zero (only a non-zero limit with
        ``select_related`` builds the pagination subquery).

        :param limit: limit passed to ``build_select_expression``
        :type limit: Optional[int]
        :return: limit presence and truthiness
        :rtype: tuple
        """
        effective = limit if limit is not None else self.limit_count
        return effective is not None, bool(effective)

    def template_key(
        self,
        variant: tuple,
        limit: Optional[int],
        offset: Optional[int],
        order_bys: Optional[list],
    ) -> tuple:
        """
        Return the shape of the statement ``build_select_expression`` would build
        with the given arguments: everything that changes its SQL text and none
        of the bound values.

        :param variant: how the select is used (plain, count, aggregate, ...)
        :type variant: tuple
        :param limit: limit passed to ``build_select_expression``
        :type limit: Optional[int]
        :param offset: offset passed to ``build_select_expression``
        :type offset: Optional[int]
        :param order_bys: order actions passed to ``build_select_expression``
        :type order_bys: Optional[list]
        :return: hashable statement shape
        :rtype: tuple
        """
        return (
            variant,
            clauses_shape(clauses=self.filter_clauses),
            clauses_shape(clauses=self.exclude_clauses),
            tuple(self._select_related),
            excludable_shape(excludable=self._excludable),
            order_shape(order_bys=order_bys or self.order_bys),
            self.effective_limit_shape(limit=limit),
            bool(offset or self.query_offset),
            self.limit_sql_raw,
        )

    def filter(  # noqa: A003
        self, *args: Any, _exclude: bool = False, **kwargs: Any
    ) -> "QuerySet[T]":
        """
        Allows you to filter by any `Model` attribute/field
        as well as to fetch instances, with a filter across an FK relationship.

        You can use special filter suffix to change the filter operands:

        *  exact - like `album__name__exact='Malibu'` (exact match)
        *  iexact - like `album__name__iexact='malibu'` (exact match case insensitive)
        *  contains - like `album__name__contains='Mal'` (sql like)
        *  icontains - like `album__name__icontains='mal'` (sql like case insensitive)
        *  in - like `album__name__in=['Malibu', 'Barclay']` (sql in)
        *  isnull - like `album__name__isnull=True` (sql is null)
           (isnotnull `album__name__isnull=False` (sql is not null))
        *  gt - like `position__gt=3` (sql >)
        *  gte - like `position__gte=3` (sql >=)
        *  lt - like `position__lt=3` (sql <)
        *  lte - like `position__lte=3` (sql <=)
        *  startswith - like `album__name__startswith='Mal'` (exact start match)
        *  istartswith - like `album__name__istartswith='mal'` (case insensitive)
        *  endswith - like `album__name__endswith='ibu'` (exact end match)
        *  iendswith - like `album__name__iendswith='IBU'` (case insensitive)

        Note that you can also use python style filters - check the docs!

        :param _exclude: flag if it should be exclude or filter
        :type _exclude: bool
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: filtered QuerySet
        :rtype: QuerySet
        """
        filter_groups, select_related = self._resolve_filter_groups(groups=args)
        qryclause = QueryClause(
            model_cls=self.model,
            select_related=select_related,
            filter_clauses=self.filter_clauses,
        )
        if _exclude:
            own_clauses, select_related = qryclause.prepare_filter(
                _own_only=True, **kwargs
            )
            if not own_clauses and not filter_groups:
                return self.rebuild_self(select_related=select_related)
            # groups are already resolved, so own actions are attached directly
            exclude_group = FilterGroup(*filter_groups)
            exclude_group.actions = own_clauses
            return self.rebuild_self(
                exclude_clauses=self.exclude_clauses + [exclude_group],
                select_related=select_related,
            )
        filter_clauses, select_related = qryclause.prepare_filter(**kwargs)
        return self.rebuild_self(
            filter_clauses=filter_clauses + filter_groups,  # type: ignore
            select_related=select_related,
        )

    def exclude(self, *args: Any, **kwargs: Any) -> "QuerySet[T]":  # noqa: A003
        """
        Works exactly the same as filter and all modifiers (suffixes) are the same,
        but returns a *not* condition.

        So if you use `filter(name='John')` which is `where name = 'John'` in SQL,
        the `exclude(name='John')` equals to `where name <> 'John'`

        Note that all conditions are joined so if you pass multiple values it
        becomes a union of conditions.

        `exclude(name='John', age>=35)` will become
        `where not (name='John' and age>=35)`

        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: filtered QuerySet
        :rtype: QuerySet
        """
        return self.filter(_exclude=True, *args, **kwargs)

    def select_related(self, related: Union[list, str, FieldAccessor]) -> "QuerySet[T]":
        """
        Allows to prefetch related models during the same query.

        **With `select_related` always only one query is run against the database**,
        meaning that one (sometimes complicated) join is generated and later nested
        models are processed in python.

        To fetch related model use `ForeignKey` names.

        To chain related `Models` relation use double underscores between names.

        :param related: list of relation field names, can be linked by '__' to nest
        :type related: Union[list, str]
        :return: QuerySet
        :rtype: QuerySet
        """
        if not isinstance(related, list):
            related = [related]
        related = cast(list, extract_access_chains(related))

        related = sorted(list(set(list(self._select_related) + related)))
        return self.rebuild_self(select_related=related)

    def select_all(self, follow: bool = False) -> "QuerySet[T]":
        """
        By default adds only directly related models.

        If follow=True is set it adds also related models of related models.

        To not get stuck in an infinite loop as related models also keep a relation
        to parent model visited models set is kept.

        That way already visited models that are nested are loaded, but the load do not
        follow them inside. So Model A -> Model B -> Model C -> Model A -> Model X
        will load second Model A but will never follow into Model X.
        Nested relations of those kind need to be loaded manually.

        :param follow: flag to trigger deep save -
        by default only directly related models are saved
        with follow=True also related models of related models are saved
        :type follow: bool
        :return: reloaded Model
        :rtype: Model
        """
        relations = list(self.model.extract_related_names())
        if follow:
            relations = self.model._iterate_related_models()
        return self.rebuild_self(select_related=relations)

    def prefetch_related(
        self, related: Union[list, str, FieldAccessor]
    ) -> "QuerySet[T]":
        """
        Allows to prefetch related models during query - but opposite to
        `select_related` each subsequent model is fetched in a separate database query.

        **With `prefetch_related` always one query per Model is run against the
        database**, meaning that you will have multiple queries executed one
        after another.

        To fetch related model use `ForeignKey` names.

        To chain related `Models` relation use double underscores between names.

        :param related: list of relation field names, can be linked by '__' to nest
        :type related: Union[list, str]
        :return: QuerySet
        :rtype: QuerySet
        """
        if not isinstance(related, list):
            related = [related]
        related = cast(list, extract_access_chains(related))

        related = list(set(list(self._prefetch_related) + related))
        return self.rebuild_self(prefetch_related=related)

    def flatten_fields(
        self,
        columns: Union[list, str, set, tuple, dict, FieldAccessor],
    ) -> "QuerySet[T]":
        """
        Render selected related models as their primary-key value on
        ``model_dump()`` instead of the default nested dict.

        Accepts the same input forms as :py:meth:`fields` / :py:meth:`exclude_fields`
        plus ``FieldAccessor`` / list of accessors. Missing relations are
        auto-loaded: single-valued foreign keys go into ``select_related``,
        many-to-many and reverse relations into ``prefetch_related``.

        Chained calls merge. A flatten directive conflicts with any include or
        exclude sub-field selection on the same relation (scalar output has no
        place for children).

        :param columns: relations to flatten on serialization
        :type columns: Union[list, str, set, tuple, dict, FieldAccessor]
        :return: new QuerySet with the flatten spec applied
        :rtype: QuerySet[T]
        """
        normalized = extract_access_chains(columns)

        excludable = ormar.ExcludableItems.from_excludable(self._excludable)
        excludable.build(
            items=normalized,
            model_cls=self.model_cls,  # type: ignore
            slot="flatten",
        )
        excludable.validate_flatten_vs_excludable(
            source_model=self.model_cls  # type: ignore
        )

        select_to_add, prefetch_to_add = self._classify_flatten_paths(
            excludable._flatten_paths
        )
        new_select = sorted(set(self._select_related) | select_to_add)
        new_prefetch = sorted(set(self._prefetch_related) | prefetch_to_add)
        return self.rebuild_self(
            excludable=excludable,
            select_related=new_select,
            prefetch_related=new_prefetch,
        )

    def _classify_flatten_paths(
        self, paths: Iterable[tuple[str, ...]]
    ) -> tuple[set[str], set[str]]:
        """
        Split flatten tuple paths into ``select_related`` (single-valued FK,
        incl. self-ref) and ``prefetch_related`` (m2m / reverse) buckets so
        the caller can auto-add whichever the user hasn't loaded already.

        :param paths: pre-split tuple paths collected during flatten build
        :type paths: Iterable[tuple[str, ...]]
        :return: (paths for select_related, paths for prefetch_related) — each
            as dunder strings for downstream ``select_related`` /
            ``prefetch_related`` call sites
        :rtype: tuple[set[str], set[str]]
        """
        select_paths: set[str] = set()
        prefetch_paths: set[str] = set()
        source_model = self.model
        for parts in paths:
            parent_model: type["Model"] = source_model
            if len(parts) > 1:
                _, parent_model, _, _ = get_relationship_alias_model_and_str(
                    source_model=source_model,
                    related_parts=list(parts[:-1]),
                )
            field = parent_model.ormar_config.model_fields[parts[-1]]
            bucket = (
                prefetch_paths
                if getattr(field, "is_multi", False) or getattr(field, "virtual", False)
                else select_paths
            )
            bucket.add("__".join(parts))
        return select_paths, prefetch_paths

    def fields(
        self, columns: Union[list, str, set, dict], slot: "Slot" = "include"
    ) -> "QuerySet[T]":
        """
        With `fields()` you can select subset of model columns to limit the data load.

        Note that `fields()` and `exclude_fields()` works both for main models
        (on normal queries like `get`, `all` etc.)
        as well as `select_related` and `prefetch_related`
        models (with nested notation).

        You can select specified fields by passing a `str, list[str], set[str] or
        dict` with nested definition.

        To include related models use notation
        `{related_name}__{column}[__{optional_next} etc.]`.

        `fields()` can be called several times, building up the columns to select.

        If you include related models into `select_related()` call but you won't specify
        columns for those models in fields - implies a list of all fields for
        those nested models.

        Mandatory fields cannot be excluded as it will raise `ValidationError`,
        to exclude a field it has to be nullable.

        Pk column cannot be excluded - it's always auto added even if
        not explicitly included.

        You can also pass fields to include as dictionary or set.

        To mark a field as included in a dictionary use it's name as key
        and ellipsis as value.

        To traverse nested models use nested dictionaries.

        To include fields at last level instead of nested dictionary a set can be used.

        To include whole nested model specify model related field name and ellipsis.

        :param slot: which Excludable slot to write into ("include" or "exclude")
        :type slot: Slot
        :param columns: columns to include
        :type columns: Union[list, str, set, dict]
        :return: QuerySet
        :rtype: QuerySet
        """
        excludable = ormar.ExcludableItems.from_excludable(self._excludable)
        excludable.build(
            items=columns,
            model_cls=self.model_cls,  # type: ignore
            slot=slot,
        )
        if excludable._flatten_paths:
            excludable.validate_flatten_vs_excludable(
                source_model=self.model_cls  # type: ignore
            )

        return self.rebuild_self(excludable=excludable)

    def exclude_fields(self, columns: Union[list, str, set, dict]) -> "QuerySet[T]":
        """
        With `exclude_fields()` you can select subset of model columns that will
        be excluded to limit the data load.

        It's the opposite of `fields()` method so check documentation above
        to see what options are available.

        Especially check above how you can pass also nested dictionaries
        and sets as a mask to exclude fields from whole hierarchy.

        Note that `fields()` and `exclude_fields()` works both for main models
        (on normal queries like `get`, `all` etc.)
        as well as `select_related` and `prefetch_related` models
        (with nested notation).

        Mandatory fields cannot be excluded as it will raise `ValidationError`,
        to exclude a field it has to be nullable.

        Pk column cannot be excluded - it's always auto added even
        if explicitly excluded.

        :param columns: columns to exclude
        :type columns: Union[list, str, set, dict]
        :return: QuerySet
        :rtype: QuerySet
        """
        return self.fields(columns=columns, slot="exclude")

    def order_by(self, columns: Union[list, str, OrderAction]) -> "QuerySet[T]":
        """
        With `order_by()` you can order the results from database based on your
        choice of fields.

        You can provide a string with field name or list of strings with fields names.

        Ordering in sql will be applied in order of names you provide in order_by.

        By default if you do not provide ordering `ormar` explicitly orders by
        all primary keys

        If you are sorting by nested models that causes that the result rows are
        unsorted by the main model `ormar` will combine those children rows into
        one main model.

        The main model will never duplicate in the result

        To order by main model field just provide a field name

        To sort on nested models separate field names with dunder '__'.

        You can sort this way across all relation types -> `ForeignKey`,
        reverse virtual FK and `ManyToMany` fields.

        To sort in descending order provide a hyphen in front of the field name

        :param columns: columns by which models should be sorted
        :type columns: Union[list, str]
        :return: QuerySet
        :rtype: QuerySet
        """
        if not isinstance(columns, list):
            columns = [columns]

        orders_by = [
            (
                OrderAction(order_str=x, model_cls=self.model_cls)  # type: ignore
                if not isinstance(x, OrderAction)
                else x
            )
            for x in columns
        ]

        order_bys = self.order_bys + [x for x in orders_by if x not in self.order_bys]
        return self.rebuild_self(order_bys=order_bys)

    async def values(
        self,
        fields: Union[list, str, set, dict, None] = None,
        exclude_through: bool = False,
        _as_dict: bool = True,
        _flatten: bool = False,
    ) -> list:
        """
        Return a list of dictionaries with column values in order of the fields
        passed or all fields from queried models.

        To filter for given row use filter/exclude methods before values,
        to limit number of rows use limit/offset or paginate before values.

        Note that it always return a list even for one row from database.

        :param exclude_through: flag if through models should be excluded
        :type exclude_through: bool
        :param _flatten: internal parameter to flatten one element tuples
        :type _flatten: bool
        :param _as_dict: internal parameter if return dict or tuples
        :type _as_dict: bool
        :param fields: field name or list of field names to extract from db
        :type fields:  Union[list, str, set, dict]
        """
        if fields:
            return await self.fields(columns=fields).values(
                _as_dict=_as_dict, _flatten=_flatten, exclude_through=exclude_through
            )
        excludable = self._excludable.with_projection_exclusions(
            source_model=self.model,
            select_related=self._select_related,
        )
        projected = (
            self
            if excludable is self._excludable
            else self.rebuild_self(excludable=excludable)
        )
        template = projected.build_select_template()
        async with self.model_config.database.get_query_executor() as executor:
            rows = await executor.fetch_all(template.statement, template.params)
        if not rows:
            return []
        column_map = values_column_map(
            model_cls=self.model_cls,  # type: ignore
            select_related=self._select_related,
            excludable=excludable,
            exclude_through=exclude_through,
            column_names=list(rows[0].keys()),  # type: ignore
        )
        result = [
            {column_map.get(k): v for k, v in dict(x).items() if k in column_map}
            for x in rows
        ]
        if _as_dict:
            return result
        if _flatten and self._excludable.include_entry_count() != 1:
            raise QueryDefinitionError(
                "You cannot flatten values_list if more than one field is selected!"
            )
        tuple_result = [tuple(x.values()) for x in result]
        return tuple_result if not _flatten else [x[0] for x in tuple_result]

    async def values_list(
        self,
        fields: Union[list, str, set, dict, None] = None,
        flatten: bool = False,
        exclude_through: bool = False,
    ) -> list:
        """
        Return a list of tuples with column values in order of the fields passed or
        all fields from queried models.

        When one field is passed you can flatten the list of tuples into list of values
        of that single field.

        To filter for given row use filter/exclude methods before values,
        to limit number of rows use limit/offset or paginate before values.

        Note that it always return a list even for one row from database.

        :param exclude_through: flag if through models should be excluded
        :type exclude_through: bool
        :param fields: field name or list of field names to extract from db
        :type fields: Union[str, list[str]]
        :param flatten: when one field is passed you can flatten the list of tuples
        :type flatten: bool
        """
        return await self.values(
            fields=fields,
            exclude_through=exclude_through,
            _as_dict=False,
            _flatten=flatten,
        )

    async def exists(self) -> bool:
        """
        Returns a bool value to confirm if there are rows matching the given criteria
        (applied with `filter` and `exclude` if set).

        :return: result of the check
        :rtype: bool
        """
        template = self.build_select_template(
            variant=("exists",), wrap=exists_statement
        )
        async with self.model_config.database.get_query_executor() as executor:
            result = await executor.fetch_val(
                query=template.statement, params=template.params
            )
            return bool(result)

    async def count(self, distinct: bool = True) -> int:
        """
        Returns number of rows matching the given criteria
        (applied with `filter` and `exclude` if set before).
        If `distinct` is `True` (the default), this will return
        the number of primary rows selected. If `False`,
        the count will be the total number of rows returned
        (including extra rows for `one-to-many` or `many-to-many`
        left `select_related` table joins).
        `False` is the legacy (buggy) behavior for workflows that depend on it.

        :param distinct: flag if the primary table rows should be distinct or not

        :return: number of rows
        :rtype: int
        """
        wrap = functools.partial(
            count_statement,
            distinct=distinct,
            pk_column_name=self.model.get_column_alias(
                field_name=self.model_config.pkname
            ),
        )
        template = self.build_select_template(variant=("count", distinct), wrap=wrap)
        async with self.model_config.database.get_query_executor() as executor:
            result = await executor.fetch_val(
                query=template.statement, params=template.params
            )
            return int(result) if result is not None else 0

    async def _query_aggr_function(self, func_name: str, columns: list) -> Any:
        func = getattr(sqlalchemy.func, func_name)
        select_actions = [
            SelectAction(select_str=column, model_cls=self.model) for column in columns
        ]
        if func_name in ["sum", "avg"]:
            if any(not x.is_numeric for x in select_actions):
                raise QueryDefinitionError(
                    "You can use sum and svg only with numeric types of columns"
                )
        if any(x.field_name not in x.target_model.model_fields for x in select_actions):
            raise QueryDefinitionError(
                "You can use aggregate functions only on "
                "existing columns of the target model"
            )
        select_columns = [x.apply_func(func, use_label=True) for x in select_actions]
        wrap = functools.partial(
            aggregate_statement, func_name=func_name, select_columns=select_columns
        )
        template = self.build_select_template(
            variant=("aggregate", func_name, tuple(columns)), wrap=wrap
        )
        async with self.model_config.database.get_query_executor() as executor:
            result = await executor.fetch_one(
                query=template.statement, params=template.params
            )
        return dict(result) if len(result) > 1 else result[columns[0]]  # type: ignore

    async def max(self, columns: Union[str, list[str]]) -> Any:  # noqa: A003
        """
        Returns max value of columns for rows matching the given criteria
        (applied with `filter` and `exclude` if set before).

        :return: max value of column(s)
        :rtype: Any
        """
        if not isinstance(columns, list):
            columns = [columns]
        return await self._query_aggr_function(func_name="max", columns=columns)

    async def min(self, columns: Union[str, list[str]]) -> Any:  # noqa: A003
        """
        Returns min value of columns for rows matching the given criteria
        (applied with `filter` and `exclude` if set before).

        :return: min value of column(s)
        :rtype: Any
        """
        if not isinstance(columns, list):
            columns = [columns]
        return await self._query_aggr_function(func_name="min", columns=columns)

    async def sum(self, columns: Union[str, list[str]]) -> Any:  # noqa: A003
        """
        Returns sum value of columns for rows matching the given criteria
        (applied with `filter` and `exclude` if set before).

        :return: sum value of columns
        :rtype: int
        """
        if not isinstance(columns, list):
            columns = [columns]
        return await self._query_aggr_function(func_name="sum", columns=columns)

    async def avg(self, columns: Union[str, list[str]]) -> Any:
        """
        Returns avg value of columns for rows matching the given criteria
        (applied with `filter` and `exclude` if set before).

        :return: avg value of columns
        :rtype: Union[int, float, list]
        """
        if not isinstance(columns, list):
            columns = [columns]
        return await self._query_aggr_function(func_name="avg", columns=columns)

    def _verify_no_related_filters(self, operation: str) -> None:
        """
        Raises if any filter or exclude clause targets a related model, as
        UPDATE and DELETE statements do not join the related tables.

        :param operation: name of the operation, used in the error message
        :type operation: str
        """
        for clause in self.filter_clauses + self.exclude_clauses:
            actions = clause._iter() if isinstance(clause, FilterGroup) else [clause]
            for action in actions:
                if action.related_parts:
                    raise QueryDefinitionError(
                        f"{operation}() does not support filters on related models "
                        f"('{action.query_str}'). Select the pks first and "
                        f"filter with pk__in instead."
                    )

    async def update(self, each: bool = False, **kwargs: Any) -> int:
        """
        Updates the model table after applying the filters from kwargs.

        You have to either pass a filter to narrow down a query or explicitly pass
        each=True flag to affect whole table.

        :param each: flag if whole table should be affected if no filter is passed
        :type each: bool
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: number of updated rows
        :rtype: int
        """
        if not each and not (self.filter_clauses or self.exclude_clauses):
            raise QueryDefinitionError(
                "You cannot update without filtering the queryset first. "
                "If you want to update all rows use update(each=True, **kwargs)"
            )

        self._verify_no_related_filters("update")
        self_fields = self.model.extract_db_own_fields().union(
            self.model.extract_related_names()
        )
        updates = {k: v for k, v in kwargs.items() if k in self_fields}
        updates = self.model.populate_onupdate_value(updates)
        updates = self.model.validate_enums(updates)
        updates = self.model.translate_columns_to_aliases(updates)
        unconsumed = [name for name in updates if name not in self.table.c]
        if unconsumed:
            raise sqlalchemy.exc.CompileError(
                f"Unconsumed column names: {', '.join(unconsumed)}"
            )

        template = self.build_write_template("update", updates)
        async with self.model_config.database.get_query_executor() as executor:
            return await executor.execute(template.statement, template.params)

    async def delete(self, *args: Any, each: bool = False, **kwargs: Any) -> int:
        """
        Deletes from the model table after applying the filters from kwargs.

        You have to either pass a filter to narrow down a query or explicitly pass
        each=True flag to affect whole table.

        :param each: flag if whole table should be affected if no filter is passed
        :type each: bool
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: number of deleted rows
        :rtype:int
        """
        if kwargs or args:
            return await self.filter(*args, **kwargs).delete()
        if not each and not (self.filter_clauses or self.exclude_clauses):
            raise QueryDefinitionError(
                "You cannot delete without filtering the queryset first. "
                "If you want to delete all rows use delete(each=True)"
            )
        self._verify_no_related_filters("delete")
        template = self.build_write_template("delete", {})
        async with self.model_config.database.get_query_executor() as executor:
            return await executor.execute(template.statement, template.params)

    def paginate(self, page: int, page_size: int = 20) -> "QuerySet[T]":
        """
        You can paginate the result which is a combination of offset and limit clauses.
        Limit is set to page size and offset is set to (page-1) * page_size.

        :param page_size: numbers of items per page
        :type page_size: int
        :param page: page number
        :type page: int
        :return: QuerySet
        :rtype: QuerySet
        """
        if page < 1 or page_size < 1:
            raise QueryDefinitionError("Page size and page have to be greater than 0.")

        limit_count = page_size
        query_offset = (page - 1) * page_size
        return self.rebuild_self(limit_count=limit_count, offset=query_offset)

    def limit(
        self, limit_count: int, limit_raw_sql: Optional[bool] = None
    ) -> "QuerySet[T]":
        """
        You can limit the results to desired number of parent models.

        To limit the actual number of database query rows instead of number of main
        models use the `limit_raw_sql` parameter flag, and set it to `True`.

        :param limit_raw_sql: flag if raw sql should be limited
        :type limit_raw_sql: bool
        :param limit_count: number of models to limit
        :type limit_count: int
        :return: QuerySet
        :rtype: QuerySet
        """
        limit_raw_sql = self.limit_sql_raw if limit_raw_sql is None else limit_raw_sql
        return self.rebuild_self(limit_count=limit_count, limit_raw_sql=limit_raw_sql)

    def offset(
        self, offset: int, limit_raw_sql: Optional[bool] = None
    ) -> "QuerySet[T]":
        """
        You can also offset the results by desired number of main models.

        To offset the actual number of database query rows instead of number of main
        models use the `limit_raw_sql` parameter flag, and set it to `True`.

        :param limit_raw_sql: flag if raw sql should be offset
        :type limit_raw_sql: bool
        :param offset: numbers of models to offset
        :type offset: int
        :return: QuerySet
        :rtype: QuerySet
        """
        limit_raw_sql = self.limit_sql_raw if limit_raw_sql is None else limit_raw_sql
        return self.rebuild_self(offset=offset, limit_raw_sql=limit_raw_sql)

    def _single_row_order_bys(self, reverse: bool) -> list["OrderAction"]:
        """
        Builds the ``order_bys`` list used by :meth:`first` and :meth:`last`.

        A pk-based tiebreaker is prepended only when the user has not
        supplied an ordering on the source model (otherwise the user's
        choice must win). For :meth:`last`, both the pk tiebreaker and the
        user's order_bys are flipped so that ``LIMIT 1`` pulls the tail of
        the user's natural order.

        :param reverse: whether to flip directions for the ``last()`` case
        :type reverse: bool
        :return: combined list of OrderAction objects
        :rtype: list[OrderAction]
        """
        pk_default = OrderAction(
            order_str=self.model.ormar_config.pkname,
            model_cls=self.model_cls,  # type: ignore
        )
        user_orders = self.order_bys
        if reverse:
            pk_default = pk_default.flipped()
            user_orders = [ob.flipped() for ob in user_orders]
        has_source_order = any(x.is_source_model_order for x in self.order_bys)
        prefix: list["OrderAction"] = [] if has_source_order else [pk_default]
        return prefix + list(user_orders)

    async def _fetch_single(self, order_bys: list["OrderAction"]) -> "T":
        """
        Runs a ``LIMIT 1`` select with the given ``order_bys``, merges results,
        runs any configured prefetch, and enforces the single-row contract
        (raises ``NoMatch`` / ``MultipleMatches``). Shared implementation
        backing :meth:`first` and :meth:`last`.

        :param order_bys: fully-resolved order list to apply to the query
        :type order_bys: list[OrderAction]
        :return: the single fetched model
        :rtype: Model
        """
        template = self.build_select_template(limit=1, order_bys=order_bys)
        async with self.model_config.database.get_query_executor() as executor:
            rows = await executor.fetch_all(template.statement, template.params)
        processed_rows = await self._process_query_result_rows(rows)
        if self._prefetch_related and processed_rows:
            processed_rows = await self._prefetch_related_models(processed_rows, rows)
        self.check_single_result_rows_count(processed_rows)
        return processed_rows[0]  # type: ignore

    async def first(self, *args: Any, **kwargs: Any) -> "T":
        """
        Gets the first row from the db ordered by primary key column ascending.

        Passing args and/or kwargs is a shortcut and equals to calling
        ``filter(*args, **kwargs).first()``.

        The query is always executed with ``LIMIT 1`` so at most one row is
        fetched; use :meth:`get` if you want to assert that the criteria
        match exactly one row.

        :raises NoMatch: if no rows match the criteria
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: returned model
        :rtype: Model
        """
        if kwargs or args:
            return await self.filter(*args, **kwargs).first()
        return await self._fetch_single(self._single_row_order_bys(reverse=False))

    async def first_or_none(self, *args: Any, **kwargs: Any) -> Optional["T"]:
        """
        Gets the first row from the db ordered by primary key column ascending.

        Behaves like :meth:`first` but returns ``None`` instead of raising
        ``NoMatch`` when no row matches.

        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: returned model or None
        :rtype: Optional[Model]
        """
        try:
            return await self.first(*args, **kwargs)
        except ormar.NoMatch:
            return None

    async def last(self, *args: Any, **kwargs: Any) -> "T":
        """
        Gets the last row from the db ordered by primary key column descending.

        Complementary to :meth:`first`: the default pk ordering is flipped
        and the top row is returned. Passing args and/or kwargs is a shortcut
        and equals to calling ``filter(*args, **kwargs).last()``.

        The query is always executed with ``LIMIT 1`` so at most one row is
        fetched; use :meth:`get` if you want to assert that the criteria
        match exactly one row.

        :raises NoMatch: if no rows match the criteria
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: returned model
        :rtype: Model
        """
        if kwargs or args:
            return await self.filter(*args, **kwargs).last()
        return await self._fetch_single(self._single_row_order_bys(reverse=True))

    async def last_or_none(self, *args: Any, **kwargs: Any) -> Optional["T"]:
        """
        Gets the last row from the db ordered by primary key column descending.

        Behaves like :meth:`last` but returns ``None`` instead of raising
        ``NoMatch`` when no row matches.

        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: returned model or None
        :rtype: Optional[Model]
        """
        try:
            return await self.last(*args, **kwargs)
        except ormar.NoMatch:
            return None

    async def get_or_none(self, *args: Any, **kwargs: Any) -> Optional["T"]:
        """
        Gets a single row from the db matching the criteria set by args/kwargs.

        Behaves like :meth:`get` but returns ``None`` instead of raising
        ``NoMatch`` when no row matches. ``MultipleMatches`` is still raised
        when criteria are provided and more than one row matches.

        :raises MultipleMatches: when criteria are set (via ``filter``/``exclude``
            or args/kwargs) and more than one row matches them.
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: returned model or None
        :rtype: Optional[Model]
        """
        try:
            return await self.get(*args, **kwargs)
        except ormar.NoMatch:
            return None

    async def get(self, *args: Any, **kwargs: Any) -> "T":  # noqa: CCR001
        """
        Gets a single row from the db matching the criteria set by args/kwargs.

        Passing args/kwargs is a shortcut for ``filter(*args, **kwargs).get()``.

        When criteria are set (either through args/kwargs here or via a
        chained :meth:`filter`/:meth:`exclude`) the query fetches every
        matching row and asserts that exactly one comes back — ``NoMatch``
        is raised when none match and ``MultipleMatches`` when more than one
        matches.

        When no criteria are set, ``get()`` falls back to returning the last
        row of the table ordered by primary key descending (``LIMIT 1``); in
        that mode ``MultipleMatches`` cannot be raised.

        :raises NoMatch: if no rows match the criteria
        :raises MultipleMatches: when criteria are set and more than one row
            matches them.
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: returned model
        :rtype: Model
        """
        if kwargs or args:
            return await self.filter(*args, **kwargs).get()

        if not (self.filter_clauses or self.exclude_clauses):
            template = self.build_select_template(
                limit=1,
                order_bys=(
                    [
                        OrderAction(
                            order_str=f"-{self.model.ormar_config.pkname}",
                            model_cls=self.model_cls,  # type: ignore
                        )
                    ]
                    if not any([x.is_source_model_order for x in self.order_bys])
                    else []
                )
                + self.order_bys,
            )
        else:
            template = self.build_select_template()

        async with self.model_config.database.get_query_executor() as executor:
            rows = await executor.fetch_all(template.statement, template.params)
        processed_rows = await self._process_query_result_rows(rows)
        if self._prefetch_related and processed_rows:
            processed_rows = await self._prefetch_related_models(processed_rows, rows)
        self.check_single_result_rows_count(processed_rows)
        return processed_rows[0]  # type: ignore

    async def get_or_create(
        self,
        _defaults: Optional[dict[str, Any]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> tuple["T", bool]:
        """
        Combination of create and get methods.

        Tries to get a row meeting the criteria for kwargs
        and if `NoMatch` exception is raised
        it creates a new one with given kwargs and _defaults.

        If a concurrent caller wins the race and creates a matching row
        between this call's ``get`` and ``create``, the ``create`` raises
        ``sqlalchemy.exc.IntegrityError`` and ``get`` is retried once.
        If the retry still finds no row, the original ``IntegrityError`` is
        re-raised because the violation isn't a race but a legitimate
        constraint conflict (e.g. a different unique key).

        Passing a criteria is actually calling filter(*args, **kwargs) method described
        below.

        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :param _defaults: default values for creating object
        :type _defaults: Optional[dict[str, Any]]
        :return: model instance and a boolean
        :rtype: tuple("T", bool)
        """
        try:
            return await self.get(*args, **kwargs), False
        except NoMatch:
            pass

        _defaults = _defaults or {}
        try:
            return await self.create(**{**kwargs, **_defaults}), True
        except sqlalchemy.exc.IntegrityError as integrity_error:
            try:
                return await self.get(*args, **kwargs), False
            except NoMatch:
                raise integrity_error from None

    async def update_or_create(self, **kwargs: Any) -> "T":
        """
        Updates the model, or in case there is no match in database creates a new one.

        If a pk is supplied but no row exists with that pk, a new row is
        created with the supplied pk and field values rather than raising
        ``NoMatch``.

        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: updated or created model
        :rtype: Model
        """
        pk_name = self.model_config.pkname
        if "pk" in kwargs:
            kwargs[pk_name] = kwargs.pop("pk")
        if pk_name not in kwargs or kwargs.get(pk_name) is None:
            return await self.create(**kwargs)
        try:
            model = await self.get(pk=kwargs[pk_name])
        except NoMatch:
            return await self.create(**kwargs)
        return await model.update(**kwargs)

    async def all(self, *args: Any, **kwargs: Any) -> list["T"]:  # noqa: A003
        """
        Returns all rows from a database for given model for set filter options.

        Passing args and/or kwargs is a shortcut and equals to calling
        `filter(*args, **kwargs).all()`.

        If there are no rows meeting the criteria an empty list is returned.

        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: list of returned models
        :rtype: list[Model]
        """
        if kwargs or args:
            return await self.filter(*args, **kwargs).all()

        template = self.build_select_template()
        async with self.model_config.database.get_query_executor() as executor:
            rows = await executor.fetch_all(template.statement, template.params)
        result_rows = await self._process_query_result_rows(rows)
        if self._prefetch_related and result_rows:
            result_rows = await self._prefetch_related_models(result_rows, rows)
        if self._reverse_result:
            result_rows.reverse()

        return result_rows

    async def iterate(  # noqa: A003
        self,
        *args: Any,
        **kwargs: Any,
    ) -> AsyncGenerator["T", None]:
        """
        Return async iterable generator for all rows from a database for given model.

        Passing args and/or kwargs is a shortcut and equals to calling
        `filter(*args, **kwargs).iterate()`.

        If there are no rows meeting the criteria an empty async generator is returned.

        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: asynchronous iterable generator of returned models
        :rtype: AsyncGenerator[Model]
        """

        if self._prefetch_related:
            raise QueryDefinitionError(
                "Prefetch related queries are not supported in iterators"
            )

        if kwargs or args:
            async for result in self.filter(*args, **kwargs).iterate():
                yield result
            return

        template = self.build_select_template()

        rows: list = []
        last_primary_key = None
        pk_alias = self.model.get_column_alias(self.model_config.pkname)
        # Single shared cache across all yielded chunks so 1-row chunks
        # (the common iterate case) still amortize the plan build.
        plan_cache: dict = {}

        # Server-side cursor (asyncpg/aiomysql) requires an open transaction,
        # which AUTOCOMMIT does not provide.
        async with self.model_config.database.get_query_executor(
            transactional=True
        ) as executor:
            async for row in executor.iterate(template.statement, template.params):
                current_primary_key = row[pk_alias]
                if last_primary_key == current_primary_key or last_primary_key is None:
                    last_primary_key = current_primary_key
                    rows.append(row)
                    continue

                yield (await self._process_query_result_rows(rows, plan_cache))[0]
                last_primary_key = current_primary_key
                rows = [row]

            if rows:
                yield (await self._process_query_result_rows(rows, plan_cache))[0]

    async def create(self, **kwargs: Any) -> "T":
        """
        Creates the model instance, saves it in a database and returns the updates model
        (with pk populated if not passed and autoincrement is set).

        The allowed kwargs are `Model` fields names and proper value types.

        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: created model
        :rtype: Model
        """
        instance = self.model(**kwargs)
        instance = await instance.save()
        return instance

    async def bulk_create(self, objects: list["T"]) -> None:
        """
        Performs a bulk create in one database session to speed up the process.

        Allows you to create multiple objects at once.

        A valid list of `Model` objects needs to be passed.

        Bulk operations do not send signals.

        :param objects: list of ormar models already initialized and ready to save.
        :type objects: list[Model]
        """

        if not objects:
            raise ModelListEmptyError("Bulk create objects are empty!")

        ready_objects = []
        for i, obj in enumerate(objects):
            ready_objects.append(obj.prepare_model_to_save(obj.model_dump()))
            if i % 100 == 99:  # pragma: no cover
                await asyncio.sleep(0)

        # rows with different key sets (explicit vs omitted pk or server_default)
        # need different inserts, so each key set runs its own insert template;
        # anything beyond a single one-row insert runs in one transaction
        rows_by_keys: dict[frozenset[str], list[dict]] = {}
        for row in ready_objects:
            rows_by_keys.setdefault(frozenset(row), []).append(row)
        several_statements = len(rows_by_keys) > 1 or len(ready_objects) > 1
        async with self.model_config.database.get_query_executor(
            transactional=several_statements
        ) as executor:
            for rows in rows_by_keys.values():
                if len(rows) == 1:
                    template = insert_template(self.model, rows[0])
                    await executor.execute(template.statement, template.params)
                    continue
                names = tuple(sorted(rows[0]))
                await executor.execute_many(
                    insert_statement(self.model, names),
                    [value_params(names, row) for row in rows],
                )

        for obj in objects:
            obj.set_save_status(True)

    async def bulk_update(  # noqa:  CCR001
        self, objects: list["T"], columns: Optional[list[str]] = None
    ) -> None:
        """
        Performs bulk update in one database session to speed up the process.

        Allows you to update multiple instance at once.

        All `Models` passed need to have primary key column populated.

        You can also select which fields to update by passing `columns` list
        as a list of string names.

        Bulk operations do not send signals.

        :param objects: list of ormar models
        :type objects: list[Model]
        :param columns: list of columns to update
        :type columns: list[str]
        """
        if not objects:
            raise ModelListEmptyError("Bulk update objects are empty!")

        ready_objects = []
        pk_name = self.model_config.pkname
        if not columns:
            columns = list(
                self.model.extract_db_own_fields().union(
                    self.model.extract_related_names()
                )
            )

        if pk_name not in columns:
            columns.append(pk_name)

        onupdate_fields = self.model._onupdate_fields
        for field_name in onupdate_fields:
            if field_name not in columns:
                columns.append(field_name)

        columns = [self.model.get_column_alias(k) for k in columns]

        for i, obj in enumerate(objects):
            explicit_fields = obj.__setattr_fields__
            new_kwargs = obj.model_dump()
            if new_kwargs.get(pk_name) is None:
                raise ModelPersistenceError(
                    "You cannot update unsaved objects. "
                    f"{self.model.__name__} has to have {pk_name} filled."
                )
            new_kwargs = obj.populate_onupdate_value(
                new_kwargs, explicit_fields=explicit_fields
            )
            obj.update_from_dict(
                {
                    field_name: new_kwargs[field_name]
                    for field_name in onupdate_fields - explicit_fields
                }
            )
            new_kwargs = obj.prepare_model_to_update(new_kwargs)
            ready_objects.append(
                {"new_" + k: v for k, v in new_kwargs.items() if k in columns}
            )
            if i % 100 == 99:  # pragma: no cover
                await asyncio.sleep(0)

        pk_column: sqlalchemy.Column = self.model_config.table.c[
            self.model.get_column_alias(pk_name)
        ]
        pk_column_name = self.model.get_column_alias(pk_name)
        table_columns = [c.name for c in self.model_config.table.c]
        updated = tuple(
            k for k in columns if k != pk_column_name and k in table_columns
        )
        expr = cached_statement(
            self.model,
            ("bulk_update", updated),
            build_bulk_update,
            self.table,
            pk_column,
            updated,
        )
        # Multi-row execute_many: run in an explicit transaction so all rows
        # share a single COMMIT instead of one per row under AUTOCOMMIT.
        async with self.model_config.database.get_query_executor(
            transactional=True
        ) as executor:
            await executor.execute_many(expr, ready_objects)

        for obj in objects:
            obj.set_save_status(True)
            obj.__setattr_fields__.clear()

        await cast(
            type["Model"], self.model_cls
        ).ormar_config.signals.post_bulk_update.send(
            sender=self.model_cls,  # type: ignore
            instances=objects,
        )

    def __getitem__(self, key: Union[int, slice]) -> "QuerySet[T]":
        """
        Returns a new ``QuerySet`` with LIMIT/OFFSET derived from a Python
        integer index or slice.

        Negative indices and negative slice bounds are translated into a
        reversed-order query plus an in-memory list reversal on ``.all()``,
        so that ``Model.objects[-N:]`` returns the last ``N`` rows in the
        original ordering. Slice shapes that would require a ``COUNT(*)``
        round-trip (a bare ``[:-N]``, or mixed positive/negative bounds) are
        rejected with a ``QueryDefinitionError``; use ``.count()`` combined
        with ``.offset()``/``.limit()`` instead.

        Each call replaces the previous pagination state rather than composing
        with it — subsequent slicing on an already-sliced queryset is not
        guaranteed to compose in a Python-list-like way.

        :param key: integer index or slice
        :type key: int | slice
        :raises QueryDefinitionError: when ``key`` is not an ``int``/``slice``
            or when the slice cannot be expressed without a count
        :return: new QuerySet with updated pagination
        :rtype: QuerySet
        """
        bounds = normalize_slice(key)
        order_bys = self.order_bys
        if self._reverse_result != bounds.reverse:
            if not order_bys:
                order_bys = OrderAction.from_model_defaults(self.model)
            order_bys = [ob.flipped() for ob in order_bys]
        return self.rebuild_self(
            limit_count=bounds.limit,
            offset=bounds.offset,
            order_bys=order_bys,
            reverse_result=bounds.reverse,
        )
