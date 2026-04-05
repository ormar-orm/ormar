import asyncio
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Generic,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

import sqlalchemy
from sqlalchemy import bindparam

import ormar  # noqa I100
from ormar import MultipleMatches, NoMatch
from ormar.exceptions import (
    ModelListEmptyError,
    ModelPersistenceError,
    QueryDefinitionError,
)
from ormar.queryset import FieldAccessor, FilterQuery, SelectAction
from ormar.queryset.actions.order_action import OrderAction
from ormar.queryset.clause import FilterGroup, QueryClause
from ormar.queryset.queries.prefetch_query import PrefetchQuery
from ormar.queryset.queries.query import Query
from ormar.queryset.reverse_alias_resolver import ReverseAliasResolver
from ormar.queryset.utils import get_limit_offset

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model
    from ormar.models import T
    from ormar.models.excludable import ExcludableItems
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

    async def _process_query_result_rows(self, rows: list) -> list["T"]:
        """
        Process database rows and initialize ormar Model from each of the rows.

        :param rows: list of database rows from query result
        :type rows: list[sqlalchemy.engine.result.RowProxy]
        :return: list of models
        :rtype: list[Model]
        """
        result_rows = []
        for i, row in enumerate(rows):
            result_rows.append(
                self.model.from_row(
                    row=row,
                    select_related=self._select_related,
                    excludable=self._excludable,
                    source_model=self.model,
                    proxy_source_model=self.proxy_source_model,
                )
            )
            if i % 100 == 99:  # pragma: no cover
                await asyncio.sleep(0)

        if result_rows:
            return self.model.merge_instances_list(result_rows)  # type: ignore
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
        )
        exp = qry.build_select_expression()
        # print("\n", exp.compile(compile_kwargs={"literal_binds": True}))
        return exp

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
        filter_clauses, select_related = qryclause.prepare_filter(**kwargs)
        filter_clauses = filter_clauses + filter_groups  # type: ignore
        if _exclude:
            exclude_clauses = filter_clauses
            filter_clauses = self.filter_clauses
        else:
            exclude_clauses = self.exclude_clauses
            filter_clauses = filter_clauses

        return self.rebuild_self(
            filter_clauses=filter_clauses,
            exclude_clauses=exclude_clauses,
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
        related = [
            rel._access_chain if isinstance(rel, FieldAccessor) else rel
            for rel in related
        ]

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
        related = [
            rel._access_chain if isinstance(rel, FieldAccessor) else rel
            for rel in related
        ]

        related = list(set(list(self._prefetch_related) + related))
        return self.rebuild_self(prefetch_related=related)

    def fields(
        self, columns: Union[list, str, set, dict], _is_exclude: bool = False
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

        :param _is_exclude: flag if it's exclude or include operation
        :type _is_exclude: bool
        :param columns: columns to include
        :type columns: Union[list, str, set, dict]
        :return: QuerySet
        :rtype: QuerySet
        """
        excludable = ormar.ExcludableItems.from_excludable(self._excludable)
        excludable.build(
            items=columns,
            model_cls=self.model_cls,  # type: ignore
            is_exclude=_is_exclude,
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
        return self.fields(columns=columns, _is_exclude=True)

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
        expr = self.build_select_expression()
        async with self.model_config.database.get_query_executor() as executor:
            rows = await executor.fetch_all(expr)
        if not rows:
            return []
        alias_resolver = ReverseAliasResolver(
            select_related=self._select_related,
            excludable=self._excludable,
            model_cls=self.model_cls,  # type: ignore
            exclude_through=exclude_through,
        )
        column_map = alias_resolver.resolve_columns(columns_names=list(rows[0].keys()))  # type: ignore
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
        expr = self.build_select_expression()
        expr = sqlalchemy.exists(expr).select()
        async with self.model_config.database.get_query_executor() as executor:
            result = await executor.fetch_val(expr)
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
        expr = self.build_select_expression().alias("subquery_for_count")
        expr = sqlalchemy.func.count().select().select_from(expr)  # type: ignore
        if distinct:
            pk_column_name = self.model.get_column_alias(self.model_config.pkname)
            expr_distinct = expr.group_by(pk_column_name).alias("subquery_for_group")  # type: ignore
            expr = sqlalchemy.func.count().select().select_from(expr_distinct)  # type: ignore
        async with self.model_config.database.get_query_executor() as executor:
            result = await executor.fetch_val(expr)  # type: ignore
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
        expr = self.build_select_expression().alias(f"subquery_for_{func_name}")
        expr = sqlalchemy.select(*select_columns).select_from(expr)  # type: ignore
        # print("\n", expr.compile(compile_kwargs={"literal_binds": True}))
        async with self.model_config.database.get_query_executor() as executor:
            result = await executor.fetch_one(expr)  # type: ignore
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

        self_fields = self.model.extract_db_own_fields().union(
            self.model.extract_related_names()
        )
        updates = {k: v for k, v in kwargs.items() if k in self_fields}
        updates = self.model.validate_enums(updates)
        updates = self.model.translate_columns_to_aliases(updates)

        expr = FilterQuery(filter_clauses=self.filter_clauses).apply(
            self.table.update().values(**updates)  # type: ignore
        )
        expr = FilterQuery(filter_clauses=self.exclude_clauses, exclude=True).apply(
            expr
        )
        async with self.model_config.database.get_query_executor() as executor:
            return await executor.execute(expr)

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
        expr = FilterQuery(filter_clauses=self.filter_clauses).apply(
            self.table.delete()  # type: ignore
        )
        expr = FilterQuery(filter_clauses=self.exclude_clauses, exclude=True).apply(
            expr
        )
        async with self.model_config.database.get_query_executor() as executor:
            return await executor.execute(expr)

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

    async def first(self, *args: Any, **kwargs: Any) -> "T":
        """
        Gets the first row from the db ordered by primary key column ascending.

        :raises NoMatch: if no rows are returned
        :raises MultipleMatches: if more than 1 row is returned.
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: returned model
        :rtype: Model
        """
        if kwargs or args:
            return await self.filter(*args, **kwargs).first()

        expr = self.build_select_expression(
            limit=1,
            order_bys=(
                [
                    OrderAction(
                        order_str=f"{self.model.ormar_config.pkname}",
                        model_cls=self.model_cls,  # type: ignore
                    )
                ]
                if not any([x.is_source_model_order for x in self.order_bys])
                else []
            )
            + self.order_bys,
        )
        async with self.model_config.database.get_query_executor() as executor:
            rows = await executor.fetch_all(expr)
        processed_rows = await self._process_query_result_rows(rows)
        if self._prefetch_related and processed_rows:
            processed_rows = await self._prefetch_related_models(processed_rows, rows)
        self.check_single_result_rows_count(processed_rows)
        return processed_rows[0]  # type: ignore

    async def first_or_none(self, *args: Any, **kwargs: Any) -> Optional["T"]:
        """
        Gets the first row from the db ordered by primary key column ascending.

        If no match is found None will be returned.

        :raises MultipleMatches: if more than 1 row is returned.
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: returned model
        :rtype: Model
        """
        try:
            return await self.first(*args, **kwargs)
        except ormar.NoMatch:
            return None

    async def get_or_none(self, *args: Any, **kwargs: Any) -> Optional["T"]:
        """
        Gets the first row from the db meeting the criteria set by kwargs.

        If no criteria set it will return the last row in db sorted by pk.

        Passing a criteria is actually calling filter(*args, **kwargs) method described
        below.

        If no match is found None will be returned.

        :raises MultipleMatches: if more than 1 row is returned.
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: returned model
        :rtype: Model
        """
        try:
            return await self.get(*args, **kwargs)
        except ormar.NoMatch:
            return None

    async def get(self, *args: Any, **kwargs: Any) -> "T":  # noqa: CCR001
        """
        Gets the first row from the db meeting the criteria set by kwargs.

        If no criteria set it will return the last row in db sorted by pk.

        Passing a criteria is actually calling filter(*args, **kwargs) method described
        below.

        :raises NoMatch: if no rows are returned
        :raises MultipleMatches: if more than 1 row is returned.
        :param kwargs: fields names and proper value types
        :type kwargs: Any
        :return: returned model
        :rtype: Model
        """
        if kwargs or args:
            return await self.filter(*args, **kwargs).get()

        if not self.filter_clauses:
            expr = self.build_select_expression(
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
            expr = self.build_select_expression()

        async with self.model_config.database.get_query_executor() as executor:
            rows = await executor.fetch_all(expr)
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
            _defaults = _defaults or {}
            return await self.create(**{**kwargs, **_defaults}), True

    async def update_or_create(self, **kwargs: Any) -> "T":
        """
        Updates the model, or in case there is no match in database creates a new one.

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
        model = await self.get(pk=kwargs[pk_name])
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

        expr = self.build_select_expression()
        async with self.model_config.database.get_query_executor() as executor:
            rows = await executor.fetch_all(expr)
        result_rows = await self._process_query_result_rows(rows)
        if self._prefetch_related and result_rows:
            result_rows = await self._prefetch_related_models(result_rows, rows)

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

        expr = self.build_select_expression()

        rows: list = []
        last_primary_key = None
        pk_alias = self.model.get_column_alias(self.model_config.pkname)

        async with self.model_config.database.get_query_executor() as executor:
            async for row in executor.iterate(expr):
                current_primary_key = row[pk_alias]
                if last_primary_key == current_primary_key or last_primary_key is None:
                    last_primary_key = current_primary_key
                    rows.append(row)
                    continue

                yield (await self._process_query_result_rows(rows))[0]
                last_primary_key = current_primary_key
                rows = [row]

            if rows:
                yield (await self._process_query_result_rows(rows))[0]

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

        # don't use execute_many, as in databases it's executed in a loop
        # instead of using execute_many from drivers
        expr = self.table.insert().values(ready_objects)
        async with self.model_config.database.get_query_executor() as executor:
            await executor.execute(expr)

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

        columns = [self.model.get_column_alias(k) for k in columns]

        for i, obj in enumerate(objects):
            new_kwargs = obj.model_dump()
            if new_kwargs.get(pk_name) is None:
                raise ModelPersistenceError(
                    "You cannot update unsaved objects. "
                    f"{self.model.__name__} has to have {pk_name} filled."
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
        expr = self.table.update().where(
            pk_column == bindparam("new_" + pk_column_name)
        )
        expr = expr.values(
            **{
                k: bindparam("new_" + k)
                for k in columns
                if k != pk_column_name and k in table_columns
            }
        )
        # databases bind params only where query is passed as string
        # otherwise it just passes all data to values and results in unconsumed columns
        expr = str(expr)  # type: ignore
        async with self.model_config.database.get_query_executor() as executor:
            await executor.execute_many(expr, ready_objects)

        for obj in objects:
            obj.set_save_status(True)

        await cast(
            type["Model"], self.model_cls
        ).ormar_config.signals.post_bulk_update.send(
            sender=self.model_cls,  # type: ignore
            instances=objects,
        )

    def __getitem__(self, key: Union[int, slice]) -> "QuerySet[T]":
        """Retrieve an item or slice from the set of results."""

        if not isinstance(key, (int, slice)):
            raise TypeError(f"{key} is neither an integer nor a range.")

        limit, offset = get_limit_offset(key=key)
        return self.rebuild_self(offset=offset, limit_count=limit)
