import abc
import copy
import logging
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional, Sequence, Union, cast

import ormar_rust_utils

import ormar  # noqa: I100, I202
from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.filter_action import KeyInFilterAction
from ormar.queryset.clause import FilterGroup, QueryClause
from ormar.queryset.queries.query import Query
from ormar.queryset.utils import translate_list_to_dict
from ormar.relations.aliases import alias_for

if TYPE_CHECKING:  # pragma: no cover
    from ormar import ForeignKeyField, Model
    from ormar.fields.link import RelationLink
    from ormar.models.excludable import ExcludableItems
    from ormar.queryset import FilterAction, OrderAction, QuerySet

logger = logging.getLogger(__name__)

UniqueList = ormar_rust_utils.UniqueList


class Node(abc.ABC):
    """
    Base Node use to build a query tree and divide job into already loaded models
    and the ones that still need to be fetched from database
    """

    def __init__(self, relation_field: "ForeignKeyField", parent: "Node") -> None:
        self.parent = parent
        self.children: list["Node"] = []
        if self.parent:
            self.parent.children.append(self)
        self.relation_field = relation_field
        self.table_prefix = ""
        self.rows: list = []
        self.models: list["Model"] = []

    @abstractmethod
    def extract_related_ids(self, link: "RelationLink") -> list:  # pragma: no cover
        pass

    @abstractmethod
    def reload_tree(self) -> None:  # pragma: no cover
        pass

    @abstractmethod
    async def load_data(self) -> None:  # pragma: no cover
        pass

    def get_filter_for_prefetch(self) -> list["FilterAction"]:
        """
        Populates where clause with condition to return only models within the
        set of extracted ids.

        If there are no ids for relation the empty list is returned.

        :return: list of filter clauses based on original models
        :rtype: list[sqlalchemy.sql.elements.TextClause]
        """
        link = self.relation_field.link
        ids = self.parent.extract_related_ids(link=link)
        if ids:
            return self._prepare_filter_clauses(link=link, ids=ids)
        return []

    def _prepare_filter_clauses(
        self, link: "RelationLink", ids: list
    ) -> list["FilterAction"]:
        """
        Gets the list of ids and construct a list of filter queries on
        the target columns of the relation link.

        :param link: columns connecting the parent models to the queried ones
        :type link: RelationLink
        :param ids: list of ids that should be used to fetch data
        :type ids: list
        :return: list of filter actions to use in query
        :rtype: list["FilterAction"]
        """
        clause_target = self.relation_field.get_filter_clause_target()
        target_columns = link.target_columns()
        if len(target_columns) > 1:
            return [
                KeyInFilterAction(
                    prefix="",
                    values=list(ids),
                    model_cls=clause_target,
                    anchor=link.target_fields()[0],
                    columns=target_columns,
                )
            ]
        (filter_column,) = target_columns
        qryclause = QueryClause(
            model_cls=clause_target,
            select_related=[],
            filter_clauses=[],
        )
        kwargs = {f"{filter_column}__in": ids}
        filter_clauses, _ = qryclause.prepare_filter(_own_only=False, **kwargs)
        return filter_clauses


class AlreadyLoadedNode(Node):
    """
    Node that was already loaded in select statement
    """

    def __init__(self, relation_field: "ForeignKeyField", parent: "Node") -> None:
        super().__init__(relation_field=relation_field, parent=parent)
        self._extract_own_models()

    def _extract_own_models(self) -> None:
        """
        Extract own models that were already fetched and attached to root node
        """
        for model in self.parent.models:
            child_models = getattr(model, self.relation_field.name)
            if isinstance(child_models, list):
                self.models.extend(child_models)
            elif child_models:
                self.models.append(child_models)

    async def load_data(self) -> None:
        """
        Triggers a data load in the child nodes
        """
        for child in self.children:
            await child.load_data()

    def reload_tree(self) -> None:
        """
        After data was loaded we reload whole tree from the bottom
        to include freshly loaded nodes
        """
        for child in self.children:
            child.reload_tree()

    def extract_related_ids(self, link: "RelationLink") -> list:
        """
        Extracts the values own models hold for a relation link.
        Those values are used to construct filter clauses and populate child models.

        :param link: link from own models to the prefetched ones
        :type link: RelationLink
        :return: list of extracted values of relation columns
        :rtype: list
        """
        list_of_ids = UniqueList()
        for model in self.models:
            key = link.key_of(model=model)
            if key is not None:
                list_of_ids.append(key)
        return list_of_ids


class RootNode(AlreadyLoadedNode):
    """
    Root model Node from which both main and prefetch query originated
    """

    def __init__(self, models: list["Model"]) -> None:
        self.models = models
        self.use_alias = False
        self.children = []

    def reload_tree(self) -> None:
        for child in self.children:
            child.reload_tree()


class LoadNode(Node):
    """
    Nodes that actually need to be fetched from database in the prefetch query
    """

    def __init__(
        self,
        relation_field: "ForeignKeyField",
        excludable: "ExcludableItems",
        orders_by: list["OrderAction"],
        parent: "Node",
        source_model: type["Model"],
        queryset: Optional["QuerySet"] = None,
    ) -> None:
        super().__init__(relation_field=relation_field, parent=parent)
        self.queryset = queryset
        if queryset is not None:
            validate_prefetch_queryset(relation_field=relation_field, queryset=queryset)
        self.excludable = excludable
        self.query_excludable = excludable
        self.exclude_prefix: str = ""
        self.orders_by = orders_by
        self.grouped_models: dict[Any, list["Model"]] = dict()
        self.source_model = source_model

    async def load_data(self) -> None:
        """
        Ensures that at least primary key columns from current model are included in
        the query.

        Gets the filter values from the parent model and runs the query.

        Triggers a data load in child tasks.
        """
        self._update_excludable_with_related_pks()
        if self.relation_field.is_multi:
            query_target = self.relation_field.through
            select_related = [
                self.relation_field.through_target_field_name()  # type: ignore
            ]
            self.query_excludable = self.excludable.with_entry_alias(
                model_cls=self.relation_field.to,
                source_alias=self.exclude_prefix,
                target_alias=self.table_prefix,
            )
        else:
            query_target = self.relation_field.to
            select_related = []

        filter_clauses = self.get_filter_for_prefetch()

        if filter_clauses:
            parts = self._queryset_parts()
            qry = Query(
                model_cls=query_target,
                select_related=select_related + parts.select_related,
                filter_clauses=filter_clauses + parts.filter_clauses,
                exclude_clauses=parts.exclude_clauses,
                offset=None,
                limit_count=None,
                excludable=self.query_excludable,
                order_bys=self._extract_own_order_bys() + parts.order_bys,
                limit_raw_sql=False,
            )
            expr = qry.build_select_expression()
            logger.debug(
                expr.compile(
                    dialect=self.source_model.ormar_config.database.dialect,
                    compile_kwargs={"literal_binds": True},
                )
            )

            async with query_target.ormar_config.database.get_query_executor() as exctr:
                self.rows = await exctr.fetch_all(expr)

            for child in self.children:
                await child.load_data()

    def _update_excludable_with_related_pks(self) -> None:
        """
        Makes sure that excludable is populated with own model primary keys values
        if the excludable has the exclude/include clauses
        """
        related_field_names = self.relation_field.link.target_fields()
        self.exclude_prefix = alias_for(self._build_relation_key())
        self._merge_queryset_excludable()
        if self.relation_field.is_multi:
            self.table_prefix = alias_for(
                self.relation_field.through_target_field_name()  # type: ignore
            )
        target_model = self.relation_field.to
        model_excludable = self.excludable.get(
            model_cls=target_model, alias=self.exclude_prefix
        )
        # includes nested pks if not included already
        for related_name in related_field_names:
            if model_excludable.include and not model_excludable.is_included(
                related_name
            ):
                model_excludable.set_values({related_name}, slot="include")

    def _merge_queryset_excludable(self) -> None:
        """
        Apply ``fields`` / ``exclude_fields`` of a ``Prefetch`` queryset to the
        prefetched models.
        """
        if self.queryset is None:
            return
        target_model = self.relation_field.to
        own = self.queryset._excludable.get(model_cls=target_model, alias="")
        merged = self.excludable.get(model_cls=target_model, alias=self.exclude_prefix)
        if own.include:
            merged.set_values(value=own.include, slot="include")
        if own.exclude:
            merged.set_values(value=own.exclude, slot="exclude")

    def _queryset_parts(self) -> "PrefetchQuerysetParts":
        """
        Filters, excludes, ordering and joins of a ``Prefetch`` queryset, adapted
        to the prefetch query: a many-to-many relation is queried through its
        through model, so the clauses are re-aliased to the joined target table.

        :raises QueryDefinitionError: if a many-to-many prefetch queryset filters
            or orders through other relations of the target
        :return: parts to add to the prefetch query
        :rtype: PrefetchQuerysetParts
        """
        queryset = self.queryset
        if queryset is None:
            return PrefetchQuerysetParts(
                filter_clauses=[], exclude_clauses=[], order_bys=[], select_related=[]
            )
        filter_clauses = copy.deepcopy(queryset.filter_clauses)
        exclude_clauses = copy.deepcopy(queryset.exclude_clauses)
        order_bys = [copy.copy(order) for order in queryset.order_bys]
        for order in order_bys:
            order.is_source_model_order = not order.related_str
        if not self.relation_field.is_multi:
            return PrefetchQuerysetParts(
                filter_clauses=filter_clauses,
                exclude_clauses=exclude_clauses,
                order_bys=order_bys,
                select_related=[
                    *queryset._select_related,
                    *(order.related_str for order in order_bys if order.related_str),
                ],
            )
        actions = [
            *iter_filter_actions(clauses=filter_clauses),
            *iter_filter_actions(clauses=exclude_clauses),
        ]
        if any(action.related_parts for action in actions) or any(
            order.related_str for order in order_bys
        ):
            raise QueryDefinitionError(
                "A Prefetch queryset of a many to many relation can only filter and "
                "order by fields of the related model itself."
            )
        for action in actions:
            action.table_prefix = self.table_prefix
        for order in order_bys:
            order.table_prefix = self.table_prefix
        return PrefetchQuerysetParts(
            filter_clauses=filter_clauses,
            exclude_clauses=exclude_clauses,
            order_bys=order_bys,
            select_related=[],
        )

    def _build_relation_string(self) -> str:
        node: Union[LoadNode, Node] = self
        relation = node.relation_field.name
        while not isinstance(node.parent, RootNode):
            relation = node.parent.relation_field.name + "__" + relation
            node = node.parent
        return relation

    def _build_relation_key(self) -> str:
        relation_key = self._build_relation_string()
        return relation_key

    def _extract_own_order_bys(self) -> list["OrderAction"]:
        """
        Extracts list of order actions related to current model.
        Since same model can happen multiple times in a tree we check not only the
        match on given model but also that path from relation tree matches the
        path in order action.

        :return: list of order actions related to current model
        :rtype: list[OrderAction]
        """
        own_order_bys = []
        own_path = self._get_full_tree_path()
        for order_by in self.orders_by:
            if (
                order_by.target_model == self.relation_field.to
                and order_by.related_str.endswith(f"{own_path}")
            ):
                order_by = copy.copy(order_by)
                order_by.is_source_model_order = True
                order_by.table_prefix = self.table_prefix
                own_order_bys.append(order_by)
        return own_order_bys

    def _get_full_tree_path(self) -> str:
        """
        Iterates the nodes to extract path from root node.

        :return: path from root node
        :rtype: str
        """
        node: Node = self
        relation_str = node.relation_field.name
        while not isinstance(node.parent, RootNode):
            node = node.parent
            relation_str = f"{node.relation_field.name}__{relation_str}"
        return relation_str

    def extract_related_ids(self, link: "RelationLink") -> list:
        """
        Extracts the values own rows hold for a relation link, read from the
        own table's (prefixed) columns.
        Those values are used to construct filter clauses and populate child models.

        :param link: link from own models to the prefetched ones
        :type link: RelationLink
        :return: list of extracted values of relation columns
        :rtype: list
        """
        read_key = link.key_getter(side="source", prefix=self.table_prefix)
        list_of_ids = UniqueList()
        for row in self.rows:
            key = read_key(row)
            if key is not None:
                list_of_ids.append(key)
        return list_of_ids

    def reload_tree(self) -> None:
        """
        Instantiates models from loaded database rows.
        Groups those instances by relation key for easy extract per parent.
        Triggers same for child nodes and then populates
        the parent node with own related models
        """
        if self.rows:
            self._instantiate_models()
            self._group_models_by_relation_key()
            for child in self.children:
                child.reload_tree()
            self._populate_parent_models()

    def _instantiate_models(self) -> None:
        """
        Iterates the rows and initializes instances of ormar.Models.
        Each model is instantiated only once (they can be duplicates for m2m relation
        when multiple parent models refer to same child model since the query have to
        also include the through model - hence full rows are unique, but related
        models without through models can be not unique).
        """
        fields_to_exclude = self.relation_field.to.get_names_to_exclude(
            excludable=self.excludable, alias=self.exclude_prefix
        )
        # ``self.table_prefix`` and ``self.exclude_prefix`` can differ, so the
        # plan is built from the sub-query excludable and only amortizes the
        # column mapping work — exclude set is reused from above.
        row_plan = self.relation_field.to.build_row_extraction_plan(
            self.table_prefix, self.query_excludable
        )
        parsed_rows: dict[tuple, "Model"] = {}
        for row in self.rows:
            item = self.relation_field.to.apply_row_plan(row_plan, row, {})
            hashable_item = self._hash_item(item)
            instance = parsed_rows.get(hashable_item)
            if instance is None:
                instance = self.relation_field.to._construct_with_excluded(
                    fields_to_exclude, **item
                )
                instance.set_persisted(persisted=instance.pk is not None)
                parsed_rows[hashable_item] = instance
            self.models.append(instance)

    def _hash_item(self, item: Union[dict, list]) -> tuple:
        """
        Converts model dictionary or list into a hashable tuple to allow its use
        as a dictionary key - used to ensure unique instances of related models.

        :param item: instance dictionary or list
        :type item: dict or list
        :return: tuple out of model dictionary or list
        :rtype: tuple
        """
        return ormar_rust_utils.hash_item(item)

    def _group_models_by_relation_key(self) -> None:
        """
        Groups own models by relation keys so it's easy later to extract those models
        when iterating parent models. Note that order is important as it reflects
        order by issued by the user.
        """
        read_key = self.relation_field.link.key_getter(side="target")
        for index, row in enumerate(self.rows):
            current_group = self.grouped_models.setdefault(read_key(row), [])
            current_group.append(self.models[index])

    def _populate_parent_models(self) -> None:
        """
        Populate parent node models with own child models from grouped dictionary
        """
        link = self.relation_field.link
        for model in self.parent.models:
            for child in self.grouped_models.get(link.key_of(model=model), []):
                setattr(model, self.relation_field.name, child)


@dataclass
class PrefetchQuerysetParts:
    """
    Parts of a ``Prefetch`` queryset added to a prefetch query.

    :ivar filter_clauses: filter actions and groups
    :ivar exclude_clauses: exclude actions and groups
    :ivar order_bys: order actions
    :ivar select_related: relations joined for filters and ordering
    """

    filter_clauses: list[Any]
    exclude_clauses: list[Any]
    order_bys: list["OrderAction"]
    select_related: list[str]


def iter_filter_actions(clauses: list[Any]) -> list["FilterAction"]:
    """
    :param clauses: filter actions and filter groups
    :type clauses: list[Any]
    :return: every filter action, including those nested in groups
    :rtype: list[FilterAction]
    """
    actions: list["FilterAction"] = []
    for clause in clauses:
        if isinstance(clause, FilterGroup):
            actions.extend(clause._iter())
        else:
            actions.append(clause)
    return actions


def validate_prefetch_queryset(
    relation_field: "ForeignKeyField", queryset: "QuerySet"
) -> None:
    """
    Check that a ``Prefetch`` queryset can be applied to its relation.

    :raises QueryDefinitionError: if the queryset is of another model or uses
        limit, offset or prefetch_related
    :param relation_field: prefetched relation
    :type relation_field: ForeignKeyField
    :param queryset: queryset passed with ``Prefetch``
    :type queryset: QuerySet
    """
    if queryset.model is not relation_field.to:
        raise QueryDefinitionError(
            f"Prefetch of {relation_field.name} needs a queryset of "
            f"{relation_field.to.__name__}, got {queryset.model.__name__}."
        )
    if (
        queryset.limit_count is not None
        or queryset.query_offset is not None
        or queryset._prefetch_related
    ):
        raise QueryDefinitionError(
            "A Prefetch queryset cannot use limit, offset, paginate or "
            "prefetch_related; prefetch nested relations with their own paths."
        )


class PrefetchQuery:
    """
    Query used to fetch related models in subsequent queries.
    Each model is fetched only ones by the name of the relation.
    That means that for each prefetch_related entry next query is issued to database.
    """

    def __init__(  # noqa: CFQ002
        self,
        model_cls: type["Model"],
        excludable: "ExcludableItems",
        prefetch_related: list,
        select_related: list,
        orders_by: list["OrderAction"],
        prefetch_querysets: Optional[dict[str, "QuerySet"]] = None,
    ) -> None:
        self.model = model_cls
        self.prefetch_querysets = prefetch_querysets or {}
        # nodes add their own includes/excludes, keep the queryset's untouched
        self.excludable = ormar.ExcludableItems.from_excludable(excludable)
        self.select_dict = translate_list_to_dict(select_related, default={})
        self.prefetch_dict = translate_list_to_dict(prefetch_related, default={})
        self.orders_by = orders_by
        self.load_tasks: list[Node] = []

    async def prefetch_related(self, models: Sequence["Model"]) -> Sequence["Model"]:
        """
        Main entry point for prefetch_query.

        Receives list of already initialized parent models with all children from
        select_related already populated. Receives also list of row sql result rows
        as it's quicker to extract ids that way instead of calling each model.

        Returns list with related models already prefetched and set.

        :param models: list of already instantiated models from main query
        :type models: Sequence[Model]
        :param rows: row sql result of the main query before the prefetch
        :type rows: list[sqlalchemy.engine.result.RowProxy]
        :return: list of models with children prefetched
        :rtype: list[Model]
        """
        parent_task = RootNode(models=cast(list["Model"], models))
        self._build_load_tree(
            prefetch_dict=self.prefetch_dict,
            select_dict=self.select_dict,
            parent=parent_task,
            model=self.model,
            path="",
        )
        await parent_task.load_data()
        parent_task.reload_tree()
        return parent_task.models

    def _build_load_tree(
        self,
        select_dict: dict,
        prefetch_dict: dict,
        parent: Node,
        model: type["Model"],
        path: str,
    ) -> None:
        """
        Build a tree of already loaded nodes and nodes that need
        to be loaded through the prefetch query.

        :param select_dict: dictionary wth select query structure
        :type select_dict: dict
        :param prefetch_dict: dictionary with prefetch query structure
        :type prefetch_dict: dict
        :param parent: parent Node
        :type parent: Node
        :param model: currently processed model
        :type model: Model
        """
        for related in prefetch_dict.keys():
            relation_path = f"{path}__{related}" if path else related
            relation_field = cast(
                "ForeignKeyField", model.ormar_config.model_fields[related]
            )
            if related in select_dict:
                if relation_path in self.prefetch_querysets:
                    raise QueryDefinitionError(
                        f"Prefetch of {relation_path} cannot be combined with "
                        "select_related of the same relation."
                    )
                task: Node = AlreadyLoadedNode(
                    relation_field=relation_field, parent=parent
                )
            else:
                task = LoadNode(
                    relation_field=relation_field,
                    excludable=self.excludable,
                    orders_by=self.orders_by,
                    parent=parent,
                    source_model=self.model,
                    queryset=self.prefetch_querysets.get(relation_path),
                )
            if prefetch_dict:
                self._build_load_tree(
                    select_dict=select_dict.get(related, {}),
                    prefetch_dict=prefetch_dict.get(related, {}),
                    parent=task,
                    model=model.ormar_config.model_fields[related].to,
                    path=relation_path,
                )
