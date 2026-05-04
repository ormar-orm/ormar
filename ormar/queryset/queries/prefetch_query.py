import abc
import logging
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Sequence, Union, cast

import ormar  # noqa:  I100, I202
from ormar.queryset.clause import QueryClause
from ormar.queryset.queries.query import Query
from ormar.queryset.utils import translate_list_to_dict

if TYPE_CHECKING:  # pragma: no cover
    from ormar import ForeignKeyField, Model
    from ormar.models.excludable import ExcludableItems
    from ormar.queryset import FilterAction, OrderAction

logger = logging.getLogger(__name__)


class UniqueList(list):
    """
    Simple subclass of list that prevents the duplicates
    Cannot use set as the order is important
    """

    def append(self, item: Any) -> None:
        if item not in self:
            super().append(item)


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
        self.use_alias: bool = False

    @property
    def target_name(self) -> str:
        """
        Return the name of the relation that is used to
        fetch excludes/includes from the excludable mixin
        as well as specifying the target to join in m2m relations

        :return: name of the relation
        :rtype: str
        """
        if (
            self.relation_field.self_reference
            and self.relation_field.self_reference_primary == self.relation_field.name
        ):
            return self.relation_field.default_source_field_name()
        else:
            return self.relation_field.default_target_field_name()

    @abstractmethod
    def extract_related_ids(self, column_name: str) -> list:  # pragma: no cover
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
        column_name = self.relation_field.get_model_relation_fields(
            self.parent.use_alias
        )

        ids = self.parent.extract_related_ids(column_name=column_name)

        if ids:
            return self._prepare_filter_clauses(ids=ids)
        return []

    def _prepare_filter_clauses(self, ids: list) -> list["FilterAction"]:
        """
        Gets the list of ids and construct a list of filter queries on
        extracted appropriate column names

        :param ids: list of ids that should be used to fetch data
        :type ids: list
        :return: list of filter actions to use in query
        :rtype: list["FilterAction"]
        """
        clause_target = self.relation_field.get_filter_clause_target()
        filter_column = self.relation_field.get_related_field_alias()
        qryclause = QueryClause(
            model_cls=clause_target,
            select_related=[],
            filter_clauses=[],
        )
        kwargs = {f"{cast(str, filter_column)}__in": ids}
        filter_clauses, _ = qryclause.prepare_filter(_own_only=False, **kwargs)
        return filter_clauses


class AlreadyLoadedNode(Node):
    """
    Node that was already loaded in select statement
    """

    def __init__(self, relation_field: "ForeignKeyField", parent: "Node") -> None:
        super().__init__(relation_field=relation_field, parent=parent)
        self.use_alias = False
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

    def extract_related_ids(self, column_name: str) -> list:
        """
        Extracts the selected column(s) values from own models.
        Those values are used to construct filter clauses and populate child models.

        :param column_name: names of the column(s) that holds the relation info
        :type column_name: Union[str, list[str]]
        :return: list of extracted values of relation columns
        :rtype: list
        """
        list_of_ids = UniqueList()
        for model in self.models:
            child = getattr(model, column_name)
            if isinstance(child, ormar.Model):
                list_of_ids.append(child.pk)
            elif child is not None:
                list_of_ids.append(child)
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
    ) -> None:
        super().__init__(relation_field=relation_field, parent=parent)
        self.excludable = excludable
        self.exclude_prefix: str = ""
        self.orders_by = orders_by
        self.use_alias = True
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
            select_related = [self.target_name]
        else:
            query_target = self.relation_field.to
            select_related = []

        filter_clauses = self.get_filter_for_prefetch()

        if filter_clauses:
            qry = Query(
                model_cls=query_target,
                select_related=select_related,
                filter_clauses=filter_clauses,
                exclude_clauses=[],
                offset=None,
                limit_count=None,
                excludable=self.excludable,
                order_bys=self._extract_own_order_bys(),
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
        related_field_names = self.relation_field.get_related_field_name()
        alias_manager = self.relation_field.to.ormar_config.alias_manager
        relation_key = self._build_relation_key()
        self.exclude_prefix = alias_manager.resolve_relation_alias_after_complex(
            source_model=self.source_model,
            relation_str=relation_key,
            relation_field=self.relation_field,
        )
        if self.relation_field.is_multi:
            self.table_prefix = self.exclude_prefix
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

    def extract_related_ids(self, column_name: str) -> list:
        """
        Extracts the selected column(s) values from own models.
        Those values are used to construct filter clauses and populate child models.

        :param column_names: names of the column(s) that holds the relation info
        :type column_names: Union[str, list[str]]
        :return: list of extracted values of relation columns
        :rtype: list
        """
        column_name = self._prefix_column_names_with_table_prefix(
            column_name=column_name
        )
        return self._extract_simple_relation_keys(column_name=column_name)

    def _prefix_column_names_with_table_prefix(self, column_name: str) -> str:
        return (f"{self.table_prefix}_" if self.table_prefix else "") + column_name

    def _extract_simple_relation_keys(self, column_name: str) -> list:
        """
        Extracts simple relation keys values.

        :param column_name: names of the column(s) that holds the relation info
        :type column_name: str
        :return: list of extracted values of relation columns
        :rtype: list
        """
        list_of_ids = UniqueList()
        for row in self.rows:
            if row[column_name]:
                list_of_ids.append(row[column_name])
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
        parsed_rows: dict[tuple, "Model"] = {}
        for row in self.rows:
            item = self.relation_field.to.extract_prefixed_table_columns(
                item={},
                row=row,
                table_prefix=self.table_prefix,
                excludable=self.excludable,
            )
            hashable_item = self._hash_item(item)
            instance = parsed_rows.setdefault(
                hashable_item,
                self.relation_field.to._construct_with_excluded(
                    fields_to_exclude, **item
                ),
            )
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
        result = []
        for key, value in (
            sorted(item.items()) if isinstance(item, dict) else enumerate(item)
        ):
            if isinstance(value, (dict, list)):
                value = self._hash_item(value)
            result.append((key, value))

        return tuple(result)

    def _group_models_by_relation_key(self) -> None:
        """
        Groups own models by relation keys so it's easy later to extract those models
        when iterating parent models. Note that order is important as it reflects
        order by issued by the user.
        """
        relation_key = self.relation_field.get_related_field_alias()
        for index, row in enumerate(self.rows):
            key = row[relation_key]
            current_group = self.grouped_models.setdefault(key, [])
            current_group.append(self.models[index])

    def _populate_parent_models(self) -> None:
        """
        Populate parent node models with own child models from grouped dictionary
        """
        relation_key = self._get_relation_key_linking_models()
        for model in self.parent.models:
            children = self._get_own_models_related_to_parent(
                model=model, relation_key=relation_key
            )
            for child in children:
                setattr(model, self.relation_field.name, child)

    def _get_relation_key_linking_models(self) -> tuple[str, str]:
        """
        Extract names and aliases of relation columns to use
        in linking between own models and parent models

        :return: tuple of name and alias of relation columns
        :rtype: list[tuple[str, str]]
        """
        column_name = self.relation_field.get_model_relation_fields(False)
        column_alias = self.relation_field.get_model_relation_fields(True)
        return column_name, column_alias

    def _get_own_models_related_to_parent(
        self, model: "Model", relation_key: tuple[str, str]
    ) -> list["Model"]:
        """
        Extracts related column values from parent and based on this key gets the
        own grouped models.

        :param model: parent model from parent node
        :type model: Model
        :param relation_key: name and aliases linking relations
        :type relation_key: list[tuple[str, str]]
        :return: list of own models to set on parent
        :rtype: list[Model]
        """
        column_name, column_alias = relation_key
        model_value = getattr(model, column_name)
        if isinstance(model_value, ormar.Model):
            model_value = model_value.pk
        return self.grouped_models.get(model_value, [])


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
    ) -> None:
        self.model = model_cls
        self.excludable = excludable
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
            relation_field = cast(
                "ForeignKeyField", model.ormar_config.model_fields[related]
            )
            if related in select_dict:
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
                )
            if prefetch_dict:
                self._build_load_tree(
                    select_dict=select_dict.get(related, {}),
                    prefetch_dict=prefetch_dict.get(related, {}),
                    parent=task,
                    model=model.ormar_config.model_fields[related].to,
                )
