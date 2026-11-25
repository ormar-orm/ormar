import copy
import decimal
import itertools
from typing import TYPE_CHECKING, Any, Callable, Iterator

import sqlalchemy

from ormar.exceptions import QueryDefinitionError  # noqa: I202
from ormar.queryset.actions.query_action import QueryAction
from ormar.queryset.expressions import (
    Case,
    Combined,
    F,
    Function,
    Inserted,
    When,
    expression_conditions,
    expression_leaves,
)

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model


class SelectAction(QueryAction):
    """
    Represents a single column target for an aggregate function on a queryset.

    Created when ``.min()`` / ``.max()`` / ``.sum()`` / ``.avg()`` is called,
    optionally traversing relations via ``__``. Also resolves the fields read
    by column expressions (see ``expression_columns``).
    """

    def __init__(self, select_str: str, model_cls: type["Model"]) -> None:
        super().__init__(query_str=select_str, model_cls=model_cls)

    def _split_value_into_parts(self, order_str: str) -> None:
        parts = order_str.split("__")
        self.field_name = parts[-1]
        self.related_parts = parts[:-1]

    @property
    def is_numeric(self) -> bool:
        return self.get_target_field_type() in [int, float, decimal.Decimal]

    def get_target_field_type(self) -> Any:
        return self.target_model.ormar_config.model_fields[self.field_name].__type__

    def get_text_clause(self) -> sqlalchemy.sql.expression.ColumnClause:
        alias = f"{self.table_prefix}_" if self.table_prefix else ""
        return sqlalchemy.column(f"{alias}{self.field_name}")

    def apply_func(
        self, func: Callable, use_label: bool = True
    ) -> sqlalchemy.sql.expression.ColumnElement:
        result = func(self.get_text_clause())
        if use_label:
            rel_prefix = f"{self.related_str}__" if self.related_str else ""
            result = result.label(f"{rel_prefix}{self.field_name}")
        return result


def expression_columns(
    node: Any, model: type["Model"], upsert: bool = False
) -> dict[str, SelectAction]:
    """
    Resolve the fields an expression reads, by path from ``model``, to the
    model and table alias each one is read from, like a filter on that path.

    In the ``update_values`` of an upsert (``upsert=True``) fields are the
    model's own only, as there are no joins, and ``Inserted`` fields resolve
    like ``F`` ones; anywhere else ``Inserted`` is rejected.

    :param node: expression node
    :type node: Any
    :param model: queried model
    :type model: type[Model]
    :param upsert: the expression is a value of an upsert's ``update_values``
    :type upsert: bool
    :raises QueryDefinitionError: if a path does not lead to a non-relation
        field, an upsert reads a related model or ``Inserted`` is used outside
        an upsert
    :return: resolved field actions by field path
    :rtype: dict[str, SelectAction]
    """
    columns: dict[str, SelectAction] = {}
    for leaf in expression_leaves(node=node):
        if isinstance(leaf, Inserted) and not upsert:
            raise QueryDefinitionError(
                f"Inserted('{leaf._path}') is only allowed in update_values of "
                f"create() and bulk_create() with on_conflict='update'"
            )
        if isinstance(leaf, (F, Inserted)) and leaf._path not in columns:
            name = type(leaf).__name__
            try:
                column = SelectAction(select_str=leaf._path, model_cls=model)
                field = column.target_model.ormar_config.model_fields.get(
                    column.field_name
                )
            except (KeyError, AttributeError):
                field = None
            if field is None or field.is_relation:
                raise QueryDefinitionError(
                    f"{name}('{leaf._path}') has to name a non-relation field, "
                    f"after the relations leading to its model if any."
                )
            if upsert and column.related_parts:
                raise QueryDefinitionError(
                    f"{name}('{leaf._path}') reads a related model: update_values "
                    f"of an upsert read only fields of the inserted model"
                )
            columns[leaf._path] = column
    return columns


def resolve_expression(
    node: Any,
    model: type["Model"],
    upsert: bool = False,
    update: bool = False,
    first_branch: int = 0,
) -> tuple[Any, dict[str, SelectAction]]:
    """
    Resolve an expression for a query from ``model``: the fields it reads (see
    ``expression_columns``) and the conditions of its ``When`` branches,
    resolved into filter actions like ``filter()`` arguments. The actions are
    kept on copies of the ``Case`` and ``When`` nodes and of the nodes
    holding them, so an expression can be reused in other queries.

    Branches are numbered from ``first_branch`` to name the slots of their
    conditions (see ``When.slot_prefix``); the two sides of a filter share
    one slot, so the second side continues the numbering of the first.

    :param node: expression node
    :type node: Any
    :param model: queried model
    :type model: type[Model]
    :param upsert: the expression is a value of an upsert's ``update_values``
    :type upsert: bool
    :param update: the expression is a value of ``update()``
    :type update: bool
    :param first_branch: number of the first ``When`` branch
    :type first_branch: int
    :raises QueryDefinitionError: if a field is not allowed (see
        ``expression_columns``) or a condition reads a related model through a
        reverse or many to many relation, or in an update or upsert any
        related model
    :return: resolved expression and its fields by path
    :rtype: tuple[Any, dict[str, SelectAction]]
    """
    resolved = resolve_branches(
        node=node,
        model=model,
        upsert=upsert,
        update=update,
        numbers=itertools.count(start=first_branch),
    )
    return resolved, expression_columns(node=resolved, model=model, upsert=upsert)


def resolve_branches(
    node: Any,
    model: type["Model"],
    upsert: bool,
    update: bool,
    numbers: Iterator[int],
) -> Any:
    """
    Copy the nodes of an expression down to its ``When`` branches and resolve
    the copies of the branches (see ``resolve_when``).

    :param node: expression node or a literal
    :type node: Any
    :param model: queried model
    :type model: type[Model]
    :param upsert: the expression is a value of an upsert's ``update_values``
    :type upsert: bool
    :param update: the expression is a value of ``update()``
    :type update: bool
    :param numbers: numbers of the branches
    :type numbers: Iterator[int]
    :return: resolved node
    :rtype: Any
    """
    if isinstance(node, Combined):
        combined = copy.copy(node)
        combined._left = resolve_branches(
            node=node._left, model=model, upsert=upsert, update=update, numbers=numbers
        )
        combined._right = resolve_branches(
            node=node._right, model=model, upsert=upsert, update=update, numbers=numbers
        )
        return combined
    if isinstance(node, Function):
        function = copy.copy(node)
        function._arguments = tuple(
            resolve_branches(
                node=argument,
                model=model,
                upsert=upsert,
                update=update,
                numbers=numbers,
            )
            for argument in node._arguments
        )
        return function
    if isinstance(node, Case):
        case = copy.copy(node)
        case._whens = [
            resolve_when(
                when=when, model=model, upsert=upsert, update=update, numbers=numbers
            )
            for when in node._whens
        ]
        case._default = resolve_branches(
            node=node._default,
            model=model,
            upsert=upsert,
            update=update,
            numbers=numbers,
        )
        return case
    return node


def resolve_when(
    when: When,
    model: type["Model"],
    upsert: bool,
    update: bool,
    numbers: Iterator[int],
) -> When:
    """
    Copy a ``When`` with its condition resolved into filter actions, in the
    order ``FilterGroup.get_text_clause`` builds them, and its value resolved.

    :param when: branch
    :type when: When
    :param model: queried model
    :type model: type[Model]
    :param upsert: the expression is a value of an upsert's ``update_values``
    :type upsert: bool
    :param update: the expression is a value of ``update()``
    :type update: bool
    :param numbers: numbers of the branches
    :type numbers: Iterator[int]
    :raises QueryDefinitionError: if the condition reads a related model
        through a reverse or many to many relation, or in an upsert any related
        model
    :return: resolved branch
    :rtype: When
    """
    duplicate = copy.copy(when)
    duplicate.number = next(numbers)
    duplicate.condition = when.condition.copy_tree()
    duplicate.condition.resolve(model_cls=model, upsert=upsert)
    duplicate.actions = list(duplicate.condition._iter())
    for action in duplicate.actions:
        for path in action.path_actions():
            check_condition_path(
                model=model, path=path.related_parts, upsert=upsert, update=update
            )
    duplicate.then = resolve_branches(
        node=when.then, model=model, upsert=upsert, update=update, numbers=numbers
    )
    return duplicate


def check_condition_path(
    model: type["Model"], path: list[str], upsert: bool, update: bool
) -> None:
    """
    Allow ``When`` conditions to read related models only through foreign
    keys, which match at most one row, and in an update or upsert none.

    ``update()`` joins the tables its expressions read, so a row with a
    ``NULL`` foreign key would not be updated at all instead of taking the
    default of the ``Case``.

    :param model: queried model
    :type model: type[Model]
    :param path: relation names leading to the read model
    :type path: list[str]
    :param upsert: the expression is a value of an upsert's ``update_values``
    :type upsert: bool
    :param update: the expression is a value of ``update()``
    :type update: bool
    :raises QueryDefinitionError: if the path follows a reverse, many to many
        or through relation, or reads a related model in an update or upsert
    """
    target = model
    for relation in path:
        field = target.ormar_config.model_fields[relation]
        if field.is_multi or field.virtual or field.is_through:
            raise QueryDefinitionError(
                f"When() conditions read related models only through foreign "
                f"keys, '{'__'.join(path)}' follows the reverse or many to many "
                f"relation '{relation}'"
            )
        target = field.to
    if upsert and path:
        raise QueryDefinitionError(
            f"When() conditions in update_values of an upsert read only fields of "
            f"the inserted model, not '{'__'.join(path)}'"
        )
    if update and path:
        raise QueryDefinitionError(
            f"When() conditions in update() read only fields of the updated "
            f"model, not '{'__'.join(path)}': the update joins the related "
            f"table, so rows with a NULL foreign key would be skipped instead of "
            f"taking the default"
        )


def expression_paths(node: Any, columns: dict[str, SelectAction]) -> list[QueryAction]:
    """
    Return the actions resolving the relation paths an expression reads: its
    fields and the fields of its ``When`` conditions.

    :param node: resolved expression node or a value
    :type node: Any
    :param columns: fields of the expression by path
    :type columns: dict[str, SelectAction]
    :return: actions each resolving one relation path
    :rtype: list[QueryAction]
    """
    return [
        *columns.values(),
        *(
            path
            for action in expression_conditions(node=node)
            for path in action.path_actions()
        ),
    ]
