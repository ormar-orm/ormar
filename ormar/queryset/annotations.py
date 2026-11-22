"""
Annotations: values computed by the database for each returned row, added to
a select with ``QuerySet.annotate()``::

    users = (
        await User.objects.annotate(
            active=Count("bookings", filter={"status": "active"})
        )
        .order_by("-active")
        .all()
    )
    ormar.annotations_of(users[0])["active"]  # -> 3

Each annotation is an expression resolved from the queried model, selected
under its name as a label. Its literals and the values of its conditions are
bound as ``ormar_a{i}_*`` slots, ``i`` being its position among the
queryset's annotations.
"""

from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Mapping, NamedTuple, Optional, cast

import sqlalchemy

from ormar.exceptions import QueryDefinitionError
from ormar.queryset.actions.order_action import OrderAction, order_text_clause
from ormar.queryset.actions.select_action import (
    SelectAction,
    resolve_expression,
)
from ormar.queryset.expressions import (
    AggregateSource,
    Expression,
    Sum,
    expression_clause,
    expression_key,
    expression_params,
)

if TYPE_CHECKING:  # pragma: no cover
    from ormar import Model


#: prefix of the slot names of annotations, ``ormar_a{i}``
ANNOTATION_SLOT_PREFIX = "ormar_a"


class Annotation(NamedTuple):
    """
    Expression of ``annotate()`` resolved from the queried model.

    :ivar name: name of the annotation, the label of its column
    :ivar node: resolved expression (see ``resolve_expression``)
    :ivar columns: fields the expression reads by path
    :ivar slot: slot name of its bound values, ``ormar_a{i}``
    """

    name: str
    node: Any
    columns: dict[str, SelectAction]
    slot: str


def column_names(model: type["Model"]) -> set[str]:
    """
    :param model: queried model
    :type model: type[Model]
    :return: database column names of the model's table
    :rtype: set[str]
    """
    return {column.name for column in model.ormar_config.table.columns}


def check_annotation_name(
    name: str, model: type["Model"], annotations: Mapping[str, Annotation]
) -> None:
    """
    Reject a name used by a field, relation or column of the model or by
    another annotation, or containing ``__``, which separates relations.

    :param name: name of the annotation
    :type name: str
    :param model: queried model
    :type model: type[Model]
    :param annotations: annotations already on the queryset
    :type annotations: Mapping[str, Annotation]
    :raises QueryDefinitionError: if the name is taken or contains ``__``
    """
    if "__" in name:
        raise QueryDefinitionError(
            f"annotate(): '{name}' contains '__', which separates relations, "
            f"give the annotation another name"
        )
    config = model.ormar_config
    if name in config.model_fields or name == "pk" or name in column_names(model):
        raise QueryDefinitionError(
            f"annotate(): '{name}' is a field or column of "
            f"{model.get_name(lower=False)}, give the annotation another name"
        )
    if name in annotations:
        raise QueryDefinitionError(f"annotate(): '{name}' is already annotated")


def resolve_annotation(
    name: str, expression: Any, model: type["Model"], index: int
) -> Annotation:
    """
    Resolve an expression of ``annotate()`` from the queried model. Its
    fields and the conditions of its ``When`` branches read related models
    only through foreign keys, so each row keeps one value.

    :param name: name of the annotation
    :type name: str
    :param expression: annotated expression
    :type expression: Any
    :param model: queried model
    :type model: type[Model]
    :param index: position of the annotation on the queryset
    :type index: int
    :raises QueryDefinitionError: if the value is not an expression or reads
        a related model through a reverse or many to many relation
    :return: resolved annotation
    :rtype: Annotation
    """
    if not isinstance(expression, Expression):
        raise QueryDefinitionError(
            f"annotate({name}=...) takes an expression (Count(), F(), Func(), "
            f"Case()...), not {type(expression).__name__}"
        )
    node, columns = resolve_expression(node=expression._node(), model=model)
    for path, column in columns.items():
        check_forward_path(name=name, path=path, column=column)
    return Annotation(
        name=name,
        node=node,
        columns=columns,
        slot=f"{ANNOTATION_SLOT_PREFIX}{index}",
    )


def check_forward_path(name: str, path: str, column: SelectAction) -> None:
    """
    Allow a field of an annotation to be read through foreign keys only,
    which match at most one row.

    :param name: name of the annotation
    :type name: str
    :param path: field path as written in the expression
    :type path: str
    :param column: resolved field
    :type column: SelectAction
    :raises QueryDefinitionError: if the path follows a reverse, many to many
        or through relation
    """
    target = column.source_model
    for relation in column.related_parts:
        field = target.ormar_config.model_fields[relation]
        if field.is_multi or field.virtual or field.is_through:
            raise QueryDefinitionError(
                f"annotate({name}=...): F('{path}') follows the reverse or many to "
                f"many relation '{relation}', annotations read related models only "
                f"through foreign keys; aggregate it with Count(), Sum()..."
            )
        target = field.to


def annotation_clause(annotation: Annotation) -> Any:
    """
    :param annotation: resolved annotation
    :type annotation: Annotation
    :return: SQL of the expression, its literals bound as slots
    :rtype: Any
    """
    return expression_clause(
        node=annotation.node, columns=annotation.columns, slot=annotation.slot
    )


def annotation_orders(
    columns: list[Any], annotations: Mapping[str, Annotation], model: type["Model"]
) -> list[Any]:
    """
    Replace the order strings naming annotations with their order actions.

    :param columns: arguments of ``order_by()``
    :type columns: list[Any]
    :param annotations: annotations of the queryset
    :type annotations: Mapping[str, Annotation]
    :param model: queried model
    :type model: type[Model]
    :raises QueryDefinitionError: if a string names neither a field, a
        column nor an annotation, e.g. one annotated only after ``order_by()``
    :return: the arguments with annotations resolved
    :rtype: list[Any]
    """
    resolved = []
    for column in columns:
        name = column.lstrip("-") if isinstance(column, str) else ""
        if name in annotations:
            column = AnnotationOrderAction(
                annotation=annotations[name],
                descending=column.startswith("-"),
                model_cls=model,
            )
        elif (
            name
            and "__" not in name
            and name not in ("pk", "?")
            and name not in model.ormar_config.model_fields
            and name not in column_names(model)
        ):
            raise QueryDefinitionError(
                f"order_by('{column}'): {model.get_name(lower=False)} has no "
                f"field, column or annotation '{name}'; annotate() has to come "
                f"before the order_by() naming it"
            )
        resolved.append(column)
    return resolved


class AnnotationOrderAction(OrderAction):
    """
    Orders the rows of the queried model by an annotation, created for
    ``order_by("-active")`` naming one. Like an order on the model's own
    field it replaces the default primary key order. The select orders by
    the label of the annotation, the limited sub query of ``select_related``
    pagination by the expression itself.
    """

    def __init__(
        self, annotation: Annotation, descending: bool, model_cls: type["Model"]
    ) -> None:
        """
        :param annotation: ordered annotation
        :type annotation: Annotation
        :param descending: order from the largest value
        :type descending: bool
        :param model_cls: queried model
        :type model_cls: type[Model]
        """
        self.annotation = annotation
        prefix = "-" if descending else ""
        super().__init__(order_str=f"{prefix}{annotation.name}", model_cls=model_cls)
        self.is_source_model_order = True

    def _split_value_into_parts(self, order_str: str) -> None:
        self.direction = "desc" if order_str.startswith("-") else ""
        self.field_name = order_str.lstrip("-")
        self.related_parts = []

    def get_field_name_text(self) -> str:
        """
        :return: name of the annotation, which keys the ordering of the
            limited sub query of the main model
        :rtype: str
        """
        return self.annotation.name

    def get_min_or_max(self) -> Any:
        """
        :return: the expression wrapped in the dialect's ordering aggregate,
            as the limited sub query groups the rows by primary key
        :rtype: Any
        """
        clause = annotation_clause(annotation=self.annotation)
        function = self.source_model.ormar_config.database.rules.ordering_aggregate(
            field_type=bool if isinstance(clause.type, sqlalchemy.Boolean) else None,
            descending=bool(self.direction),
        )
        ordered = getattr(sqlalchemy.func, function)(clause)
        return ordered.desc() if self.direction else ordered

    def get_text_clause(self) -> sqlalchemy.sql.expression.TextClause:
        """
        :return: the quoted label of the annotation with the direction
        :rtype: sqlalchemy.sql.elements.TextClause
        """
        quoter = self.source_model.ormar_config.database.dialect.identifier_preparer
        direction = f" {self.direction}" if self.direction else ""
        return order_text_clause(f"{quoter.quote(self.annotation.name)}{direction}")

    def check_if_filter_apply(self, target_model: type["Model"], alias: str) -> bool:
        """
        :param target_model: model of a joined table
        :type target_model: type[Model]
        :param alias: alias of the joined table
        :type alias: str
        :return: False, an annotation orders no joined table
        :rtype: bool
        """
        return False


def annotations_cache_key(annotations: Mapping[str, Annotation]) -> tuple:
    """
    :param annotations: annotations of a queryset
    :type annotations: Mapping[str, Annotation]
    :return: names and expression shapes of the annotations, in order
    :rtype: tuple
    """
    return tuple(
        (name, expression_key(node=annotation.node))
        for name, annotation in annotations.items()
    )


def annotations_params(annotations: Mapping[str, Annotation]) -> dict[str, Any]:
    """
    :param annotations: annotations of a queryset
    :type annotations: Mapping[str, Annotation]
    :return: values the annotations bind, by slot name (see
        ``expression_params``)
    :rtype: dict[str, Any]
    """
    params: dict[str, Any] = {}
    for annotation in annotations.values():
        params.update(expression_params(value=annotation.node, slot=annotation.slot))
    return params


def result_type(node: Any) -> Optional[type]:
    """
    Return the Python type an annotation is converted to where drivers
    differ: ``int`` for a ``Sum`` of an integer field, which MySQL returns as
    a ``DECIMAL``. ``Count`` is an ``int``, ``Avg`` a ``Decimal`` and
    ``Min`` / ``Max`` take the field's type on every dialect already.

    :param node: resolved expression
    :type node: Any
    :return: the type, None to keep the value the driver returns
    :rtype: Optional[type]
    """
    if not isinstance(node, Sum):
        return None
    target = cast(SelectAction, cast(AggregateSource, node.source).target)
    field = target.target_model.ormar_config.model_fields[target.field_name]
    return int if field.__type__ is int else None


def annotation_value(annotation: Annotation, value: Any) -> Any:
    """
    :param annotation: resolved annotation
    :type annotation: Annotation
    :param value: value returned by the database
    :type value: Any
    :return: the value as its result type (see ``result_type``)
    :rtype: Any
    """
    target_type = result_type(node=annotation.node)
    if value is None or target_type is None or isinstance(value, target_type):
        return value
    return target_type(value)  # pragma: no cover - MySQL only


def coerced_annotations(
    annotations: Mapping[str, Annotation],
) -> dict[str, Annotation]:
    """
    :param annotations: annotations of a queryset
    :type annotations: Mapping[str, Annotation]
    :return: the annotations whose values are converted (see ``result_type``)
    :rtype: dict[str, Annotation]
    """
    return {
        name: annotation
        for name, annotation in annotations.items()
        if result_type(node=annotation.node) is not None
    }


def row_annotations(row: Any, annotations: Mapping[str, Annotation]) -> dict[str, Any]:
    """
    :param row: database row
    :type row: Any
    :param annotations: annotations of the queryset
    :type annotations: Mapping[str, Annotation]
    :return: values of the annotations in the row, by name
    :rtype: dict[str, Any]
    """
    return {
        name: annotation_value(annotation=annotation, value=row[name])
        for name, annotation in annotations.items()
    }


def attach_annotations(
    instance: "Model", row: Any, annotations: Mapping[str, Annotation]
) -> None:
    """
    Keep the annotations of a row on the model built from it, read with
    ``annotations_of()``.

    :param instance: model built from the row
    :type instance: Model
    :param row: database row
    :type row: Any
    :param annotations: annotations of the queryset
    :type annotations: Mapping[str, Annotation]
    """
    object.__setattr__(
        instance,
        "__ormar_annotations__",
        row_annotations(row=row, annotations=annotations),
    )


def annotations_of(instance: "Model") -> Mapping[str, Any]:
    """
    Return the values ``annotate()`` computed for a model loaded by a
    queryset, by annotation name::

        user = await User.objects.annotate(active=Count("bookings")).first()
        ormar.annotations_of(user)["active"]  # -> 3

    Annotations are not fields: ``model_dump()`` does not return them and
    ``save()`` does not write them.

    :param instance: loaded model
    :type instance: Model
    :return: read-only mapping of the annotations, empty for a model loaded
        without them
    :rtype: Mapping[str, Any]
    """
    return MappingProxyType(getattr(instance, "__ormar_annotations__", None) or {})


def split_annotation_fields(
    fields: Any, annotations: Mapping[str, Annotation]
) -> tuple[Any, list[str]]:
    """
    Split the ``fields`` of ``values()`` into the model's fields and the
    names of annotations::

        split_annotation_fields(fields=["name", "active"], annotations=...)
        # -> (["name"], ["active"])

    :param fields: field name, list, set or dict of fields
    :type fields: Any
    :param annotations: annotations of the queryset
    :type annotations: Mapping[str, Annotation]
    :return: the model's fields in the same form, None if there are none,
        and the annotation names
    :rtype: tuple[Any, list[str]]
    """
    if isinstance(fields, str):
        fields = [fields]
    names = [name for name in fields if name in annotations]
    if isinstance(fields, dict):
        remaining: Any = {
            name: value for name, value in fields.items() if name not in annotations
        }
    else:
        remaining = type(fields)(name for name in fields if name not in annotations)
    return remaining or None, names
