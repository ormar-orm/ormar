import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    ForwardRef,
    Optional,
    Union,
    cast,
)

from pydantic import field_serializer
from pydantic._internal._decorators import DecoratorInfos
from pydantic.fields import FieldInfo
from pydantic_core.core_schema import SerializerFunctionWrapHandler

import ormar
from ormar import ForeignKey, ManyToMany
from ormar.exceptions import ModelDefinitionError
from ormar.fields import Through
from ormar.fields.relation_kind import (
    REVERSE_FOREIGN_KEY,
    REVERSE_ONE_TO_ONE,
    RelationKind,
)
from ormar.models.descriptors import RelationDescriptor
from ormar.models.helpers.rebuild import mark_for_rebuild
from ormar.models.helpers.sqlalchemy import adjust_through_many_to_many_model
from ormar.models.relation_graph import RelationGraph

if TYPE_CHECKING:  # pragma no cover
    from ormar import Model
    from ormar.fields import BaseField, ForeignKeyField, ManyToManyField


def expand_reverse_relationship(model_field: "ForeignKeyField") -> None:
    """
    If the reverse relation has not been set before it's set here.

    :param model_field:
    :type model_field:
    :return: None
    :rtype: None
    """
    if reverse_field_not_already_registered(model_field=model_field):
        register_reverse_model_fields(model_field=model_field)


def expand_reverse_relationships(model: type["Model"]) -> None:
    """
    Iterates through model_fields of given model and verifies if all reverse
    relation have been populated on related models.

    If the reverse relation has not been set before it's set here.

    :param model: model on which relation should be checked and registered
    :type model: Model class
    """
    model_fields = list(model.ormar_config.model_fields.values())
    for model_field in model_fields:
        if (
            model_field.is_relation
            and not model_field.has_unresolved_forward_refs()
            and not model_field.is_through
        ):
            model_field = cast("ForeignKeyField", model_field)
            expand_reverse_relationship(model_field=model_field)


def reverse_field_annotation(kind: RelationKind, annotation: Any) -> Any:
    """
    Pydantic annotation of a reverse relation field: a reverse foreign key
    accepts one model or a list of them, a reverse one-to-one a single model,
    and a many-to-many keeps the annotation of its field.

    :param kind: kind of the reverse relation field
    :type kind: RelationKind
    :param annotation: annotation of the reverse field
    :type annotation: Any
    :return: annotation to register on the related model
    :rtype: Any
    """
    if kind is REVERSE_FOREIGN_KEY:
        return Union[annotation, list[annotation], None]
    if kind is REVERSE_ONE_TO_ONE:
        return Optional[annotation]
    return annotation


def reset_relation_caches(model: type["Model"]) -> None:
    """
    Drop the lazily cached relation metadata of a model that gains a reverse
    relation, so models already used before a related model was defined see
    the new relation (and lose trusted row construction). The cached relation
    paths of every model connected to it are rebuilt on next use too.

    :param model: model receiving a reverse relation
    :type model: type[Model]
    """
    model._related_names = None
    model._related_fields = None
    model._related_field_map = None
    model._trusted_rows = None
    RelationGraph.changed()


def resolve_through_fields(model_field: "ManyToManyField") -> None:
    """
    Adopt the foreign keys a user-defined through model already declares to the
    owner and the target of a many-to-many relation (e.g. a legacy link table
    keyed by them), so ormar uses those fields instead of adding its own.
    Explicit ``through_relation_name`` / ``through_reverse_relation_name`` must
    name foreign keys to the owner / target when the through model declares
    them; several foreign keys to one side, and any self-referencing link
    table, need those names.

    :raises ModelDefinitionError: if the through model has relations to other
        models or unresolved forward references, if an explicit name points at
        the wrong field, or if the foreign keys are ambiguous
    :param model_field: many-to-many relation field
    :type model_field: ManyToManyField
    """
    through = model_field.through
    through_name = through.get_name(lower=False)
    through_fields = through.ormar_config.model_fields
    relations = {
        name: field
        for name, field in through_fields.items()
        if field.is_relation and not field.virtual
    }
    if not relations:
        return
    unresolved = [
        name for name, field in relations.items() if field.to.__class__ == ForwardRef
    ]
    if unresolved:
        raise ModelDefinitionError(
            f"Through model {through_name} has unresolved forward references "
            f"{unresolved}, call {through_name}.update_forward_refs() before the "
            "relation using it is completed."
        )
    allowed = (model_field.owner, model_field.to)
    foreign = [name for name, field in relations.items() if field.to not in allowed]
    if foreign:
        raise ModelDefinitionError(
            f"Through model {through_name} can only have relations to "
            f"{model_field.owner.get_name(lower=False)} and "
            f"{model_field.to.get_name(lower=False)}, got {foreign}."
        )
    if model_field.owner is model_field.to and not (
        model_field.through_relation_name and model_field.through_reverse_relation_name
    ):
        raise ModelDefinitionError(
            f"Self-referencing relation through {through_name} needs "
            "through_relation_name and through_reverse_relation_name naming its "
            "foreign keys."
        )
    for attribute, side in (
        ("through_relation_name", model_field.owner),
        ("through_reverse_relation_name", model_field.to),
    ):
        resolve_through_side(
            model_field=model_field, relations=relations, attribute=attribute, side=side
        )


def resolve_through_side(
    model_field: "ManyToManyField",
    relations: dict[str, "BaseField"],
    attribute: str,
    side: type["Model"],
) -> None:
    """
    Validate or pick the through model foreign key leading to one side of a
    many-to-many relation and store its name on the relation field.

    :raises ModelDefinitionError: if an explicit name is not a foreign key to
        the side, or several foreign keys lead to it without an explicit name
    :param model_field: many-to-many relation field
    :type model_field: ManyToManyField
    :param relations: non-virtual relation fields of the through model by name
    :type relations: dict[str, BaseField]
    :param attribute: ``through_relation_name`` or
        ``through_reverse_relation_name``
    :type attribute: str
    :param side: model the foreign key must lead to
    :type side: type[Model]
    """
    through_name = model_field.through.get_name(lower=False)
    name = getattr(model_field, attribute)
    if name is not None:
        existing = model_field.through.ormar_config.model_fields.get(name)
        if existing is not None and (
            name not in relations or relations[name].to is not side
        ):
            raise ModelDefinitionError(
                f"{attribute} '{name}' must be a foreign key of {through_name} to "
                f"{side.get_name(lower=False)}."
            )
        return
    candidates = [field_name for field_name, f in relations.items() if f.to is side]
    if len(candidates) > 1:
        raise ModelDefinitionError(
            f"Through model {through_name} has several foreign keys to "
            f"{side.get_name(lower=False)} ({candidates}), pass {attribute} to "
            "choose one."
        )
    if candidates:
        setattr(model_field, attribute, candidates[0])


def register_reverse_model_fields(model_field: "ForeignKeyField") -> None:
    """
    Registers reverse ForeignKey field on related model.
    By default it's name.lower()+'s' of the model on which relation is defined.

    But if the related_model name is provided it's registered with that name.
    Autogenerated reverse fields also set related_name to the original field name.

    :param model_field: original relation ForeignKey field
    :type model_field: relation Field
    """
    related_name = model_field.get_related_name()
    reset_relation_caches(model=model_field.to)
    related_model_fields = model_field.to.ormar_config.model_fields
    if model_field.is_multi:
        resolve_through_fields(model_field=cast("ManyToManyField", model_field))
        related_model_fields[related_name] = ManyToMany(  # type: ignore
            model_field.owner,
            through=model_field.through,
            name=related_name,
            virtual=True,
            related_name=model_field.name,
            owner=model_field.to,
            self_reference=model_field.self_reference,
            self_reference_primary=model_field.self_reference_primary,
            orders_by=model_field.related_orders_by,
            skip_field=model_field.skip_reverse,
            through_relation_name=model_field.through_reverse_relation_name,
            through_reverse_relation_name=model_field.through_relation_name,
            through_relation_nullable=model_field.through_reverse_relation_nullable,
            through_reverse_relation_nullable=model_field.through_relation_nullable,
        )
        # register foreign keys on through model
        model_field = cast("ManyToManyField", model_field)
        register_through_shortcut_fields(model_field=model_field)
        adjust_through_many_to_many_model(model_field=model_field)
    else:
        related_model_fields[related_name] = ForeignKey(  # type: ignore
            model_field.owner,
            real_name=related_name,
            virtual=True,
            related_name=model_field.name,
            owner=model_field.to,
            self_reference=model_field.self_reference,
            orders_by=model_field.related_orders_by,
            skip_field=model_field.skip_reverse,
            one_to_one=model_field.one_to_one,
        )
    if not model_field.skip_reverse:
        reverse_field = related_model_fields[related_name]
        field_type = reverse_field_annotation(
            kind=reverse_field.kind,
            annotation=reverse_field.__type__,
        )
        model_field.to.model_fields[related_name] = FieldInfo.from_annotated_attribute(
            annotation=field_type, default=None
        )
        add_field_serializer_for_reverse_relations(
            to_model=model_field.to, related_name=related_name
        )
        mark_for_rebuild(model=model_field.to)
        setattr(model_field.to, related_name, RelationDescriptor(name=related_name))


def add_field_serializer_for_reverse_relations(
    to_model: type["Model"], related_name: str
) -> None:
    def serialize(
        self: "Model", children: list["Model"], handler: SerializerFunctionWrapHandler
    ) -> Any:
        """
        Serialize a list of nodes, handling circular references
        by excluding the children.
        """
        try:
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore", message="Pydantic serializer warnings"
                    )
                    return handler(children)
            except ValueError as exc:  # pragma: no cover
                if not str(exc).startswith("Circular reference"):
                    raise exc

                if hasattr(children, "ormar_config"):
                    return cast("Model", children).reference_dict()
                result = []
                for child in children:
                    if not hasattr(child, "ormar_config"):
                        continue
                    # If there is one circular ref dump all children as pk only
                    result.append(child.reference_dict())
                return result
        except ReferenceError:
            # Pydantic >= 2.13 may invoke this serializer with weakref
            # children whose referent is already gone (e.g. when ormar's
            # late metaclass rebuild changed dispatch ordering).
            return None

    decorator = field_serializer(related_name, mode="wrap", check_fields=False)(
        serialize
    )
    setattr(to_model, f"serialize_{related_name}", decorator)
    # DecoratorInfos.build will overwrite __pydantic_decorators__ on to_model,
    # deleting the previous decorators. We need to save them and then merge them.
    prev_decorators = getattr(to_model, "__pydantic_decorators__", DecoratorInfos())
    new_decorators = DecoratorInfos.build(to_model)
    prev_decorators.validators.update(new_decorators.validators)
    prev_decorators.field_validators.update(new_decorators.field_validators)
    prev_decorators.root_validators.update(new_decorators.root_validators)
    prev_decorators.field_serializers.update(new_decorators.field_serializers)
    prev_decorators.model_serializers.update(new_decorators.model_serializers)
    prev_decorators.model_validators.update(new_decorators.model_validators)
    prev_decorators.computed_fields.update(new_decorators.computed_fields)
    setattr(to_model, "__pydantic_decorators__", prev_decorators)


def register_through_shortcut_fields(model_field: "ManyToManyField") -> None:
    """
    Registers m2m relation through shortcut on both ends of the relation.

    :param model_field: relation field defined in parent model
    :type model_field: ManyToManyField
    """
    through_model = model_field.through
    through_name = through_model.get_name(lower=True)
    related_name = model_field.get_related_name()
    for model in (model_field.owner, model_field.to):
        existing = model.ormar_config.model_fields.get(through_name)
        if existing is not None and not existing.is_through:
            raise ModelDefinitionError(
                f"Field {through_name} of {model.get_name(lower=False)} collides "
                f"with the shortcut to the through model {through_model.__name__}, "
                "rename the relation."
            )

    model_field.owner.ormar_config.model_fields[through_name] = Through(
        through_model,
        real_name=through_name,
        virtual=True,
        related_name=model_field.name,
        owner=model_field.owner,
        nullable=True,
    )

    model_field.to.ormar_config.model_fields[through_name] = Through(
        through_model,
        real_name=through_name,
        virtual=True,
        related_name=related_name,
        owner=model_field.to,
        nullable=True,
    )
    setattr(model_field.owner, through_name, RelationDescriptor(name=through_name))
    setattr(model_field.to, through_name, RelationDescriptor(name=through_name))


def verify_related_name_dont_duplicate(
    related_name: str, model_field: "ForeignKeyField"
) -> None:
    """
    Verifies whether the used related_name (regardless of the fact if user defined or
    auto generated) is already used on related model, but is connected with other model
    than the one that we connect right now.

    A field still leading to an unresolved ForwardRef cannot be compared yet, so the
    check is deferred and repeated by ``verify_deferred_related_names`` once the
    field is resolved.

    :raises ModelDefinitionError: if name is already used but lead to different related
    model
    :param related_name:
    :type related_name:
    :param model_field: original relation ForeignKey field
    :type model_field: relation Field
    :return: None
    :rtype: None
    """
    fk_field = model_field.to.ormar_config.model_fields.get(related_name)
    if not fk_field:  # pragma: no cover
        return
    if fk_field.has_unresolved_forward_refs():
        cast("ForeignKeyField", fk_field).deferred_related_name_checks.append(
            model_field
        )
        return
    if (
        fk_field.to != model_field.owner
        and fk_field.to.ormar_config != model_field.owner.ormar_config
    ):
        raise ormar.ModelDefinitionError(
            f"Relation with related_name "
            f"'{related_name}' "
            f"leading to model "
            f"{model_field.to.get_name(lower=False)} "
            f"cannot be used on model "
            f"{model_field.owner.get_name(lower=False)} "
            f"because it's already used by model "
            f"{fk_field.to.get_name(lower=False)}"
        )


def verify_deferred_related_names(model_field: "ForeignKeyField") -> None:
    """
    Repeats the related_name checks deferred while given field was leading to an
    unresolved ForwardRef, now that its target model is known.

    :raises ModelDefinitionError: if the field's name was used as related_name by a
    relation leading from a different model
    :param model_field: just resolved relation field
    :type model_field: relation Field
    :return: None
    :rtype: None
    """
    deferred = model_field.deferred_related_name_checks
    model_field.deferred_related_name_checks = []
    for claiming_field in deferred:
        verify_related_name_dont_duplicate(
            related_name=model_field.name, model_field=claiming_field
        )


def reverse_field_not_already_registered(model_field: "ForeignKeyField") -> bool:
    """
    Checks if the reverse side of the relation is not yet registered on the
    related model, i.e. no field named as the relation's related_name exists there.

    :raises ModelDefinitionError: if related name is already used but lead to different
    related model
    :param model_field: original relation ForeignKey field
    :type model_field: relation Field
    :return: result of the check
    :rtype: bool
    """
    related_name = model_field.get_related_name()
    if related_name not in model_field.to.ormar_config.model_fields:
        return True
    verify_related_name_dont_duplicate(
        related_name=related_name, model_field=model_field
    )
    return False
